//! Foreground activity and idle retirement for resident workspace resources.

use std::{ops::Deref, sync::Arc, time::Duration};

use tokio::time::Instant;

use super::{WorkspaceRuntime, WorkspaceRuntimeManager, lock};
use crate::DaemonError;

const DEFAULT_IDLE_TTL: Duration = Duration::from_hours(4);
const MAINTENANCE_INTERVAL: Duration = Duration::from_mins(1);

/// Node's maximum timer delay (`2_147_483_647` ms) rounded down to whole seconds.
pub(crate) const MAX_TIMER_DELAY_SECONDS: u64 = 2_147_483;
pub(crate) const WATCHER_IDLE_TIMEOUT_SECONDS_ENV: &str = "ZVEC_GREP_WATCHER_IDLE_TIMEOUT_SECONDS";

/// Resolves the idle deadline from the environment, defaulting to four hours.
///
/// # Errors
///
/// Returns `DaemonError::InvalidWatcherIdleTimeout` when the value is not an
/// integer between zero and `MAX_TIMER_DELAY_SECONDS` seconds. Zero disables
/// idle retirement, matching the TypeScript manager that schedules no check.
pub(crate) fn configured_idle_ttl() -> Result<Duration, DaemonError> {
    parse_idle_ttl(
        std::env::var(WATCHER_IDLE_TIMEOUT_SECONDS_ENV)
            .ok()
            .as_deref(),
    )
}

/// Unset or blank values keep the default. Surrounding whitespace and leading
/// zeros are accepted, exactly like the TypeScript `^\d+$` check.
pub(crate) fn parse_idle_ttl(configured: Option<&str>) -> Result<Duration, DaemonError> {
    let Some(configured) = configured.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(DEFAULT_IDLE_TTL);
    };
    let invalid = || DaemonError::InvalidWatcherIdleTimeout {
        max_seconds: MAX_TIMER_DELAY_SECONDS,
    };
    if !configured.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    let seconds = configured.parse::<u64>().map_err(|_| invalid())?;
    if seconds > MAX_TIMER_DELAY_SECONDS {
        return Err(invalid());
    }
    Ok(Duration::from_secs(seconds))
}

pub(super) struct RuntimeLifecycle {
    last_used: Instant,
    active: usize,
    retired: bool,
}

impl Default for RuntimeLifecycle {
    fn default() -> Self {
        Self {
            last_used: Instant::now(),
            active: 0,
            retired: false,
        }
    }
}

/// An admitted operation protects its runtime until completion or cancellation.
pub(super) struct RuntimeActivity {
    runtime: Arc<WorkspaceRuntime>,
    foreground: bool,
}

impl RuntimeActivity {
    pub(super) fn continuation(&self) -> Self {
        lock(&self.runtime.lifecycle).active += 1;
        Self {
            runtime: Arc::clone(&self.runtime),
            foreground: false,
        }
    }

    pub(super) fn begin(runtime: &Arc<WorkspaceRuntime>, foreground: bool) -> Option<Self> {
        let mut state = lock(&runtime.lifecycle);
        if state.retired {
            return None;
        }
        state.active += 1;
        if foreground {
            state.last_used = Instant::now();
        }
        Some(Self {
            runtime: Arc::clone(runtime),
            foreground,
        })
    }
}

impl Deref for RuntimeActivity {
    type Target = Arc<WorkspaceRuntime>;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl Drop for RuntimeActivity {
    fn drop(&mut self) {
        let mut state = lock(&self.runtime.lifecycle);
        state.active -= 1;
        if self.foreground {
            state.last_used = Instant::now();
        }
    }
}

impl WorkspaceRuntimeManager {
    #[cfg(test)]
    pub(super) const DEFAULT_IDLE_TTL: Duration = DEFAULT_IDLE_TTL;

    pub(super) fn start_maintenance(&self) {
        // A zero deadline schedules no check in the TypeScript manager, so it
        // disables retirement rather than retiring every runtime immediately.
        if self.inner.idle_ttl.is_zero() {
            return;
        }
        let mut task = lock(&self.inner.maintenance);
        if task.is_some() || self.inner.closed.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        let weak = Arc::downgrade(&self.inner);
        let shutdown = self.inner.shutdown.clone();
        let interval = self.inner.idle_ttl.min(MAINTENANCE_INTERVAL);
        *task = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    () = tokio::time::sleep(interval) => {}
                }
                let Some(inner) = weak.upgrade() else { break };
                let manager = WorkspaceRuntimeManager { inner };
                manager.retire_idle(Instant::now()).await;
            }
        }));
    }

    pub(super) async fn retire_idle(&self, now: Instant) {
        if self.inner.idle_ttl.is_zero() {
            return;
        }
        let retired = {
            let mut runtimes = lock(&self.inner.runtimes);
            let mut retired = Vec::new();
            runtimes.retain(|root, runtime| {
                let mut state = lock(&runtime.lifecycle);
                if state.active != 0
                    || now.saturating_duration_since(state.last_used) < self.inner.idle_ttl
                    || self.inner.scheduler.has_active_root(root)
                {
                    return true;
                }
                // Admission and history removal share the map lock. A replacement
                // runtime cannot submit a job that this retirement would cancel.
                state.retired = true;
                self.inner.scheduler.forget_root(root);
                retired.push(Arc::clone(runtime));
                false
            });
            retired
        };
        for runtime in retired {
            if let Err(error) = Self::close_watcher(&runtime).await {
                tracing::warn!(%error, root = %runtime.canonical_root.display(), "idle watcher close failed");
            }
        }
    }
}

impl WorkspaceRuntime {
    pub(super) fn retire(&self) {
        lock(&self.lifecycle).retired = true;
    }

    pub(super) fn is_retired(&self) -> bool {
        lock(&self.lifecycle).retired
    }
}
