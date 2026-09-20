use thiserror::Error;

use crate::{EngineError, ErrorSite};

/// Codes for failures that affect a whole model rather than one input, matching
/// main's shared-terminal classes (`ZVEC_GREP.ENGINE.MODELS.*` there, `ZG.ENGINE`
/// here). The pipeline stops the indexing operation instead of retrying per file.
pub(crate) const MODEL2VEC_DOWNLOAD_FAILED: &str = "ZG.ENGINE.MODELS.MODEL2VEC_DOWNLOAD_FAILED";
pub(crate) const MODEL2VEC_LOAD_FAILED: &str = "ZG.ENGINE.MODELS.MODEL2VEC_LOAD_FAILED";
pub(crate) const TRANSFORMERS_JS_LOAD_FAILED: &str = "ZG.ENGINE.MODELS.TRANSFORMERS_JS_LOAD_FAILED";

/// Prefix shared by every model preparation failure code.
const MODEL_CODE_PREFIX: &str = "ZG.ENGINE.MODELS.";

#[derive(Debug, Error)]
#[error("{message}")]
pub struct ModelError {
    code: &'static str,
    message: String,
    context: Option<String>,
    cause: Option<String>,
    origin: ErrorSite,
}

impl ModelError {
    #[track_caller]
    pub(crate) fn new(
        code: &'static str,
        message: impl Into<String>,
        context: Option<String>,
    ) -> Self {
        Self {
            code,
            message: message.into(),
            context,
            cause: None,
            origin: ErrorSite::capture(),
        }
    }

    #[track_caller]
    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(EngineError::INVALID_ARGUMENT, message, None)
    }

    #[track_caller]
    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::new(EngineError::UNSUPPORTED, message, None)
    }

    #[track_caller]
    pub(crate) fn storage_failure(message: impl Into<String>) -> Self {
        Self::new(EngineError::STORAGE_FAILURE, message, None)
    }

    #[track_caller]
    pub(crate) fn cancelled(message: impl Into<String>) -> Self {
        Self::new(EngineError::CANCELLED, message, None)
    }

    #[track_caller]
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(EngineError::INTERNAL, message, None)
    }

    pub(crate) fn with_cause(mut self, cause: impl std::fmt::Display) -> Self {
        self.cause = Some(cause.to_string());
        self
    }

    /// Labels a model preparation failure so the pipeline can tell a shared
    /// failure from one caused by a single input.
    ///
    /// Failures that already carry a model code keep it, so a `Model2Vec` download
    /// failure stays distinct from a load failure.
    pub(crate) fn relabel_preparation_failure(self, code: &'static str) -> Self {
        if self.code.starts_with(MODEL_CODE_PREFIX) {
            self
        } else {
            Self { code, ..self }
        }
    }

    pub(crate) fn wrap(self, message: impl Into<String>, context: Option<String>) -> Self {
        let Self {
            code,
            message: cause_message,
            context: cause_context,
            cause,
            origin,
        } = self;
        Self {
            code,
            message: message.into(),
            context,
            cause: Some(compose_message(cause_message, cause_context, cause)),
            origin,
        }
    }

    pub(crate) fn into_engine_error(self) -> EngineError {
        let Self {
            code,
            message,
            context,
            cause,
            origin,
        } = self;
        EngineError::new_at(code, compose_message(message, context, cause), origin)
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }

    #[must_use]
    pub fn context(&self) -> Option<&str> {
        self.context.as_deref()
    }

    #[must_use]
    pub fn cause(&self) -> Option<&str> {
        self.cause.as_deref()
    }
}

fn compose_message(mut message: String, context: Option<String>, cause: Option<String>) -> String {
    if let Some(context) = context {
        message.push_str(": ");
        message.push_str(&context.replace('\n', "; "));
    }
    if let Some(cause) = cause {
        message.push_str("; cause: ");
        message.push_str(&cause);
    }
    message
}

#[cfg(test)]
mod tests {
    use super::ModelError;

    #[test]
    fn preserves_the_original_site_across_wrapping_and_conversion() {
        let origin_line = line!() + 1;
        let cause = ModelError::internal("model operation failed");
        let error = cause
            .wrap("embedding failed", Some("model=test".to_owned()))
            .into_engine_error();

        assert!(error.origin().file.ends_with("src/models/error.rs"));
        assert_eq!(error.origin().line, origin_line);
        assert_eq!(
            error.message(),
            "embedding failed: model=test; cause: model operation failed"
        );
    }
}
