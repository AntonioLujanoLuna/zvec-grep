# Rust / main parity audit

Tracks changes from the Node.js `main` branch that must be reflected in the Rust
implementation, and records what the Rust workspace already covers.

## Audit metadata

- Audit date: 2026-09-20 (previous audit: 2026-09-13).
- Main snapshot: `b1d0ce9a83448f90db4d5fdb17faae7a2f01a790` (includes #167, which
  integrated the Rust workspace into `main`; `upstream/dev/rust` `a0e75d4` is
  tree-identical to this commit).
- Previous main snapshot: `d4e8a3eabac13172b1c78dfa2f1b4ccfc8b99035`.
- Rust tracking baseline: same tree as `main@b1d0ce9` (see above).
- Method: static inspection of both implementations on Linux aarch64, plus local
  builds and tests (see below). Statuses are code-reading results unless marked
  `[!]`.
- Local verification, Linux aarch64, rustc/cargo 1.98.0:
  - `cargo check --workspace --all-targets` — passed (4m06s, cold cache).
  - `bash rust/scripts/check.sh` — passed (3m35s, warm cache): `cargo fmt --all
    --check`, per-package `cargo check`, `cargo clippy --workspace --all-targets
    -- -D warnings` with zero warnings, `cargo test --workspace` with 553 passed
    / 0 failed / 5 ignored across 30 test binaries, and the npm-local Node tests
    (7 passed / 0 failed).
  - Environment note: the first attempt was OOM-killed (`exit 143`, linker killed
    by the per-command memory cgroup on this host). Running `check.sh` in its own
    systemd user scope (`systemd-run --user -p MemoryMax=16G`, peak use ~8.3 GB)
    completed normally; the passing run above used the repository's default
    profile with no overrides.
- Review status: this is a working document for our own remediation of work item A,
  on the `fix/rust-shared-credential-redaction` branch. It has not been proposed
  upstream.
- Not established by this audit: runtime behavior on Windows and macOS, inotify
  watch counts, real-model downloads, and anything requiring an MCP host.
  Items that need those are marked `[!]` rather than assumed.

## Status legend

- `[x]` verified covered by the Rust workspace (evidence cited).
- `[~]` partially covered; the cited remainder is still outstanding.
- `[ ]` not covered by the Rust workspace.
- `[!]` cannot be settled by reading code here; needs runtime or platform evidence.

## Changes to main since the previous audit

### Carried over from the 2026-09-13 audit

Statuses updated where this pass re-verified them; the rest are unchanged from
the previous revision.

| Main commit | Change | Rust coverage | Work item |
| --- | --- | --- | --- |
| `5265395` [#81](https://github.com/zvec-ai/zvec-grep/pull/81) | Embedding failures and retries | Partial; central failure handling is missing (see A) | A |
| `8d0d5e2` [#112](https://github.com/zvec-ai/zvec-grep/pull/112) | ModelScope fallback and download reliability | Mostly missing; no ModelScope mapping at all (see B) | B |
| `13df3ad` [#114](https://github.com/zvec-ai/zvec-grep/pull/114) | Spurious stdio bridge exits | Partial; no consecutive-miss grace window (see C) | C |
| `ef544e1` [#117](https://github.com/zvec-ai/zvec-grep/pull/117) | Race-safe daemon lease publication | Different lease architecture; atomic hard-link publication is present (`crates/zg-daemon/src/controller.rs:110`), replacement-during-write still unverified | C |
| `0c3a6e7` [#96](https://github.com/zvec-ai/zvec-grep/pull/96) | Watcher idle timeout | No idle eviction or timeout configuration (see D) | D |
| `03ae9cb` [#86](https://github.com/zvec-ai/zvec-grep/pull/86) | Linux directory watchers | Per-directory backend used and registration now consults the ignore-aware policy; verification with the real policy still missing (see D) | D |
| `891401e` [#84](https://github.com/zvec-ai/zvec-grep/pull/84) | Windows managed-rg paths | Missing in the MCP command-string parser (see E) | E |
| `d4e8a3e` [#107](https://github.com/zvec-ai/zvec-grep/pull/107) | OpenCode JSONC and config selection | Missing; `OPENCODE_CONFIG` override present, no `XDG_CONFIG_HOME`/precedence, trailing commas rejected (see F) | F |
| `3cc4f81` [#63](https://github.com/zvec-ai/zvec-grep/pull/63) | Qoder tool permissions | Mostly covered; forced takeover retains unrelated approvals (not re-verified this pass) | F |
| `02fbf87` [#62](https://github.com/zvec-ai/zvec-grep/pull/62) | Qoder IDE install path | Core behavior covered; environment-path normalization differs (not re-verified this pass) | F |
| `69602cb` [#58](https://github.com/zvec-ai/zvec-grep/pull/58) | Qoder IDE integration | Partial; Windows detection and public MCP parameters differ (not re-verified this pass) | F, G |
| `462d3be` [#52](https://github.com/zvec-ai/zvec-grep/pull/52) | Qoder integration | Installer mostly covered; consent/elicitation behavior differs (not re-verified this pass) | F, G |
| `1c3dbff` [#105](https://github.com/zvec-ai/zvec-grep/pull/105) | Search as the default CLI action | Missing (see G) | G |
| `7d73ca1` [#100](https://github.com/zvec-ai/zvec-grep/pull/100) | CI concurrency isolation | Covered; `concurrency.group` scopes workflow, event, and PR/SHA (see H) | H |
| `87f2c2a` [#119](https://github.com/zvec-ai/zvec-grep/pull/119) | CPU-only packaged-model smoke test | No equivalent enabled Rust smoke test (see H) | H |

### Landed after the 2026-09-13 audit

| Main commit | Change | Rust coverage | Work item |
| --- | --- | --- | --- |
| `f6dddcf` [#71](https://github.com/zvec-ai/zvec-grep/pull/71) | GitHub Copilot and VS Code install targets | Missing; `Agent` enum has Claude, Codex, Cursor, OpenCode, Qoder only (`crates/zg-cli/src/install.rs:42-60`) | F, G |
| `e76c89f` [#159](https://github.com/zvec-ai/zvec-grep/pull/159) | Unify embedding concurrency controls | Partial; `resolve_embedding_concurrency` exists (`crates/zg-engine/src/models/runtime.rs:186`), no `ZVEC_GREP_INDEX_EMBEDDING_CONCURRENCY` equivalent | A |
| `86fae49` [#145](https://github.com/zvec-ai/zvec-grep/pull/145) | Daemon log rotation, quiet successful health probes | Missing; no rotation or retention in `crates/zg-daemon/src` | C |
| `156aa1f` [#82](https://github.com/zvec-ai/zvec-grep/pull/82) | Bound scheduler job retention, release finished run closures | Partial; the scheduler bounds its queue (`queue_capacity`, default 64, `crates/zg-daemon/src/job_scheduler.rs:81,94`) but has no per-root retention release equivalent to main's (`src/daemon/job-scheduler.ts:178-182`) | C |
| `ed2ebfc` [#102](https://github.com/zvec-ai/zvec-grep/pull/102) | Recover watcher activation after registration failures | Partial; `watch_loop`/`refresh_watcher` repair registration (`crates/zg-host-native/src/watcher.rs:263,437`) without main's failure accounting | D |
| `8371829` [#150](https://github.com/zvec-ai/zvec-grep/pull/150) | Stop retrying failed Transformers initialization | `[!]` needs the same regression as A: a failed initialization must not be retried per queued item | A |
| `b6764c9` [#154](https://github.com/zvec-ai/zvec-grep/pull/154) | Validate shutdown request origins | Missing; `/control/shutdown` (`crates/zg-daemon/src/runtime.rs:104`) has no `Origin` validation — no `Origin` handling exists anywhere in `zg-daemon` — while main rejects non-loopback or mismatched origins with `forbidden_origin` (`src/daemon/http-server.ts:186-310`) | C |
| `b1d0ce9` [#167](https://github.com/zvec-ai/zvec-grep/pull/167) | Integrate the Rust workspace into `main` | n/a (integration commit) | H |

## A. Embedding failure handling

Relevant code: [indexing pipeline](crates/zg-engine/src/pipelines/indexing/pipeline.rs),
[model interface](crates/zg-engine/src/models/spi.rs),
[model runtime manager](crates/zg-engine/src/models/runtime.rs),
[Model2Vec](crates/zg-engine/src/models/model2vec/model.rs),
[job scheduler](crates/zg-daemon/src/job_scheduler.rs),
[CLI entry point](crates/zg/src/main.rs), and [rendering](crates/zg-cli/src/render.rs).

- `[ ]` Prepare required models once before dispatching embedding work.
  `ModelRuntimeManager::acquire_impl` constructs the model handle under the
  manager lock and the code comment states the design intent: *"Backends load
  heavy resources lazily, so this guarantees a single instance without blocking
  on I/O"* (`crates/zg-engine/src/models/runtime.rs:157-166`). There is no
  `prepare`/`ensure_ready` equivalent on the model SPI or the runtime manager
  (no matches for `prepare`, `ensure_ready`, `initialize`, `warm`).
- `[~]` Keep unchanged and empty-content operations free of unnecessary model
  downloads or loading. Invalid globs and unsupported sources are rejected before
  model acquisition (`crates/zg-engine/src/pipelines/indexing/service.rs:196`),
  and empty-fragment files are committed without embedding
  (`pipeline.rs:649-651`). The unchanged-file path and its request counts still
  need the regression that main uses.
- `[~]` Stop scheduling on shared terminal failures. Implemented on this branch:
  `classify_embedding_retry` now sets `fail_fast`, and the batch, per-file and
  one-by-one decision points abort the operation instead of recording one failure
  per file (`crates/zg-engine/src/pipelines/indexing/pipeline.rs`). Local model
  preparation failures carry their own codes
  (`ZG.ENGINE.MODELS.MODEL2VEC_DOWNLOAD_FAILED`, `MODEL2VEC_LOAD_FAILED`,
  `TRANSFORMERS_JS_LOAD_FAILED`, with `RESOURCE_CLOSED` standing in for main's
  disposed-model state), relabelled at the two `ensure_loaded` entry points and
  the Model2Vec download site. At the job level `EngineError::is_retryable()`
  remains limited to `RESOURCE_BUSY | DEADLINE_EXCEEDED`
  (`crates/zg-engine/src/error.rs:178-180`), which the scheduler consumes
  (`crates/zg-daemon/src/job_scheduler.rs:681`).
- `[~]` Classify transient failures, including HTTP 408 and network/timeouts.
  Implemented on this branch: `classify_embedding_retry` covers 429 (including
  rate-limit wording and `retry-after`/`retryafterms`), 5xx, 408, and transport
  failures, which the Rust remote client reports as an endpoint context without a
  status where main uses a `_REQUEST_FAILED` code. Permanent classes are carried
  too: 401/403/404 and missing-credential wording, a rejected model or dimension
  (main's provider-code set and provider-message patterns), and a local vector
  dimension mismatch. The retry budget keeps main's numbers (3 transient / 6
  rate-limited attempts, 500 ms and 2 s base delays, jitter, `retry-after`
  override). Classification expectations in the tests were derived by reading
  `classifyEmbeddingRetry`; unlike the redaction cases there is no captured
  fixture for them yet.
- `[x]` One bounded budget, and no shared failure reaching the second pass.
  Main retries failed files in a second pass on purpose
  (`src/engine/pipeline/indexing/index.ts:250-268`, "retried failed files once
  automatically"), so the Rust pass loop matches it
  (`crates/zg-engine/src/pipelines/indexing/pipeline.rs:117-145`). The earlier
  wording of this item implied the second pass should disappear; with fail-fast
  classification in place, the shared failures that made it wasteful no longer
  reach it.
- `[x]` Preserve targeted fallback for request-specific content failures. Batch
  failure still falls back to single-fragment embedding, and a fail-fast error
  inside the fallback is now rethrown with its original classification instead of
  being wrapped as an internal failure, matching main's
  `shouldFailFastEmbeddingError` checks
  (`crates/zg-engine/src/pipelines/indexing/pipeline.rs`, `embed_fragment_batch`).
- `[~]` Preserve cancellation behavior and check cancellation before model
  initialization. `check_cancelled` guards the per-item loops in Model2Vec
  (`crates/zg-engine/src/models/model2vec/model.rs:485,497,554`) and llama.cpp
  (`crates/zg-engine/src/models/llama_cpp/mod.rs:501,512,569,660,818`), and a
  queued embedding is cancelled while waiting for compute capacity
  (`crates/zg-engine/src/models/runtime.rs:286-288`). Initialization itself
  happens inside the embed call, so it is not covered by a pre-initialization
  check.
- `[~]` Retain useful provider errors, context, causes, and retry hints across
  engine, daemon, MCP, CLI, and status. `ModelError` carries
  code/message/context/cause and composes them
  (`crates/zg-engine/src/models/error.rs:6-95`); `ErrorReport` adds help and
  origin (`crates/zg-engine/src/error.rs:178-203`). There is no retry-hint field,
  and `retryAfterMs` survives only inside the classifier's string parsing.
- `[~]` Extend redaction to quoted credential fields, Basic authentication, URL
  userinfo, passwords/secrets, and standalone API-key forms. Implemented on this
  branch as a shared engine helper that ports the five oracle passes
  (`crates/zg-engine/src/redaction.rs`) and is now used by persisted job errors
  (`crates/zg-daemon/src/job_scheduler.rs:681-690`, replacing the previous
  bearer/assigned-name-only implementation), CLI error rendering
  (`crates/zg/src/main.rs:23-38`) and CLI progress text
  (`crates/zg-cli/src/progress.rs:271-305`). Before that change redaction existed
  only for persisted job errors, with no CLI or daemon-surface coverage. Oracle
  parity is recorded in `compat/redaction/cases.json` (28 cases) and asserted by
  `crates/zg-engine/tests/redaction_compat.rs`, which fails when any pass is
  removed (verified by a deliberate mutation). Remaining gap: daemon HTTP error
  replies and MCP responses still serialize the raw report, exactly as main does,
  because redaction happens in the CLI and status layers there too.

## B. Model download reliability

Relevant code: [model catalog](crates/zg-engine/src/models/catalog.rs),
[Model2Vec](crates/zg-engine/src/models/model2vec/model.rs),
[Transformers](crates/zg-engine/src/models/transformers/mod.rs),
[llama.cpp](crates/zg-engine/src/models/llama_cpp/mod.rs), and
[artifact publication](crates/zg-engine/src/models/artifacts.rs).

- `[ ]` Add pinned source revisions, artifact sizes/checksums, and Hugging Face /
  ModelScope mappings. Verified drift against main:
  - No ModelScope support anywhere in the workspace (`grep -ri modelscope rust/`
    matches this checklist only). Main maps both sources for 12 models, e.g.
    `bge-small-en-v1.5` HF `4a9a46c7…` / ModelScope `f246b360…`
    (`src/engine/models/catalog.ts:112-124`).
  - No sizes or checksums in the Rust catalog (`sha256`/`checksum` have no
    matches in `crates/zg-engine/src/models/catalog.rs`). Main declares
    `artifacts[]` with `size` and `sha256` for every local model (1-5 artifacts).
  - GGUF URIs are unpinned: `uri: "hf:ggml-org/embeddinggemma-300M-GGUF/embeddinggemma-300M-Q8_0.gguf"`
    (`catalog.rs:143`) and the Qwen3-Embedding GGUF (`catalog.rs:154`), where
    main appends `#<revision>` and records `cacheFile`
    (`src/engine/models/catalog.ts`, `local/embeddinggemma-300m`). Rust therefore
    fetches whatever the repository default revision currently is.
  - `TransformersConfig`/`Model2VecConfig` carry a single `revision`
    (`catalog.rs:100-128`) with no per-source mapping.
- `[~]` Check both source caches before networking and use the selected snapshot
  consistently. Not applicable until B1 lands; caches are keyed per artifact name
  and validate non-empty content only (`crates/zg-engine/src/models/model2vec/model.rs`,
  GGUF magic check in `crates/zg-engine/src/models/llama_cpp/mod.rs`).
- `[ ]` Add response-header and read-idle deadlines.
  `reqwest::Client::new()` is used with no configured timeout for Model2Vec,
  Transformers, and llama.cpp downloads
  (`models/model2vec/model.rs:380`, `models/transformers/mod.rs:149`,
  `models/llama_cpp/mod.rs:147`); only the Qwen remote path sets `.timeout(REMOTE_TIMEOUT)`
  (`models/qwen/mod.rs:302-303`), which is a total deadline rather than the
  header/read-idle pair main uses.
- `[~]` Verify cached/downloaded artifacts, repair completion metadata, and
  publish atomically. Publication is a temp-file rename
  (`crates/zg-engine/src/models/artifacts.rs:21,107`); there is no completion
  metadata repair or content verification.
- `[ ]` Coordinate downloads across processes, recover stale owners, and prevent
  an old writer from replacing a successor's artifact. No download lock or owner
  record exists in `models/`; main has a dedicated cache lock
  (`src/engine/models/artifact-cache-lock.ts`).
- `[ ]` Refresh the main catalog oracle fixture and record its source revision.
  Confirmed stale and, more importantly, it encodes the Rust shape rather than
  main's: `crates/zg-engine/src/models/tests/fixtures/catalog-main-oracle.json`
  has no `revision` fragment in `uri`, no `cacheFile`, no `sources`, and no
  `artifacts`/`sha256` — so it agrees with the Rust catalog by construction and
  cannot detect the drift in B1. See "Fixture and oracle health" below.

## C. Daemon publication and stdio lifetime

Relevant code: [controller](crates/zg-daemon/src/controller.rs) and
[stdio bridge](crates/zg-daemon/src/stdio.rs).

- `[~]` Publish initial and ready instance records atomically. The controller
  documents an atomic hard-link publication
  (`crates/zg-daemon/src/controller.rs:110`) with ownership checks against the
  current record (`controller.rs:204-218`). The "readable previous record during
  replacement" guarantee still needs an interrupted-write test.
- `[ ]` Match main's grace period for missing observations. `crates/zg-daemon/src/stdio.rs`
  has no consecutive-miss counter or grace window (no matches for
  `consecutive`/`grace`/`miss`); main tolerates two and stops on the third
  (`src/mcp/stdio-bridge.ts`).
- `[!]` Lease acquisition/release and shutdown under concurrency.

## D. Watcher lifetime and Linux resource usage

Relevant code: [workspace runtime](crates/zg-daemon/src/workspace_runtime.rs) and
[native watcher](crates/zg-host-native/src/watcher.rs).

- `[ ]` Add the four-hour idle default and `ZVEC_GREP_WATCHER_IDLE_TIMEOUT_SECONDS`;
  zero disables eviction. No match for `IDLE`/`idle_timeout`/`idle_evict` in
  `crates/zg-daemon/src` or `crates/zg-host-native/src`, and the variable is
  absent from the Rust environment surface (`src/daemon/watch-manager.ts` has it
  in main).
- `[~]` Prune excluded directory trees during native watch registration.
  Registration already consults `root.policy.can_descend(path)` before registering
  and before descending (`crates/zg-host-native/src/watcher.rs:748,765-766`), and
  the real policy implements ignore-aware descent
  (`crates/zg-engine/src/file_selection/policy.rs:316-327`). What is missing is
  verification with the real policy: `crates/zg-host-native/tests/watcher_compat.rs`
  exercises this path with fake `PathPolicy` implementations
  (`watcher_compat.rs:144-168`), so ignored-tree resource behavior is not yet
  established by a test.
- `[~]` Recover activation after registration failures (#102). `refresh_watcher`
  rebuilds registration on demand (`crates/zg-host-native/src/watcher.rs:437-455`);
  the failure accounting and retry policy main added are not present.
- `[!]` Verify directory additions/removals and continued delivery after dynamic
  policy/tree changes on the native Linux backend.

## E. Windows managed-rg paths

Relevant code: [MCP transport](crates/zg-transport-mcp/src/lib.rs),
`scan_rg_command`.

- `[ ]` Preserve unquoted Windows backslashes in command-string input. In the
  command lexer an unquoted `\` sets the escape flag and is dropped, so
  `rg needle src\cli` is scanned as `srccli`
  (`crates/zg-transport-mcp/src/lib.rs:1437-1441`). Inside double quotes only
  `"`, `\`, `$`, and backtick are treated as escapes
  (`crates/zg-transport-mcp/src/lib.rs:1405-1420`), which is the Unix behavior
  that must be retained.
- `[!]` Verify unquoted `rg needle src\cli`, quoted paths with spaces, and
  platform-specific escaping on Windows and Unix.

Ordinary CLI argument parsing already receives separate arguments and is not the
affected lexer.

## F. OpenCode and Qoder installation

Relevant code: [installer](crates/zg-cli/src/install.rs),
[JSONC handling](crates/zg-cli/src/jsonc.rs) and
[installation tests](crates/zg/tests/install.rs).

- `[ ]` Select OpenCode's active config using explicit overrides, `XDG_CONFIG_HOME`,
  and existing JSON/JSONC files in main's precedence order. The explicit
  `OPENCODE_CONFIG` override is implemented, but the fallback is hardcoded to
  `~/.config/opencode/opencode.json` for both install and uninstall, with no
  `XDG_CONFIG_HOME` handling and no JSON/JSONC probe of the active file
  (`crates/zg-cli/src/install.rs:532-534,570-572`).
- `[ ]` Support trailing commas while preserving unrelated JSONC content.
  `crates/zg-cli/src/jsonc.rs:512` is a test named `rejects_trailing_commas`.
- `[ ]` GitHub Copilot and VS Code install targets (#71) are absent from the
  `Agent` enum (`crates/zg-cli/src/install.rs:42-60`).
- `[~]` Forced Qoder takeover must not inherit unrelated `alwaysAllow`
  permissions; uninstall must remove only owned permissions.
- `[!]` Windows Qoder discovery, environment-path normalization, and dual-file
  uninstall parity.

## G. CLI and public MCP behavior

Relevant code: [CLI parser](crates/zg-cli/src/lib.rs),
[binary](crates/zg/src/main.rs), and
[MCP transport](crates/zg-transport-mcp/src/lib.rs).

- `[ ]` Make search the default CLI action and support main's flag-based actions,
  including `--index`, `--status`, `--install`, and `--server`. Neither
  `crates/zg-cli/src/lib.rs` nor `crates/zg/src/main.rs` accepts those flags
  (no matches).
- `[ ]` Remove `apiKey` and `device` from public MCP search schemas and strip
  caller-supplied values. `api_key` is still a public field with schema
  validation (`crates/zg-transport-mcp/src/lib.rs:580,641,1087,1205`) and
  `device` still appears in the emitted schema (schema snapshots at
  `lib.rs:2095,2186`).
- `[~]` Consent behavior. A consent module exists and runs for both search and
  index (`crates/zg-transport-mcp/src/lib.rs:6,328,370`); interactive MCP
  elicitation, cancellation, and unsupported-host behavior are unverified.
- `[~]` Embedding concurrency controls (#159): `resolve_embedding_concurrency`
  and CLI plumbing exist (`crates/zg-engine/src/models/runtime.rs:186`), the
  environment equivalent does not (see A).
- `[!]` Bidirectional request forwarding through the stdio relay.

## H. CI and parity evidence

Relevant code: [Rust CI workflow](../.github/workflows/rust-ci.yml).

- `[x]` Scope concurrency by workflow, event, and pull request/commit (#100).
  `concurrency.group` is `${{ github.workflow }}-${{ github.event_name }}-${{ github.event.pull_request.number || github.sha }}`
  with `cancel-in-progress: true`.
- `[ ]` Add an enabled CPU-only smoke test for real local models using the built
  application, equivalent to #119. No `#[ignore]`-style real-model test or smoke
  job exists (`crates/zg/tests/`, `crates/zg-engine/tests/`, `rust-ci.yml`).
- `[ ]` Add the focused regressions listed above and run platform-specific cases
  on their target operating systems.
- `[ ]` Record the upstream revision for compatibility fixtures. `rust/compat/`
  currently holds one fixture (`cli/managed-rg-no-match.json`) plus the allowance
  files; the fixture schema and runner carry no oracle revision.

## Fixture and oracle health

Three oracle artifacts are recorded. The first two cannot detect drift from main
today; the third was added with this branch and can:

| Artifact | State |
| --- | --- |
| `rust/compat/cli/managed-rg-no-match.json` | One case; no recorded main revision, so it cannot tell which main behavior it was captured from. |
| `crates/zg-engine/src/models/tests/fixtures/catalog-main-oracle.json` | 14 rows, but the row shape omits `uri#revision`, `cacheFile`, `sources` (Hugging Face + ModelScope), and `artifacts`/`size`/`sha256`, i.e. it matches the Rust catalog rather than main. |
| `rust/compat/redaction/cases.json` (added with this branch) | 28 cases captured from `redactErrorText` at main `b1d0ce9`, with the oracle file/function and revision recorded. Asserted by `crates/zg-engine/tests/redaction_compat.rs`; removing one redaction pass makes it fail, so this oracle can detect drift. |

Catalog comparison method used here (kept for reuse):
`/home/ubuntu/zvec-grep-audit/catalog_diff.py` and `fixture_diff.py` parse
`src/engine/models/catalog.ts`, `crates/zg-engine/src/models/catalog.rs`, and the
fixture, then compare per model: `dimension`, `metric`, `pooling`, `normalize`,
`maxInputTokens`, `maxBatchSize`, `revision`, `dtype`, source mappings, and
artifact hashes. Result on 2026-09-20: the 14 model references match on both
sides with no semantic drift beyond the B1 findings above.

## A work plan (next slices, in order)

Scope: embed failure handling. Each slice should land with its regressions and,
where an oracle exists, a captured `compat/` fixture.

1. **Credential redaction parity — implemented on this branch.** Shared
   helper in `crates/zg-engine/src/redaction.rs`, wired into persisted job errors,
   CLI error rendering, and CLI progress text, with the oracle fixture in
   `compat/redaction/cases.json`. Remaining: decide the upstream PR scope (helper
   plus call sites plus fixture) and whether the daemon should also redact reply
   bodies rather than relying on the CLI layer, as main does.
2. **Transient-failure classification — implemented on this branch.**
   `classify_embedding_retry` gained 408, transport, permanent-remote and
   shared-local classes plus the `fail_fast` flag, and the pass now stops on
   shared failures instead of recording them per file. Remaining: capture the
   classification expectations as a fixture the way `compat/redaction` does, and
   re-run the failure-path regressions main uses (request counts for permanent
   versus transient failures, cancellation, and empty or unchanged input).
3. **Single preparation per operation.** Add an explicit model preparation step
   before embedding dispatch so a failed initialization is not re-attempted per
   queued item (main's #81/#150 behavior), while keeping the lazy handle design
   in `crates/zg-engine/src/models/runtime.rs:157-166` for resource lifetime.
4. **Diagnostics retention.** Carry provider status/context/cause and retry hints
   from `ModelError` through engine, daemon, MCP, CLI, and status output, with
   concise default output and full detail under debug.

Verification for each slice: workspace tests via `rust/scripts/check.sh`, request
counts for permanent versus transient failures, preparation failure across
multiple batches, cancellation and recovery, empty/unchanged input, and
diagnostic preservation/redaction.

## Completion

- `[ ]` Every gap has an implementation and appropriate verification evidence.
- `[ ]` Architecture-specific fixes have documented equivalent behavior and coverage.
- `[ ]` Formatting, linting, relevant workspace tests, platform checks, and packaged
  smoke tests pass.
- `[ ]` Record the final Rust commit and update this file with the closing
  commits/tests.
