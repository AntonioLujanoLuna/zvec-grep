//! Publication and verification of downloaded model cache artifacts.

use std::{fs, io, io::Read, path::Path};

use sha2::{Digest, Sha256};

use crate::{
    EngineError, EngineResult,
    models::{catalog::ArtifactSpec, error::ModelError},
    utils::sync_directory,
};

/// Bytes hashed per read while verifying an artifact.
const VERIFY_CHUNK_BYTES: usize = 1024 * 1024;

/// Returns whether a cached artifact exists and matches its catalog entry.
///
/// Artifacts without a catalog entry (files a backend synthesises locally) only
/// have to be non-empty. A present but unverifiable artifact is reported as
/// missing so the caller refetches it, which is how a truncated or substituted
/// cache file is recovered from.
pub(super) async fn is_verified_cached_file(path: &Path, specs: &[ArtifactSpec]) -> bool {
    if !is_non_empty_file(path).await {
        return false;
    }
    match artifact_spec(specs, path) {
        Some(spec) => verify_artifact(path, spec).await.is_ok(),
        None => true,
    }
}

/// Verifies a freshly downloaded artifact, demanding the recorded bytes.
///
/// A mismatch here is a source or transport fault rather than a stale cache, so
/// it fails loudly instead of publishing the file; the caller removes it.
pub(super) async fn verify_downloaded_artifact(
    path: &Path,
    specs: &[ArtifactSpec],
    model: &str,
    artifact: &str,
) -> Result<(), ModelError> {
    let Some(spec) = artifact_spec(specs, path) else {
        return Ok(());
    };
    if let Err(error) = verify_artifact(path, spec).await {
        return Err(ModelError::new(
            EngineError::STORAGE_FAILURE,
            "Model artifact failed its integrity check",
            Some(format!("model={model} artifact={artifact}")),
        )
        .with_cause(error));
    }
    Ok(())
}

async fn is_non_empty_file(path: &Path) -> bool {
    tokio::fs::metadata(path)
        .await
        .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
}

/// Returns the catalog entry for a cached artifact, matched by file name.
///
/// Catalog paths may include a directory (for example `onnx/model_q4.onnx`), so
/// both the full path and its final segment are considered.
pub(super) fn artifact_spec<'spec>(
    specs: &'spec [ArtifactSpec],
    path: &Path,
) -> Option<&'spec ArtifactSpec> {
    let name = path.file_name()?.to_str()?;
    specs.iter().find(|spec| {
        spec.path == name || spec.path.rsplit_once('/').map(|(_, file)| file) == Some(name)
    })
}

/// Verifies a cached or freshly downloaded artifact against its catalog entry.
///
/// Mirrors main's integrity check: the recorded size and SHA-256 must both match,
/// so a truncated, overwritten or substituted cache file cannot be used as a
/// model. The file is hashed in chunks, keeping memory flat for multi-hundred
/// megabyte artifacts.
pub(super) async fn verify_artifact(path: &Path, spec: &ArtifactSpec) -> EngineResult<()> {
    let path = path.to_path_buf();
    let spec = *spec;
    tokio::task::spawn_blocking(move || verify_artifact_sync(&path, &spec))
        .await
        .map_err(|error| {
            EngineError::internal(format!("artifact verification task failed: {error}"))
        })?
}

fn verify_artifact_sync(path: &Path, spec: &ArtifactSpec) -> EngineResult<()> {
    let metadata = fs::metadata(path).map_err(|error| {
        EngineError::from_io(
            format!("Unable to inspect model artifact '{}'", path.display()),
            &error,
        )
    })?;
    let actual_size = metadata.len();
    let mismatch = |actual_sha256: &str| {
        EngineError::internal(format!(
            "Integrity check failed for '{}': expected {} bytes/{} received {} bytes/{}",
            spec.path, spec.size, spec.sha256, actual_size, actual_sha256
        ))
    };
    if actual_size != spec.size {
        return Err(mismatch("unread"));
    }

    let mut file = fs::File::open(path).map_err(|error| {
        EngineError::from_io(
            format!("Unable to read model artifact '{}'", path.display()),
            &error,
        )
    })?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; VERIFY_CHUNK_BYTES];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            EngineError::from_io(
                format!("Unable to read model artifact '{}'", path.display()),
                &error,
            )
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let actual_sha256 = hex::encode(digest.finalize());
    if !actual_sha256.eq_ignore_ascii_case(spec.sha256) {
        return Err(mismatch(&actual_sha256));
    }
    Ok(())
}

/// Publishes a downloaded model artifact and syncs its file and cache directory.
///
/// Both paths must be in the same directory. The caller must flush buffered writes
/// before calling, and owns cleanup if publication fails. Existing regular files
/// are replaced; the prepared file's permissions are retained. Blocking filesystem
/// operations run off the async executor, without buffering the file in memory.
/// The temporary file must be writable. Once blocking work starts, cancelling the
/// future does not stop publication; cancellation does not imply an unchanged target.
pub(super) async fn publish_downloaded_file(
    temporary: &Path,
    destination: &Path,
) -> EngineResult<()> {
    let temporary = temporary.to_path_buf();
    let destination = destination.to_path_buf();
    tokio::task::spawn_blocking(move || publish_downloaded_file_sync(&temporary, &destination))
        .await
        .map_err(|error| EngineError::internal(format!("file publication task failed: {error}")))?
}

fn publish_downloaded_file_sync(temporary: &Path, destination: &Path) -> EngineResult<()> {
    let failure = |operation: &str, source: &io::Error| {
        EngineError::from_io(
            format!(
                "file publication failed {operation} from '{}' to '{}'",
                temporary.display(),
                destination.display()
            ),
            source,
        )
    };
    let parent = |path: &Path| -> io::Result<_> {
        if !path.file_name().is_some_and(|name| {
            path.as_os_str()
                .as_encoded_bytes()
                .ends_with(name.as_encoded_bytes())
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "publication paths must end with a file name",
            ));
        }
        fs::canonicalize(
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )
    };
    let directory = parent(destination).map_err(|e| {
        failure(
            "while resolving destination (destination not published)",
            &e,
        )
    })?;
    let temporary_directory = parent(temporary).map_err(|e| {
        failure(
            "while resolving temporary file (destination not published)",
            &e,
        )
    })?;
    if temporary_directory != directory {
        return Err(failure(
            "during path validation (destination not published)",
            &io::Error::new(
                io::ErrorKind::InvalidInput,
                "temporary file and destination must share a parent directory",
            ),
        ));
    }
    let metadata = fs::symlink_metadata(temporary).map_err(|e| {
        failure(
            "while reading temporary file (destination not published)",
            &e,
        )
    })?;
    if !metadata.is_file() {
        return Err(failure(
            "during path validation (destination not published)",
            &io::Error::new(
                io::ErrorKind::InvalidInput,
                "temporary file must be a regular file",
            ),
        ));
    }
    validate_destination(destination)
        .map_err(|e| failure("while checking destination (destination not published)", &e))?;
    sync_directory(&directory)?;
    // Windows FlushFileBuffers requires write access. Opening with no truncate
    // also leaves an unpublished temporary file available for caller cleanup.
    let file = fs::OpenOptions::new()
        .write(true)
        .open(temporary)
        .map_err(|e| {
            failure(
                "while opening temporary file (destination not published)",
                &e,
            )
        })?;
    file.sync_all()
        .map_err(|e| failure("during file sync (destination not published)", &e))?;
    drop(file);
    fs::rename(temporary, destination)
        .map_err(|e| failure("during rename (outcome unknown)", &e))?;
    sync_directory(&directory).map_err(|error| {
        EngineError::from_report(crate::ErrorReport {
            message: format!(
                "file publication to '{}' completed; durability unconfirmed: {error}",
                destination.display()
            ),
            ..error.into_report()
        })
    })
}

fn validate_destination(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => {
            #[cfg(windows)]
            if metadata.permissions().readonly() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "destination is readonly",
                ));
            }
            Ok(())
        }
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "destination must be a regular file, not a symlink or directory",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, ffi::OsString, fs::File, io::Read};

    use super::*;

    fn assert_entries(directory: &Path, expected: &[&str]) {
        let entries: BTreeSet<_> = fs::read_dir(directory)
            .expect("read directory")
            .map(|entry| entry.expect("directory entry").file_name())
            .collect();
        assert_eq!(entries, expected.iter().map(OsString::from).collect());
    }

    #[tokio::test]
    async fn publishes_streamed_files_and_preserves_open_readers() {
        use tokio::io::AsyncWriteExt;

        let root = tempfile::tempdir().expect("temporary directory");
        let destination = root.path().join("模型.bin");
        let temporary = root.path().join("模型.bin.part");
        for contents in ["original", "replacement 中文 😀"] {
            let mut reader = File::open(&destination).ok();
            let mut output = tokio::fs::File::create(&temporary)
                .await
                .expect("stream destination");
            for chunk in contents.as_bytes().chunks(3) {
                output.write_all(chunk).await.expect("stream chunk");
            }
            output.flush().await.expect("flush stream");
            drop(output);
            publish_downloaded_file(&temporary, &destination)
                .await
                .expect("publish stream");
            assert_eq!(
                fs::read(&destination).expect("published contents"),
                contents.as_bytes()
            );
            if let Some(reader) = &mut reader {
                let mut previous = String::new();
                reader
                    .read_to_string(&mut previous)
                    .expect("existing reader");
                assert_eq!(previous, "original");
            }
            assert_entries(root.path(), &["模型.bin"]);
        }
    }

    #[tokio::test]
    async fn publication_rejects_invalid_paths_without_removing_temporary_files() {
        let root = tempfile::tempdir().expect("temporary directory");
        let other = root.path().join("other");
        fs::create_dir(&other).expect("other directory");
        let temporary = root.path().join("record.part");
        let destination = root.path().join("record");
        fs::write(&temporary, b"prepared").expect("temporary contents");
        fs::write(&destination, b"original").expect("existing destination");

        for (source, target) in [
            (temporary.clone(), other.join("record")),
            (temporary.clone(), other.clone()),
            (other, destination.clone()),
            (root.path().join("missing"), destination.clone()),
            (temporary.clone(), destination.join(".")),
        ] {
            let error = publish_downloaded_file(&source, &target)
                .await
                .expect_err("invalid publication");
            assert!(
                error.message().contains("destination not published"),
                "{error}"
            );
            assert_eq!(
                fs::read(&temporary).expect("unpublished contents"),
                b"prepared"
            );
            assert_eq!(
                fs::read(&destination).expect("unchanged destination"),
                b"original"
            );
        }
        assert_entries(root.path(), &["other", "record", "record.part"]);
    }

    /// `sha256("abc")`, the payload the verification tests write.
    const DIGEST_OF_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    fn spec() -> ArtifactSpec {
        ArtifactSpec {
            path: "onnx/model_q4.onnx",
            size: 3,
            sha256: DIGEST_OF_ABC,
        }
    }

    #[test]
    fn records_are_matched_by_path_or_file_name() {
        let specs = [spec()];
        assert_eq!(
            artifact_spec(&specs, Path::new("onnx/model_q4.onnx")).map(|spec| spec.path),
            Some("onnx/model_q4.onnx")
        );
        assert_eq!(
            artifact_spec(&specs, Path::new("/cache/models/model_q4.onnx")).map(|spec| spec.path),
            Some("onnx/model_q4.onnx")
        );
        assert!(artifact_spec(&specs, Path::new("/cache/models/other.onnx")).is_none());
    }

    #[tokio::test]
    async fn artifact_matching_the_recorded_bytes_verifies() {
        let root = tempfile::tempdir().expect("temporary directory");
        let path = root.path().join("model_q4.onnx");
        fs::write(&path, b"abc").expect("artifact contents");
        verify_artifact(&path, &spec())
            .await
            .expect("recorded bytes must verify");
        assert!(is_verified_cached_file(&path, &[spec()]).await);
    }

    #[tokio::test]
    async fn artifact_with_other_contents_reports_both_checksums() {
        let root = tempfile::tempdir().expect("temporary directory");
        let path = root.path().join("model_q4.onnx");
        fs::write(&path, b"abd").expect("artifact contents");
        let error = verify_artifact(&path, &spec())
            .await
            .expect_err("substituted contents must not verify");
        assert!(
            error.message().contains("Integrity check failed"),
            "{error}"
        );
        assert!(error.message().contains(DIGEST_OF_ABC), "{error}");
        assert!(!is_verified_cached_file(&path, &[spec()]).await);
    }

    #[tokio::test]
    async fn artifact_with_other_size_reports_the_expected_size() {
        let root = tempfile::tempdir().expect("temporary directory");
        let path = root.path().join("model_q4.onnx");
        fs::write(&path, b"abcd").expect("artifact contents");
        let error = verify_artifact(&path, &spec())
            .await
            .expect_err("truncated or extended contents must not verify");
        assert!(error.message().contains("expected 3 bytes"), "{error}");
        assert!(error.message().contains("received 4 bytes"), "{error}");
        assert!(!is_verified_cached_file(&path, &[spec()]).await);
    }

    #[tokio::test]
    async fn artifacts_without_a_record_only_have_to_be_present() {
        let root = tempfile::tempdir().expect("temporary directory");
        let synthesised = root.path().join("tokenizer_config.json");
        fs::write(
            &synthesised,
            b"{\"tokenizer_class\":\"PreTrainedTokenizer\"}",
        )
        .expect("contents");
        assert!(is_verified_cached_file(&synthesised, &[spec()]).await);
        assert!(!is_verified_cached_file(&root.path().join("missing.json"), &[spec()]).await);
        let empty = root.path().join("empty.json");
        fs::write(&empty, b"").expect("empty contents");
        assert!(!is_verified_cached_file(&empty, &[]).await);
    }

    #[tokio::test]
    async fn downloaded_artifact_failure_names_the_model_and_artifact() {
        let root = tempfile::tempdir().expect("temporary directory");
        let path = root.path().join("model_q4.onnx");
        fs::write(&path, b"abd").expect("artifact contents");
        let error = verify_downloaded_artifact(&path, &[spec()], "local/test", "model_q4.onnx")
            .await
            .expect_err("mismatched download must fail");
        assert_eq!(error.code(), EngineError::STORAGE_FAILURE);
        let context = error.context().unwrap_or_default();
        assert!(context.contains("model=local/test"), "{context}");
        assert!(context.contains("artifact=model_q4.onnx"), "{context}");
    }
}
