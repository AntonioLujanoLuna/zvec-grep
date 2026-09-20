//! Bounded model-artifact downloads.
//!
//! Main bounds two phases of an artifact download — the wait for response
//! headers and the idle gap between body chunks — so a stalled connection fails
//! instead of holding an indexing run open (`DEFAULT_RESPONSE_HEADER_TIMEOUT_MS`
//! and `DEFAULT_READ_IDLE_TIMEOUT_MS`, `src/engine/models/artifact-downloader.ts`).
//! Every local backend goes through this module, and the budgets are injectable
//! so tests can exercise both deadlines without waiting out main's defaults.

use std::time::Duration;

use futures_util::{Stream, StreamExt};

use crate::{EngineError, models::error::ModelError};

/// Main's `DEFAULT_RESPONSE_HEADER_TIMEOUT_MS`.
pub(crate) const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(10);
/// Main's `DEFAULT_READ_IDLE_TIMEOUT_MS`.
pub(crate) const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// The two deadlines a model-artifact download must respect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DownloadTimeouts {
    /// How long the wait for response headers may take.
    pub(crate) response_header: Duration,
    /// How long the gap between two body chunks may take.
    pub(crate) read_idle: Duration,
}

impl Default for DownloadTimeouts {
    fn default() -> Self {
        Self {
            response_header: RESPONSE_HEADER_TIMEOUT,
            read_idle: READ_IDLE_TIMEOUT,
        }
    }
}

/// The deadlines every local backend applies, mirroring main's defaults.
pub(crate) const DOWNLOAD_TIMEOUTS: DownloadTimeouts = DownloadTimeouts {
    response_header: RESPONSE_HEADER_TIMEOUT,
    read_idle: READ_IDLE_TIMEOUT,
};

impl DownloadTimeouts {
    /// Sends an artifact request, failing when headers do not arrive in time.
    ///
    /// A timeout here covers the whole phase main bounds, connection included,
    /// and reports the URL and the budget it applied.
    pub(crate) async fn send(
        &self,
        url: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, ModelError> {
        match tokio::time::timeout(self.response_header, request.send()).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(ModelError::storage_failure(format!(
                "Unable to request '{url}'"
            ))
            .with_cause(error)),
            Err(_) => Err(self.header_deadline_error(url)),
        }
    }

    /// Reads the next body chunk, failing when chunks stop arriving.
    ///
    /// `Ok(None)` is the end of the body, matching `StreamExt::next`.
    pub(crate) async fn next_chunk<T, S>(
        &self,
        stream: &mut S,
        url: &str,
    ) -> Result<Option<T>, ModelError>
    where
        S: Stream<Item = Result<T, reqwest::Error>> + Unpin,
    {
        match tokio::time::timeout(self.read_idle, stream.next()).await {
            Ok(Some(Ok(chunk))) => Ok(Some(chunk)),
            Ok(Some(Err(error))) => Err(ModelError::storage_failure(format!(
                "Unable to read '{url}'"
            ))
            .with_cause(error)),
            Ok(None) => Ok(None),
            Err(_) => Err(self.read_idle_error(url)),
        }
    }

    fn header_deadline_error(&self, url: &str) -> ModelError {
        let budget = self.response_header.as_millis();
        ModelError::new(
            EngineError::STORAGE_FAILURE,
            format!("No response headers for '{url}' within {budget} ms"),
            Some(format!("url={url} responseHeaderMs={budget}")),
        )
    }

    fn read_idle_error(&self, url: &str) -> ModelError {
        let budget = self.read_idle.as_millis();
        ModelError::new(
            EngineError::STORAGE_FAILURE,
            format!("Download of '{url}' stalled: no body data within {budget} ms"),
            Some(format!("url={url} readIdleMs={budget}")),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    /// Accepts one connection, consumes the request, writes `response` and then
    /// holds the socket open for `hold` without closing it.
    async fn stub_server(response: Option<&'static [u8]>, hold: Duration) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stub listener");
        let address = listener.local_addr().expect("stub address");
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await;
            if let Some(bytes) = response {
                let _ = socket.write_all(bytes).await;
                let _ = socket.flush().await;
            }
            tokio::time::sleep(hold).await;
        });
        format!("http://{address}/artifact")
    }

    fn client() -> reqwest::Client {
        reqwest::Client::new()
    }

    #[test]
    fn defaults_match_main() {
        assert_eq!(RESPONSE_HEADER_TIMEOUT, Duration::from_secs(10));
        assert_eq!(READ_IDLE_TIMEOUT, Duration::from_secs(30));
        assert_eq!(
            DownloadTimeouts::default(),
            DownloadTimeouts {
                response_header: RESPONSE_HEADER_TIMEOUT,
                read_idle: READ_IDLE_TIMEOUT,
            }
        );
    }

    #[tokio::test]
    async fn header_deadline_stops_a_server_that_never_answers() {
        let url = stub_server(None, Duration::from_secs(30)).await;
        let timeouts = DownloadTimeouts {
            response_header: Duration::from_millis(150),
            read_idle: Duration::from_secs(5),
        };
        let started = Instant::now();
        let error = timeouts
            .send(&url, client().get(&url))
            .await
            .expect_err("a server that never answers must not be awaited forever");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "the header deadline must bound the wait, took {:?}",
            started.elapsed()
        );
        assert_eq!(error.code(), EngineError::STORAGE_FAILURE);
        let message = error.into_engine_error().message().to_owned();
        assert!(message.contains("No response headers"), "{message}");
        assert!(message.contains("responseHeaderMs=150"), "{message}");
    }

    #[tokio::test]
    async fn complete_responses_are_streamed_to_the_end() {
        let url = stub_server(
            Some(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"),
            Duration::from_millis(50),
        )
        .await;
        let timeouts = DownloadTimeouts {
            response_header: Duration::from_secs(2),
            read_idle: Duration::from_secs(2),
        };
        let response = timeouts
            .send(&url, client().get(&url))
            .await
            .expect("complete response");
        assert!(response.status().is_success());
        let mut stream = response.bytes_stream();
        let chunk = timeouts
            .next_chunk(&mut stream, &url)
            .await
            .expect("first chunk")
            .expect("first chunk must be present");
        assert_eq!(&chunk[..], b"hello");
        assert!(
            timeouts
                .next_chunk(&mut stream, &url)
                .await
                .expect("end of body")
                .is_none(),
            "the end of the body must be reported as end of stream"
        );
    }

    #[tokio::test]
    async fn read_idle_deadline_stops_a_body_that_stops_arriving() {
        let url = stub_server(
            Some(b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\n\r\npartial"),
            Duration::from_secs(30),
        )
        .await;
        let timeouts = DownloadTimeouts {
            response_header: Duration::from_secs(2),
            read_idle: Duration::from_millis(150),
        };
        let response = timeouts
            .send(&url, client().get(&url))
            .await
            .expect("headers arrive");
        let mut stream = response.bytes_stream();
        let started = Instant::now();
        let first = timeouts
            .next_chunk(&mut stream, &url)
            .await
            .expect("first chunk");
        assert_eq!(first.as_deref(), Some(&b"partial"[..]));
        let error = timeouts
            .next_chunk::<_, _>(&mut stream, &url)
            .await
            .expect_err("a stalled body must not be awaited forever");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "the read-idle deadline must bound the wait, took {:?}",
            started.elapsed()
        );
        let message = error.into_engine_error().message().to_owned();
        assert!(message.contains("stalled"), "{message}");
        assert!(message.contains("readIdleMs=150"), "{message}");
    }

    #[tokio::test]
    async fn a_body_cut_short_reports_the_transport_failure() {
        let url = stub_server(
            Some(b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\n\r\npartial"),
            Duration::from_millis(50),
        )
        .await;
        let timeouts = DownloadTimeouts {
            response_header: Duration::from_secs(2),
            read_idle: Duration::from_secs(2),
        };
        let response = timeouts
            .send(&url, client().get(&url))
            .await
            .expect("headers arrive");
        let mut stream = response.bytes_stream();
        assert!(
            timeouts
                .next_chunk(&mut stream, &url)
                .await
                .expect("first chunk")
                .is_some()
        );
        // The stub closes with less than the declared length, so the next read is
        // an error rather than a stalled socket.
        let error = loop {
            match timeouts.next_chunk::<_, _>(&mut stream, &url).await {
                Ok(None) => panic!("a truncated body must not look complete"),
                Ok(Some(_)) => {}
                Err(error) => break error,
            }
        };
        assert_eq!(error.code(), EngineError::STORAGE_FAILURE);
        let message = error.into_engine_error().message().to_owned();
        assert!(message.contains("Unable to read"), "{message}");
    }
}
