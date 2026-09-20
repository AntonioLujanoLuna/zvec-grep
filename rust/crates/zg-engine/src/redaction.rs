//! Credential redaction for text that leaves the engine.
//!
//! Mirrors the five replacement passes of `redactErrorText` in the TypeScript
//! implementation (`src/engine/errors.ts`), which is the behavioral oracle for
//! the rewrite: CLI output, daemon replies and persisted job errors must hide the
//! same credential shapes. The captured oracle cases live in
//! `compat/redaction/cases.json` and are exercised by
//! `crates/zg-engine/tests/redaction_compat.rs`.
//!
//! Differences from the oracle, both accepted deliberately:
//!
//! - The URL-userinfo pass is implemented as a scanner instead of a regular
//!   expression because the `regex` crate has no lookbehind; the scanner rejects
//!   exactly the same candidates as the oracle's `(?<![a-z0-9+.-])` guard.
//! - Truncation counts Unicode scalar values rather than UTF-16 code units, so a
//!   string with astral characters near the limit may be cut one character
//!   earlier or later than the oracle.

use std::sync::OnceLock;

use regex::Regex;

/// Character used by both implementations to mark truncated text.
const ELLIPSIS: char = '…';

/// Redacts credential shapes in `value` and truncates it to `max_chars`.
///
/// `max_chars` follows the oracle: a longer value keeps `max_chars - 1`
/// characters and the ellipsis, and a limit of zero keeps everything but the
/// final character.
#[must_use]
pub fn redact_text(value: &str, max_chars: usize) -> String {
    let userinfo = redact_url_userinfo(value);
    let authorization = authorization_pattern().replace_all(&userinfo, "$1[redacted]");
    let scheme_credentials =
        scheme_credential_pattern().replace_all(&authorization, "$1 [redacted]");
    let assigned = assigned_credential_pattern().replace_all(&scheme_credentials, "$1[redacted]");
    let api_keys = api_key_pattern().replace_all(&assigned, "sk-[redacted]");
    truncate(&api_keys, max_chars)
}

/// `(?<![a-z0-9+.-])([a-z][a-z0-9+.-]*:\/\/)[^/\s@]+@`, as a scanner.
fn redact_url_userinfo(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut output = String::with_capacity(value.len());
    let mut copied = 0;
    let mut search = 0;

    while let Some(relative) = value[search..].find("://") {
        let separator = search + relative;
        let mut scheme_start = separator;
        while scheme_start > 0 && is_scheme_character(bytes[scheme_start - 1]) {
            scheme_start -= 1;
        }
        let scheme = &value[scheme_start..separator];
        let preceded_by_scheme_character =
            scheme_start > 0 && is_scheme_character(bytes[scheme_start - 1]);
        if scheme.is_empty()
            || !scheme.as_bytes()[0].is_ascii_alphabetic()
            || preceded_by_scheme_character
        {
            search = separator + "://".len();
            continue;
        }

        let userinfo_start = separator + "://".len();
        let mut index = userinfo_start;
        let mut terminator = None;
        while let Some(character) = value[index..].chars().next() {
            if character == '@' {
                terminator = Some(index);
                break;
            }
            if character == '/' || character.is_whitespace() {
                break;
            }
            index += character.len_utf8();
        }

        match terminator {
            Some(at) if at > userinfo_start => {
                output.push_str(&value[copied..userinfo_start]);
                output.push_str("[redacted]@");
                copied = at + 1;
                search = at + 1;
            }
            _ => search = userinfo_start,
        }
    }

    output.push_str(&value[copied..]);
    output
}

fn is_scheme_character(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-')
}

/// `(["']?authorization["']?\s*[:=]\s*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\r\n]+)`
fn authorization_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(["']?authorization["']?\s*[:=]\s*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\r\n]+)"#,
        )
        .expect("authorization redaction pattern must compile")
    })
}

/// `\b(Bearer|Basic)[^\S\r\n]+(?:"(?:\\.|[^"\r\n])*"|'(?:\\.|[^'\r\n])*'|[^\s"',;]+)`
fn scheme_credential_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)\b(Bearer|Basic)[^\S\r\n]+(?:"(?:\\.|[^"\r\n])*"|'(?:\\.|[^'\r\n])*'|[^\s"',;]+)"#,
        )
        .expect("scheme credential redaction pattern must compile")
    })
}

/// `(["']?(?:api[_ -]?key|(?:access[_ -]?|refresh[_ -]?|id[_ -]?)?token|authorization|password|secret)["']?\s*[:=]\s*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s&]+)`
fn assigned_credential_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r#"(?i)(["']?(?:api[_ -]?key|(?:access[_ -]?|refresh[_ -]?|id[_ -]?)?token|authorization|password|secret)["']?\s*[:=]\s*)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s&]+)"#,
        )
        .expect("assigned credential redaction pattern must compile")
    })
}

/// `\bsk-[A-Za-z0-9_-]{8,}\b`, with the oracle's ASCII word boundaries.
fn api_key_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)(?-u:\b)sk-[A-Za-z0-9_-]{8,}(?-u:\b)")
            .expect("API key redaction pattern must compile")
    })
}

fn truncate(value: &str, max_chars: usize) -> String {
    let length = value.chars().count();
    if length <= max_chars {
        return value.to_owned();
    }
    let keep = max_chars.checked_sub(1).unwrap_or_else(|| length - 1);
    let mut truncated = String::with_capacity(value.len());
    truncated.extend(value.chars().take(keep));
    truncated.push(ELLIPSIS);
    truncated
}

#[cfg(test)]
mod tests {
    use super::redact_text;

    #[test]
    fn redacts_assigned_credentials() {
        assert_eq!(
            redact_text("fetch failed token=model-download-secret", 512),
            "fetch failed token=[redacted]"
        );
        assert_eq!(
            redact_text("api_key=abc123 status=401", 512),
            "api_key=[redacted] status=401"
        );
        assert_eq!(
            redact_text("api-key: 'quoted-secret'", 512),
            "api-key: [redacted]"
        );
        assert_eq!(
            redact_text(r#"{"api key": "json-secret", "other": 1}"#, 512),
            r#"{"api key": [redacted], "other": 1}"#
        );
        assert_eq!(
            redact_text("password=hunter2 secret=s3cr3t", 512),
            "password=[redacted] secret=[redacted]"
        );
        assert_eq!(
            redact_text("access_token=a1 refresh_token=b2 id_token=c3", 512),
            "access_token=[redacted] refresh_token=[redacted] id_token=[redacted]"
        );
        assert_eq!(
            redact_text("https://host/path?token=abc&x=1", 512),
            "https://host/path?token=[redacted]&x=1"
        );
        assert_eq!(
            redact_text("token=\"quoted value with space\"", 512),
            "token=[redacted]"
        );
        assert_eq!(redact_text("secret: 'sq'", 512), "secret: [redacted]");
    }

    #[test]
    fn redacts_authorization_and_scheme_credentials() {
        assert_eq!(
            redact_text("Authorization: Bearer sk-abcdefgh12345", 512),
            "Authorization: [redacted]"
        );
        assert_eq!(
            redact_text("authorization=Basic dXNlcjpwYXNz", 512),
            "authorization=[redacted]"
        );
        assert_eq!(
            redact_text("Bearer standalone-token-here", 512),
            "Bearer [redacted]"
        );
        assert_eq!(redact_text("Basic abc", 512), "Basic [redacted]");
        assert_eq!(
            redact_text("Bearer dXNlcjpwYXNz; next", 512),
            "Bearer [redacted]; next"
        );
        assert_eq!(
            redact_text("Authorization: \"Bearer quoted\" trailing", 512),
            "Authorization: [redacted] trailing"
        );
    }

    #[test]
    fn redacts_url_userinfo_and_standalone_api_keys() {
        assert_eq!(
            redact_text("GET https://user:pa%40ss@example.com/path", 512),
            "GET https://[redacted]@example.com/path"
        );
        assert_eq!(
            redact_text("url=https://user@example.com/x", 512),
            "url=https://[redacted]@example.com/x"
        );
        assert_eq!(
            redact_text("endpoint=http://user:pass@localhost:8080/v1", 512),
            "endpoint=http://[redacted]@localhost:8080/v1"
        );
        assert_eq!(
            redact_text("myapp://a:b@c/d then app://d:e@f/g", 512),
            "myapp://[redacted]@c/d then app://[redacted]@f/g"
        );
        assert_eq!(
            redact_text(
                "GET https://user@example.com/x and https://other@example.com/y",
                512
            ),
            "GET https://[redacted]@example.com/x and https://[redacted]@example.com/y"
        );
        assert_eq!(
            redact_text("key sk-1234567890abcdefgh trailing", 512),
            "key sk-[redacted] trailing"
        );
    }

    #[test]
    fn leaves_text_without_credentials_unchanged() {
        assert_eq!(
            redact_text("no credentials here", 512),
            "no credentials here"
        );
        assert_eq!(
            redact_text("1http://user@host/path", 512),
            "1http://user@host/path"
        );
        assert_eq!(
            redact_text("mailto:user@example.com", 512),
            "mailto:user@example.com"
        );
        assert_eq!(redact_text("s3://bucket/data", 512), "s3://bucket/data");
    }

    #[test]
    fn truncates_like_the_oracle() {
        let long = format!("{} token=very-last-secret", "x".repeat(600));
        let truncated = redact_text(&long, 512);
        assert_eq!(truncated.chars().count(), 512);
        assert!(truncated.ends_with('…'));
        assert_eq!(redact_text("token=short-secret", 8), "token=[…");
        assert_eq!(redact_text("token=secret", 0), "token=[redacted…");
        assert_eq!(redact_text("token=secret", 512), "token=[redacted]");
    }
}
