use color_eyre::eyre::{Result, bail};
use miso_workflow_types::value::Value;
use regex::Regex;

pub(crate) fn extract(source: &str, pattern: &str, capture_group: i64) -> Result<Option<Value>> {
    if capture_group < 0 {
        bail!("capture group index must be non-negative");
    }

    let re = Regex::new(pattern)
        .map_err(|e| color_eyre::eyre::eyre!("invalid regex pattern: {}", e))?;

    let Some(caps) = re.captures(source) else {
        return Ok(Some(Value::Null));
    };

    let group_idx = capture_group as usize;
    match caps.get(group_idx) {
        Some(m) => Ok(Some(Value::String(m.as_str().to_string()))),
        None => Ok(Some(Value::Null)),
    }
}

/// Generic function to find a phrase in text using configurable search and comparison callbacks.
fn find_phrase_with_boundaries<F, G>(
    text: &str,
    phrase: &str,
    find_first: F,
    compare_match: G,
) -> bool
where
    F: Fn(u8, &[u8]) -> Option<usize>,
    G: Fn(&[u8], &[u8]) -> bool,
{
    if phrase.is_empty() || phrase.len() > text.len() {
        return false;
    }

    let text_bytes = text.as_bytes();
    let phrase_bytes = phrase.as_bytes();
    let phrase_len = phrase_bytes.len();
    let first = phrase_bytes[0];

    let mut start = 0;

    while let Some(pos) = find_first(first, &text_bytes[start..]) {
        let i = start + pos;

        if i + phrase_len <= text_bytes.len() {
            let phrase_matched = compare_match(&text_bytes[i..i + phrase_len], phrase_bytes);

            if phrase_matched
                && (i == 0 || !text_bytes[i - 1].is_ascii_alphanumeric())
                && (i + phrase_len == text_bytes.len()
                    || !text_bytes[i + phrase_len].is_ascii_alphanumeric())
            {
                return true;
            }
        }
        start = i + 1;
    }
    false
}

/// Case-insensitive phrase search with word boundaries.
pub(crate) fn has(text: &str, phrase: &str) -> bool {
    if phrase.is_empty() {
        return false;
    }

    let first = phrase.as_bytes()[0];
    let lower = first.to_ascii_lowercase();
    let upper = first.to_ascii_uppercase();

    find_phrase_with_boundaries(
        text,
        phrase,
        |_, haystack| memchr::memchr2(lower, upper, haystack),
        |text_slice, phrase_bytes| {
            text_slice.len() == phrase_bytes.len()
                && text_slice
                    .iter()
                    .zip(phrase_bytes.iter())
                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
        },
    )
}

/// Case-sensitive phrase search with word boundaries.
pub(crate) fn has_cs(text: &str, phrase: &str) -> bool {
    find_phrase_with_boundaries(text, phrase, memchr::memchr, |text_slice, phrase_bytes| {
        text_slice == phrase_bytes
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_case_insensitive() {
        assert!(has("Hello World", "hello"));
        assert!(has("Hello World", "WORLD"));
        assert!(!has("Hello World", "ell"));
        assert!(!has("Hello World", "wor"));
        assert!(has("test-case", "test"));
        assert!(has("test-case", "case"));
    }

    #[test]
    fn test_has_case_sensitive() {
        assert!(has_cs("Hello World", "Hello"));
        assert!(!has_cs("Hello World", "hello"));
        assert!(has_cs("Hello World", "World"));
        assert!(!has_cs("Hello World", "world"));
        assert!(!has_cs("Hello World", "ell"));
    }

    #[test]
    fn test_has_edge_cases() {
        assert!(!has("", "test"));
        assert!(!has("test", ""));
        assert!(!has("short", "longer"));
        assert!(has("exact", "exact"));
        assert!(has_cs("exact", "exact"));
    }
}
