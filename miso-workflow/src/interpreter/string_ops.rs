use std::cell::RefCell;
use std::collections::HashMap;

use color_eyre::eyre::{Result, bail, eyre};
use miso_workflow_types::value::Value;
use regex::Regex;

thread_local! {
    /// Cache compiled regexes per thread to avoid recompilation overhead
    /// when the same pattern is used repeatedly (e.g., in row-by-row evaluation).
    static REGEX_CACHE: RefCell<HashMap<String, Regex>> = RefCell::new(HashMap::new());
}

pub(crate) fn extract(source: &str, pattern: &str, capture_group: i64) -> Result<Option<Value>> {
    if capture_group < 0 {
        bail!("capture group index must be non-negative");
    }

    REGEX_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let re = if let Some(re) = cache.get(pattern) {
            re
        } else {
            let compiled =
                Regex::new(pattern).map_err(|e| eyre!("invalid regex pattern: {}", e))?;
            cache.insert(pattern.to_string(), compiled);
            cache.get(pattern).unwrap()
        };

        let Some(caps) = re.captures(source) else {
            return Ok(Some(Value::Null));
        };

        let group_idx = capture_group as usize;
        match caps.get(group_idx) {
            Some(m) => Ok(Some(Value::String(m.as_str().to_string()))),
            None => Ok(Some(Value::Null)),
        }
    })
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

    #[test]
    fn test_extract_basic() {
        let result = extract("error code: 123", "code: (\\d+)", 1).unwrap();
        assert_eq!(result, Some(Value::String("123".to_string())));
    }

    #[test]
    fn test_extract_no_match() {
        let result = extract("no numbers", "(\\d+)", 1).unwrap();
        assert_eq!(result, Some(Value::Null));
    }

    #[test]
    fn test_extract_invalid_group() {
        let result = extract("123", "(\\d+)", 5).unwrap();
        assert_eq!(result, Some(Value::Null));
    }

    #[test]
    fn test_extract_group_zero() {
        let result = extract("abc 123 def", "\\d+", 0).unwrap();
        assert_eq!(result, Some(Value::String("123".to_string())));
    }

    #[test]
    fn test_extract_multiple_groups() {
        let result = extract("user=john, id=42", "user=(\\w+), id=(\\d+)", 2).unwrap();
        assert_eq!(result, Some(Value::String("42".to_string())));
    }

    #[test]
    fn test_extract_negative_group() {
        let result = extract("test", "(\\w+)", -1);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_invalid_regex() {
        let result = extract("test", "([", 1);
        assert!(result.is_err());
    }
}
