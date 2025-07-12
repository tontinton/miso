pub(crate) fn has(text: &str, phrase: &str) -> bool {
    if phrase.is_empty() || phrase.len() > text.len() {
        return false;
    }

    let text_bytes = text.as_bytes();
    let phrase_bytes = phrase.as_bytes();
    let phrase_len = phrase_bytes.len();

    let first = phrase_bytes[0];
    let lower = first.to_ascii_lowercase();
    let upper = first.to_ascii_uppercase();

    let mut start = 0;
    while let Some(pos) = memchr::memchr2(lower, upper, &text_bytes[start..]) {
        let i = start + pos;

        if i + phrase_len <= text_bytes.len() {
            let mut j = 0;
            while j < phrase_len
                && text_bytes[i + j].to_ascii_lowercase() == phrase_bytes[j].to_ascii_lowercase()
            {
                j += 1;
            }
            let phrase_matched = j == phrase_len;

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
