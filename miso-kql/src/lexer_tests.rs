use logos::Logos;
use test_case::test_case;
use time::Duration;

use crate::lexer::{StringValue, Token};

#[test_case(r#""hello world""#, StringValue::Text("hello world".to_string()) ; "double_quote")]
#[test_case("'hello world'", StringValue::Text("hello world".to_string()) ; "single_quote")]
#[test_case(r#"h"hello""#, StringValue::Bytes("hello".to_string()) ; "hex_double_quote_lower")]
#[test_case(r#"H"world""#, StringValue::Bytes("world".to_string()) ; "hex_double_quote_upper")]
#[test_case("h'test'", StringValue::Bytes("test".to_string()) ; "hex_single_quote_lower")]
#[test_case("H'TEST'", StringValue::Bytes("TEST".to_string()) ; "hex_single_quote_upper")]
#[test_case(r#""""#, StringValue::Text("".to_string()) ; "empty_double_quote")]
#[test_case("''", StringValue::Text("".to_string()) ; "empty_single_quote")]
#[test_case(r#"h"""#, StringValue::Bytes("".to_string()) ; "empty_hex")]
fn test_string_literals(input: &str, expected: StringValue) {
    let mut lex = Token::lexer(input);
    assert_eq!(lex.next(), Some(Ok(Token::String(expected))));
    assert_eq!(lex.next(), None);
}

#[test_case(r#""Hello\nWorld""#, "Hello\nWorld" ; "newline")]
#[test_case(r#""Tab\there""#, "Tab\there" ; "tab")]
#[test_case(r#""Quote: \"Hello\"""#, "Quote: \"Hello\"" ; "escaped_quote")]
#[test_case(r#""Backslash: \\""#, "Backslash: \\" ; "backslash")]
fn test_escape_sequences(input: &str, expected: &str) {
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(expected.to_string()))))
    );
    assert_eq!(lex.next(), None);
}

#[test]
fn test_all_basic_escape_sequences() {
    let test_cases = vec![
        (r#""\a""#, "\x07"), // Bell
        (r#""\b""#, "\x08"), // Backspace
        (r#""\f""#, "\x0C"), // Form feed
        (r#""\n""#, "\n"),   // Newline
        (r#""\r""#, "\r"),   // Carriage return
        (r#""\t""#, "\t"),   // Tab
        (r#""\v""#, "\x0B"), // Vertical tab
        (r#""\'""#, "'"),    // Single quote
        (r#""\"""#, "\""),   // Double quote
        (r#""\\""#, "\\"),   // Backslash
    ];

    for (input, expected) in test_cases {
        let mut lex = Token::lexer(input);
        assert_eq!(
            lex.next(),
            Some(Ok(Token::String(StringValue::Text(expected.to_string())))),
            "Failed for input: {input}"
        );
    }
}

#[test_case(r#""\x41""#, "A" ; "hex_41")]
#[test_case(r#""\x7A""#, "z" ; "hex_7a")]
#[test_case(r#""\x4""#, "\\x4" ; "hex_invalid_too_short")]
#[test_case(r#""\xGG""#, "\\xGG" ; "hex_invalid_non_hex")]
fn test_hex_escape_sequences(input: &str, expected: &str) {
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(expected.to_string()))))
    );
    assert_eq!(lex.next(), None);
}

#[test_case(r#""\u0041""#, "A" ; "u_lowercase_A")]
#[test_case(r#""\U00000041""#, "A" ; "U_uppercase_A")]
#[test_case(r#""\u263A""#, "☺" ; "u_smiley")]
#[test_case(r#""\u41""#, "\\u41" ; "u4_invalid_too_short")]
#[test_case(r#""\U0041""#, "\\U0041" ; "u8_invalid_too_short")]
fn test_unicode_escape_sequences(input: &str, expected: &str) {
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(expected.to_string()))))
    );
    assert_eq!(lex.next(), None);
}

#[test_case(r#""\7""#, "\x07" ; "single_digit")]
#[test_case(r#""\77""#, "?" ; "two_digits")]
#[test_case(r#""\101""#, "A" ; "three_digits")]
#[test_case(r#""\8""#, "\\8" ; "invalid_digit_gt_7")]
#[test_case(r#""\78""#, "\x078" ; "trailing_non_octal")]
fn test_octal_escape_sequences(input: &str, expected: &str) {
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(expected.to_string()))))
    );
    assert_eq!(lex.next(), None);
}

#[test_case(r#""\q""#, "\\q" ; "unknown_q")]
#[test_case(r#""\z""#, "\\z" ; "unknown_z")]
#[test_case(r#""hello\""#, "hello\\" ; "trailing_backslash")]
fn test_unknown_and_edge_escape_sequences(input: &str, expected: &str) {
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(expected.to_string()))))
    );
    assert_eq!(lex.next(), None);
}

#[test_case(r#"@"hello world""#, StringValue::Text("hello world".to_string()) ; "raw_double_quote")]
#[test_case("@'hello world'", StringValue::Text("hello world".to_string()) ; "raw_single_quote")]
#[test_case(r#"@"hello\nworld""#, StringValue::Text("hello\\nworld".to_string()) ; "raw_no_escape_processing")]
fn test_raw_strings(input: &str, expected: StringValue) {
    let mut lex = Token::lexer(input);
    assert_eq!(lex.next(), Some(Ok(Token::String(expected))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_raw_string_quote_escaping() {
    // Double quotes in raw string
    let mut lex = Token::lexer(r#"@"Say ""Hello"" to me""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "Say \"Hello\" to me".to_string()
        ))))
    );

    // Single quotes in raw string
    let mut lex = Token::lexer("@'Don''t worry'");
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "Don't worry".to_string()
        ))))
    );
}

#[test_case(r#"h@"hello""#, StringValue::Bytes("hello".to_string()) ; "hex_raw_double_lower")]
#[test_case("H@'world'", StringValue::Bytes("world".to_string()) ; "hex_raw_single_upper")]
fn test_hex_raw_strings(input: &str, expected: StringValue) {
    let mut lex = Token::lexer(input);
    assert_eq!(lex.next(), Some(Ok(Token::String(expected))));
    assert_eq!(lex.next(), None);
}

#[test_case("```hello\nworld```", StringValue::Text("hello\nworld".to_string()) ; "backtick")]
#[test_case("h```hex\ncontent```", StringValue::Bytes("hex\ncontent".to_string()) ; "backtick_hex")]
#[test_case("~~~hello\nworld~~~", StringValue::Text("hello\nworld".to_string()) ; "tilde")]
#[test_case("H~~~HEX\nCONTENT~~~", StringValue::Bytes("HEX\nCONTENT".to_string()) ; "tilde_hex")]
#[test_case("```hello\nworld", StringValue::Text("hello\nworld".to_string()) ; "backtick_no_end")]
#[test_case("~~~incomplete", StringValue::Text("incomplete".to_string()) ; "tilde_no_end")]
#[test_case("``````", StringValue::Text("".to_string()) ; "backtick_empty")]
#[test_case("~~~~~~", StringValue::Text("".to_string()) ; "tilde_empty")]
fn test_multiline_strings(input: &str, expected: StringValue) {
    let mut lex = Token::lexer(input);
    assert_eq!(lex.next(), Some(Ok(Token::String(expected))));
}

#[test]
fn test_whitespace_skipping() {
    let input = r#"  "hello"   "world"  "#;
    let mut lex = Token::lexer(input);

    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("hello".to_string()))))
    );
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("world".to_string()))))
    );
    assert_eq!(lex.next(), None);
}

#[test]
fn test_mixed_string_types() {
    let input = r#""regular" h'hex' @"raw" ```multi
line``` ~~~tilde~~~"#;
    let mut lex = Token::lexer(input);

    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("regular".to_string()))))
    );
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Bytes("hex".to_string()))))
    );
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("raw".to_string()))))
    );
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "multi\nline".to_string()
        ))))
    );
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("tilde".to_string()))))
    );
    assert_eq!(lex.next(), None);
}

#[test]
fn test_complex_escape_combinations() {
    let mut lex = Token::lexer(r#""\n\t\r\\\"""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "\n\t\r\\\"".to_string()
        ))))
    );

    let mut lex = Token::lexer(r#""\x41\u0042\101""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("ABA".to_string()))))
    );
}

#[test]
fn test_edge_case_octal_sequences() {
    // Maximum 3-digit octal sequence
    let mut lex = Token::lexer(r#""\377""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\u{FF}".to_string()))))
    );

    // Octal sequence that would overflow u8 - should be treated literally
    let mut lex = Token::lexer(r#""\400""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\\400".to_string()))))
    );
}

#[test]
fn test_invalid_unicode_codepoints() {
    // Test invalid Unicode code point (too large)
    let mut lex = Token::lexer(r#""\U00110000""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "\\U00110000".to_string()
        ))))
    );
}

#[test]
fn test_multiline_with_delimiter_in_content() {
    let input = "```hello``` ```world```";
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("hello".to_string()))))
    );
    // Should continue lexing after the first closing delimiter
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("world".to_string()))))
    );
}

#[test_case("123e4", 123e4 ; "sci_int_lower_e")]
#[test_case("456E10", 456E10 ; "sci_int_upper_e")]
#[test_case("789e-3", 789e-3 ; "sci_int_neg_exp")]
#[test_case("42E+5", 42E+5 ; "sci_int_pos_exp")]
#[test_case("123.456e4", 123.456e4 ; "sci_dec_lower_e")]
#[test_case("0.5E-10", 0.5E-10 ; "sci_dec_neg_exp")]
#[test_case("99.99e+2", 99.99e+2 ; "sci_dec_pos_exp")]
#[test_case("123.e5", 123.0e5 ; "sci_trailing_dot")]
#[test_case("123.456", 123.456 ; "decimal")]
#[test_case("0.5", 0.5 ; "decimal_half")]
#[test_case("42.0", 42.0 ; "decimal_zero_frac")]
#[test_case("123.", 123.0 ; "trailing_dot")]
#[test_case("1e100", 1e100 ; "large_number")]
#[test_case("1e-100", 1e-100 ; "very_small_number")]
#[test_case("0.0", 0.0 ; "zero_decimal")]
#[test_case("0e0", 0.0 ; "zero_sci")]
fn test_float_tokens(input: &str, expected: f64) {
    let mut lex = Token::lexer(input);
    assert_eq!(lex.next(), Some(Ok(Token::Float(expected))));
    assert_eq!(lex.next(), None);
}

#[test_case("0x1A", 0x1A ; "hex_lower_x")]
#[test_case("0X2B", 0x2B ; "hex_upper_x")]
#[test_case("0xDEADBEEF", 0xDEADBEEF ; "hex_deadbeef")]
#[test_case("0x0", 0x0 ; "hex_zero")]
#[test_case("0xfF", 0xFF ; "hex_mixed_case")]
#[test_case("123", 123 ; "decimal_123")]
#[test_case("0", 0 ; "decimal_zero")]
#[test_case("999999", 999999 ; "decimal_large")]
fn test_integer_tokens(input: &str, expected: i64) {
    let mut lex = Token::lexer(input);
    assert_eq!(lex.next(), Some(Ok(Token::Integer(expected))));
    assert_eq!(lex.next(), None);
}

#[test_case("true", true ; "lowercase_true")]
#[test_case("false", false ; "lowercase_false")]
#[test_case("True", true ; "capitalized_true")]
#[test_case("False", false ; "capitalized_false")]
#[test_case("TRUE", true ; "uppercase_true")]
#[test_case("FALSE", false ; "uppercase_false")]
fn test_boolean_literals(input: &str, expected: bool) {
    let mut lex = Token::lexer(input);
    assert_eq!(lex.next(), Some(Ok(Token::Bool(expected))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_pattern_precedence() {
    // Scientific notation should take precedence over decimal + integer
    let mut lex = Token::lexer("123e4");
    assert_eq!(lex.next(), Some(Ok(Token::Float(123e4))));
    assert_eq!(lex.next(), None);

    // Decimal should take precedence over integer
    let mut lex = Token::lexer("123.456");
    assert_eq!(lex.next(), Some(Ok(Token::Float(123.456))));
    assert_eq!(lex.next(), None);

    // Hex should take precedence over decimal integer
    let mut lex = Token::lexer("0x123");
    assert_eq!(lex.next(), Some(Ok(Token::Integer(0x123))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_mixed_tokens() {
    let mut lex = Token::lexer("123 45.6 true 0xFF 1e10 false");

    assert_eq!(lex.next(), Some(Ok(Token::Integer(123))));
    assert_eq!(lex.next(), Some(Ok(Token::Float(45.6))));
    assert_eq!(lex.next(), Some(Ok(Token::Bool(true))));
    assert_eq!(lex.next(), Some(Ok(Token::Integer(0xFF))));
    assert_eq!(lex.next(), Some(Ok(Token::Float(1e10))));
    assert_eq!(lex.next(), Some(Ok(Token::Bool(false))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_whitespace_handling() {
    let mut lex = Token::lexer("  123.45   true   0xFF  ");

    assert_eq!(lex.next(), Some(Ok(Token::Float(123.45))));
    assert_eq!(lex.next(), Some(Ok(Token::Bool(true))));
    assert_eq!(lex.next(), Some(Ok(Token::Integer(0xFF))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_comments() {
    let mut lex = Token::lexer("// This is a comment");
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("// Comment\nx = 5");
    assert_eq!(lex.next(), Some(Ok(Token::Ident("x".to_string()))));
    assert_eq!(lex.next(), Some(Ok(Token::Eq)));
    assert_eq!(lex.next(), Some(Ok(Token::Integer(5))));

    let mut lex = Token::lexer("x = 5 // This is an inline comment");
    assert_eq!(lex.next(), Some(Ok(Token::Ident("x".to_string()))));
    assert_eq!(lex.next(), Some(Ok(Token::Eq)));
    assert_eq!(lex.next(), Some(Ok(Token::Integer(5))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("// First comment\n// Second comment\nx = 5");
    assert_eq!(lex.next(), Some(Ok(Token::Ident("x".to_string()))));
    assert_eq!(lex.next(), Some(Ok(Token::Eq)));
    assert_eq!(lex.next(), Some(Ok(Token::Integer(5))));

    let mut lex = Token::lexer("// Comment with symbols: !@#$%^&*()");
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("//");
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("// Comment\rx = 5");
    assert_eq!(lex.next(), Some(Ok(Token::Ident("x".to_string()))));

    let mut lex = Token::lexer("x / y");
    assert_eq!(lex.next(), Some(Ok(Token::Ident("x".to_string()))));
    assert_eq!(lex.next(), Some(Ok(Token::Div)));
    assert_eq!(lex.next(), Some(Ok(Token::Ident("y".to_string()))));
}

#[test_case("5ms", Duration::milliseconds(5) ; "ms_short")]
#[test_case("30s", Duration::seconds(30) ; "seconds_short")]
#[test_case("2.5m", Duration::seconds_f64(2.5 * 60.0) ; "minutes_short_decimal")]
#[test_case("10minutes", Duration::seconds(10 * 60) ; "minutes_full")]
#[test_case("45seconds", Duration::seconds(45) ; "seconds_full")]
#[test_case("3hours", Duration::seconds(3 * 3600) ; "hours_full")]
#[test_case("1.5days", Duration::seconds_f64(1.5 * 86400.0) ; "days_decimal")]
#[test_case("24hrs", Duration::seconds(24 * 3600) ; "hrs_abbrev")]
#[test_case("1hr", Duration::seconds(3600) ; "hr_abbrev")]
#[test_case("15min", Duration::seconds(15 * 60) ; "min_abbrev")]
#[test_case("500milliseconds", Duration::milliseconds(500) ; "milliseconds_full")]
#[test_case("1000microseconds", Duration::microseconds(1000) ; "microseconds_full")]
#[test_case("500nanoseconds", Duration::nanoseconds(500) ; "nanoseconds_full")]
#[test_case("100ticks", Duration::nanoseconds(100 * 100) ; "ticks")]
#[test_case("3.25s", Duration::seconds_f64(3.25) ; "seconds_decimal")]
#[test_case("0.5hours", Duration::seconds_f64(0.5 * 3600.0) ; "hours_decimal")]
fn test_timespan_literals(input: &str, expected: Duration) {
    let mut lex = Token::lexer(input);
    assert_eq!(lex.next(), Some(Ok(Token::Timespan(expected))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_timespan_vs_ident_priority() {
    let mut lex = Token::lexer("5ms");
    assert_eq!(
        lex.next(),
        Some(Ok(Token::Timespan(Duration::milliseconds(5))))
    );
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("5msABC");
    assert_eq!(lex.next(), Some(Ok(Token::Ident("5msABC".to_string()))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("123xyz");
    assert_eq!(lex.next(), Some(Ok(Token::Ident("123xyz".to_string()))));
    assert_eq!(lex.next(), None);
}
