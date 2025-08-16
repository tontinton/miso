use logos::Logos;

use crate::lexer::{StringValue, Token};

#[test]
fn test_basic_string_literals() {
    let mut lex = Token::lexer(r#""hello world""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "hello world".to_string()
        ))))
    );
    assert_eq!(lex.next(), None);
}

#[test]
fn test_single_quote_string_literals() {
    let mut lex = Token::lexer("'hello world'");
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "hello world".to_string()
        ))))
    );
    assert_eq!(lex.next(), None);
}

#[test]
fn test_hex_string_literals() {
    let mut lex = Token::lexer(r#"h"hello""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Bytes("hello".to_string()))))
    );

    let mut lex = Token::lexer(r#"H"world""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Bytes("world".to_string()))))
    );
}

#[test]
fn test_hex_single_quote_string_literals() {
    let mut lex = Token::lexer("h'test'");
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Bytes("test".to_string()))))
    );

    let mut lex = Token::lexer("H'TEST'");
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Bytes("TEST".to_string()))))
    );
}

#[test]
fn test_empty_strings() {
    let mut lex = Token::lexer(r#""""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("".to_string()))))
    );

    let mut lex = Token::lexer("''");
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("".to_string()))))
    );

    let mut lex = Token::lexer(r#"h"""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Bytes("".to_string()))))
    );
}

#[test]
fn test_escape_sequences() {
    let mut lex = Token::lexer(r#""Hello\nWorld""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "Hello\nWorld".to_string()
        ))))
    );

    let mut lex = Token::lexer(r#""Tab\there""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "Tab\there".to_string()
        ))))
    );

    let mut lex = Token::lexer(r#""Quote: \"Hello\"""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "Quote: \"Hello\"".to_string()
        ))))
    );

    let mut lex = Token::lexer(r#""Backslash: \\""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "Backslash: \\".to_string()
        ))))
    );
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

#[test]
fn test_hex_escape_sequences() {
    // \x followed by 2 hex digits
    let mut lex = Token::lexer(r#""\x41""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("A".to_string()))))
    );

    let mut lex = Token::lexer(r#""\x7A""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("z".to_string()))))
    );

    // Invalid hex escape (not enough digits)
    let mut lex = Token::lexer(r#""\x4""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\\x4".to_string()))))
    );

    // Invalid hex escape (non-hex character)
    let mut lex = Token::lexer(r#""\xGG""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\\xGG".to_string()))))
    );
}

#[test]
fn test_unicode_escape_sequences() {
    // \u followed by 4 hex digits
    let mut lex = Token::lexer(r#""\u0041""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("A".to_string()))))
    );

    // \U followed by 8 hex digits
    let mut lex = Token::lexer(r#""\U00000041""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("A".to_string()))))
    );

    // Unicode character (smiley face)
    let mut lex = Token::lexer(r#""\u263A""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("☺".to_string()))))
    );

    // Invalid unicode (not enough digits for \u)
    let mut lex = Token::lexer(r#""\u41""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\\u41".to_string()))))
    );

    // Invalid unicode (not enough digits for \U)
    let mut lex = Token::lexer(r#""\U0041""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\\U0041".to_string()))))
    );
}

#[test]
fn test_octal_escape_sequences() {
    // Single octal digit
    let mut lex = Token::lexer(r#""\7""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\x07".to_string()))))
    );

    // Two octal digits
    let mut lex = Token::lexer(r#""\77""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("?".to_string()))))
    );

    // Three octal digits
    let mut lex = Token::lexer(r#""\101""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("A".to_string()))))
    );

    // Invalid octal (digit > 7)
    let mut lex = Token::lexer(r#""\8""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\\8".to_string()))))
    );

    // Octal with following non-octal digit
    let mut lex = Token::lexer(r#""\78""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\x078".to_string()))))
    );
}

#[test]
fn test_unknown_escape_sequences() {
    let mut lex = Token::lexer(r#""\q""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\\q".to_string()))))
    );

    let mut lex = Token::lexer(r#""\z""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("\\z".to_string()))))
    );
}

#[test]
fn test_trailing_backslash() {
    let mut lex = Token::lexer(r#""hello\""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("hello\\".to_string()))))
    );
}

#[test]
fn test_raw_strings() {
    // Basic raw string
    let mut lex = Token::lexer(r#"@"hello world""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "hello world".to_string()
        ))))
    );

    // Raw string with single quotes
    let mut lex = Token::lexer("@'hello world'");
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "hello world".to_string()
        ))))
    );

    // Raw string with escaped sequences (should not be processed)
    let mut lex = Token::lexer(r#"@"hello\nworld""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "hello\\nworld".to_string()
        ))))
    );
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

#[test]
fn test_hex_raw_strings() {
    let mut lex = Token::lexer(r#"h@"hello""#);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Bytes("hello".to_string()))))
    );

    let mut lex = Token::lexer("H@'world'");
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Bytes("world".to_string()))))
    );
}

#[test]
fn test_multiline_backtick_strings() {
    let input = "```hello\nworld```";
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "hello\nworld".to_string()
        ))))
    );

    // With hex prefix
    let input = "h```hex\ncontent```";
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Bytes(
            "hex\ncontent".to_string()
        ))))
    );
}

#[test]
fn test_multiline_tilde_strings() {
    let input = "~~~hello\nworld~~~";
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "hello\nworld".to_string()
        ))))
    );

    // With hex prefix
    let input = "H~~~HEX\nCONTENT~~~";
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Bytes(
            "HEX\nCONTENT".to_string()
        ))))
    );
}

#[test]
fn test_multiline_without_ending_delimiter() {
    // Should consume rest of input if delimiter not found
    let input = "```hello\nworld";
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "hello\nworld".to_string()
        ))))
    );

    let input = "~~~incomplete";
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text(
            "incomplete".to_string()
        ))))
    );
}

#[test]
fn test_multiline_empty() {
    let input = "``````";
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("".to_string()))))
    );

    let input = "~~~~~~";
    let mut lex = Token::lexer(input);
    assert_eq!(
        lex.next(),
        Some(Ok(Token::String(StringValue::Text("".to_string()))))
    );
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

#[test]
fn test_scientific_notation_integer_base() {
    let mut lex = Token::lexer("123e4");
    assert_eq!(lex.next(), Some(Ok(Token::Float(123e4))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("456E10");
    assert_eq!(lex.next(), Some(Ok(Token::Float(456E10))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("789e-3");
    assert_eq!(lex.next(), Some(Ok(Token::Float(789e-3))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("42E+5");
    assert_eq!(lex.next(), Some(Ok(Token::Float(42E+5))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_scientific_notation_decimal_base() {
    let mut lex = Token::lexer("123.456e4");
    assert_eq!(lex.next(), Some(Ok(Token::Float(123.456e4))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("0.5E-10");
    assert_eq!(lex.next(), Some(Ok(Token::Float(0.5E-10))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("99.99e+2");
    assert_eq!(lex.next(), Some(Ok(Token::Float(99.99e+2))));
    assert_eq!(lex.next(), None);

    // Test with trailing decimal point
    let mut lex = Token::lexer("123.e5");
    assert_eq!(lex.next(), Some(Ok(Token::Float(123.0e5))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_decimal_floats() {
    let mut lex = Token::lexer("123.456");
    assert_eq!(lex.next(), Some(Ok(Token::Float(123.456))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("0.5");
    assert_eq!(lex.next(), Some(Ok(Token::Float(0.5))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("42.0");
    assert_eq!(lex.next(), Some(Ok(Token::Float(42.0))));
    assert_eq!(lex.next(), None);

    // Test with trailing decimal point (no fractional part)
    let mut lex = Token::lexer("123.");
    assert_eq!(lex.next(), Some(Ok(Token::Float(123.0))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_hexadecimal_integers() {
    let mut lex = Token::lexer("0x1A");
    assert_eq!(lex.next(), Some(Ok(Token::Integer(0x1A))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("0X2B");
    assert_eq!(lex.next(), Some(Ok(Token::Integer(0x2B))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("0xDEADBEEF");
    assert_eq!(lex.next(), Some(Ok(Token::Integer(0xDEADBEEF))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("0x0");
    assert_eq!(lex.next(), Some(Ok(Token::Integer(0x0))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("0xfF");
    assert_eq!(lex.next(), Some(Ok(Token::Integer(0xFF))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_decimal_integers() {
    let mut lex = Token::lexer("123");
    assert_eq!(lex.next(), Some(Ok(Token::Integer(123))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("0");
    assert_eq!(lex.next(), Some(Ok(Token::Integer(0))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("999999");
    assert_eq!(lex.next(), Some(Ok(Token::Integer(999999))));
    assert_eq!(lex.next(), None);
}

#[test]
fn test_boolean_literals() {
    // Test lowercase
    let mut lex = Token::lexer("true");
    assert_eq!(lex.next(), Some(Ok(Token::Bool(true))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("false");
    assert_eq!(lex.next(), Some(Ok(Token::Bool(false))));
    assert_eq!(lex.next(), None);

    // Test capitalized
    let mut lex = Token::lexer("True");
    assert_eq!(lex.next(), Some(Ok(Token::Bool(true))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("False");
    assert_eq!(lex.next(), Some(Ok(Token::Bool(false))));
    assert_eq!(lex.next(), None);

    // Test uppercase
    let mut lex = Token::lexer("TRUE");
    assert_eq!(lex.next(), Some(Ok(Token::Bool(true))));
    assert_eq!(lex.next(), None);

    let mut lex = Token::lexer("FALSE");
    assert_eq!(lex.next(), Some(Ok(Token::Bool(false))));
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
fn test_float_edge_cases() {
    // Large numbers
    let mut lex = Token::lexer("1e100");
    assert_eq!(lex.next(), Some(Ok(Token::Float(1e100))));
    assert_eq!(lex.next(), None);

    // Very small numbers
    let mut lex = Token::lexer("1e-100");
    assert_eq!(lex.next(), Some(Ok(Token::Float(1e-100))));
    assert_eq!(lex.next(), None);

    // Zero with decimal
    let mut lex = Token::lexer("0.0");
    assert_eq!(lex.next(), Some(Ok(Token::Float(0.0))));
    assert_eq!(lex.next(), None);

    // Zero in scientific notation
    let mut lex = Token::lexer("0e0");
    assert_eq!(lex.next(), Some(Ok(Token::Float(0.0))));
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
