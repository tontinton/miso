use logos::Logos;

#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\r\n\f]+")]
pub enum Token {
    Error,

    #[token("|")]
    Pipe,
    #[token(".")]
    Dot,
    #[token("==")]
    DoubleEq,
    #[token("=")]
    Eq,
    #[token("!=")]
    Ne,
    #[token(">=")]
    Gte,
    #[token(">")]
    Gt,
    #[token("<=")]
    Lte,
    #[token("<")]
    Lt,
    #[token("*")]
    Mul,
    #[token("/")]
    Div,
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,

    #[token("or")]
    Or,
    #[token("and")]
    And,
    #[token("not")]
    Not,

    // When adding a keyword, please also add to ident_parser() in parser.rs.
    #[token("in")]
    In,
    #[token("contains")]
    Contains,
    #[token("startswith")]
    StartsWith,
    #[token("endswith")]
    EndsWith,
    #[token("has")]
    Has,
    #[token("has_cs")]
    HasCs,
    #[token("tostring")]
    ToString,
    #[token("toint")]
    ToInt,
    #[token("tolong")]
    ToLong,
    #[token("toreal")]
    ToReal,
    #[token("todecimal")]
    ToDecimal,
    #[token("tobool")]
    ToBool,
    #[token("null")]
    Null,
    #[token("hint")]
    Hint,
    #[token("partitions")]
    Partitions,
    #[token("exists")]
    Exists,
    #[token("by")]
    By,
    #[token("asc")]
    Asc,
    #[token("desc")]
    Desc,
    #[token("nulls")]
    Nulls,
    #[token("first")]
    First,
    #[token("last")]
    Last,
    #[token("where")]
    Where,
    #[token("filter")]
    Filter,
    #[token("project")]
    Project,
    #[token("extend")]
    Extend,
    #[token("limit")]
    Limit,
    #[token("take")]
    Take,
    #[token("sort")]
    Sort,
    #[token("order")]
    Order,
    #[token("top")]
    Top,
    #[token("summarize")]
    Summarize,
    #[token("distinct")]
    Distinct,
    #[token("join")]
    Join,
    #[token("kind")]
    Kind,
    #[token("inner")]
    Inner,
    #[token("outer")]
    Outer,
    #[token("left")]
    Left,
    #[token("right")]
    Right,
    #[token("on")]
    On,
    #[token("union")]
    Union,
    #[token("count")]
    Count,
    #[token("dcount")]
    DCount,
    #[token("sum")]
    Sum,
    #[token("min")]
    Min,
    #[token("max")]
    Max,
    #[token("avg")]
    Avg,
    #[token("bin")]
    Bin,

    #[token(",")]
    Comma,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,

    #[token("false", |_| false)]
    #[token("False", |_| false)]
    #[token("FALSE", |_| false)]
    #[token("true", |_| true)]
    #[token("True", |_| true)]
    #[token("TRUE", |_| true)]
    Bool(bool),

    #[regex(
        r#"[hH]?"([^"]|\\['"\\abfnrtuUxv]|\\[0-3][0-7][0-7]|\\[0-7][0-7]?)*""#,
        parse_string_literal
    )]
    #[regex(
        r#"[hH]?'([^']|\\['"\\abfnrtuUxv]|\\[0-3][0-7][0-7]|\\[0-7][0-7]?)*'"#,
        parse_string_literal
    )]
    #[regex(r#"[hH]?@"([^"]|"")*""#, parse_raw_string)]
    #[regex(r#"[hH]?@'([^']|'')*'"#, parse_raw_string)]
    #[regex(r"[hH]?```", parse_multiline_triple_backtick)]
    #[regex(r"[hH]?~~~", parse_multiline_triple_tilde)]
    String(StringValue),

    #[regex(r"[0-9]+[eE][+-]?[0-9]+", |lex| lex.slice().parse::<f64>().ok())]
    #[regex(r"[0-9]+\.[0-9]*[eE][+-]?[0-9]+", |lex| lex.slice().parse::<f64>().ok())]
    #[regex(r"[0-9]+\.[0-9]*", |lex| lex.slice().parse::<f64>().ok())]
    Float(f64),

    #[regex(r"0[xX][0-9a-fA-F]+", |lex| i64::from_str_radix(&lex.slice()[2..], 16).ok())]
    #[regex(r"[0-9]+", |lex| lex.slice().parse::<i64>().ok())]
    Integer(i64),

    #[regex(r"[$_a-zA-Z][$_a-zA-Z0-9]*", |lex| lex.slice().to_owned())]
    #[regex(r"[0-9]+[$_a-zA-Z][$_a-zA-Z0-9]*", |lex| lex.slice().to_owned())]
    Ident(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum StringValue {
    // A UTF-8 string.
    Text(String),
    /// Hex string - raw bytes, not assumed to be valid UTF-8.
    /// Example: "68656c6c6f" (will be decoded to "hello").
    Bytes(String),
}

fn is_hex_string(slice: &str) -> bool {
    slice.starts_with('h') || slice.starts_with('H')
}

/// Parse a string with escaping.
fn parse_string_literal(lex: &mut logos::Lexer<Token>) -> Option<StringValue> {
    let slice = lex.slice();

    let is_hex = is_hex_string(slice);
    let start_idx = if is_hex { 2 } else { 1 };

    let content = &slice[start_idx..slice.len() - 1]; // Up to closing quote.
    let mut chars = content.chars();
    let mut result = String::new();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            result.push(ch);
            continue;
        }

        let Some(escaped) = chars.next() else {
            result.push('\\');
            continue;
        };

        match escaped {
            '\'' => result.push('\''),
            '"' => result.push('"'),
            '\\' => result.push('\\'),
            'a' => result.push('\x07'), // Bell
            'b' => result.push('\x08'), // Backspace
            'f' => result.push('\x0C'), // Form feed
            'n' => result.push('\n'),
            'r' => result.push('\r'),
            't' => result.push('\t'),
            'v' => result.push('\x0B'), // Vertical tab
            'x' | 'u' | 'U' => {
                let hex_digit_count = match escaped {
                    'x' => 2,
                    'u' => 4,
                    'U' => 8,
                    _ => unreachable!(),
                };
                let hex_chars_opt = chars.as_str().get(..hex_digit_count);

                match hex_chars_opt {
                    Some(hex_chars)
                        if hex_chars.len() == hex_digit_count
                            && hex_chars.chars().all(|c| c.is_ascii_hexdigit()) =>
                    {
                        for _ in 0..hex_digit_count {
                            chars.next();
                        }

                        let push_as_literal =
                            if let Ok(code_point) = u32::from_str_radix(hex_chars, 16) {
                                if let Some(unicode_char) = char::from_u32(code_point) {
                                    result.push(unicode_char);
                                    false
                                } else {
                                    true
                                }
                            } else {
                                true
                            };

                        if push_as_literal {
                            result.push('\\');
                            result.push(escaped);
                            result.push_str(hex_chars);
                        }
                    }
                    _ => {
                        // Not enough hex digits or invalid hex, treat as literal.
                        result.push('\\');
                        result.push(escaped);
                    }
                }
            }
            '0'..='7' => {
                let mut octal = String::new();
                octal.push(escaped);

                let remaining: String = chars.as_str().to_string();
                let mut consumed = 0;

                for (i, c) in remaining.chars().enumerate() {
                    if i < 2 && c.is_ascii_digit() && c <= '7' {
                        octal.push(c);
                        consumed += 1;
                    } else {
                        break;
                    }
                }

                for _ in 0..consumed {
                    chars.next();
                }

                if let Ok(code) = u8::from_str_radix(&octal, 8) {
                    result.push(code as char);
                } else {
                    // Fallback: include as literal.
                    result.push('\\');
                    result.push_str(&octal);
                }
            }
            _ => {
                // Unknown escape, keep as-is.
                result.push('\\');
                result.push(escaped);
            }
        }
    }

    Some(if is_hex {
        StringValue::Bytes(result)
    } else {
        StringValue::Text(result)
    })
}

/// Parse a string without escaping.
fn parse_raw_string(lex: &mut logos::Lexer<Token>) -> Option<StringValue> {
    let slice = lex.slice();

    let is_hex = is_hex_string(slice);
    let start_idx = if is_hex { 3 } else { 2 };
    let quote_char = slice.chars().nth(start_idx - 1)?;

    let content = &slice[start_idx..slice.len() - 1]; // Up to closing quote.

    let mut chars = content.chars().peekable();
    let mut result = String::new();

    while let Some(ch) = chars.next() {
        if ch == quote_char {
            if chars.peek() == Some(&quote_char) {
                chars.next();
                result.push(quote_char);
            } else {
                result.push(ch);
            }
        } else {
            result.push(ch);
        }
    }

    Some(if is_hex {
        StringValue::Bytes(result)
    } else {
        StringValue::Text(result)
    })
}

fn parse_multiline_custom_delimiter(
    lex: &mut logos::Lexer<Token>,
    delimiter: &str,
) -> Option<StringValue> {
    let is_hex = is_hex_string(lex.slice());

    let remainder = lex.remainder();
    let result = if let Some(end_pos) = remainder.find(delimiter) {
        let content = &remainder[..end_pos];
        lex.bump(end_pos + delimiter.len());
        content.to_string()
    } else {
        let content = remainder.to_string();
        lex.bump(remainder.len());
        content
    };

    Some(if is_hex {
        StringValue::Bytes(result)
    } else {
        StringValue::Text(result)
    })
}

fn parse_multiline_triple_backtick(lex: &mut logos::Lexer<Token>) -> Option<StringValue> {
    parse_multiline_custom_delimiter(lex, "```")
}

fn parse_multiline_triple_tilde(lex: &mut logos::Lexer<Token>) -> Option<StringValue> {
    parse_multiline_custom_delimiter(lex, "~~~")
}
