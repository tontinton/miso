use logos::Logos;
use miso_kql::Token;
use ratatui::{
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
};

pub fn highlight_text_with_cursor(
    lines: &[String],
    focused: bool,
    cursor_y: usize,
    cursor_x: usize,
    scroll_x: usize,
) -> Vec<Line<'static>> {
    let mut styled_lines: Vec<Line> = Vec::with_capacity(lines.len());

    for (y, line) in lines.iter().enumerate() {
        if focused && y == cursor_y {
            let visible_line = &line[scroll_x.min(line.len())..];
            let cursor_x_pos = cursor_x.saturating_sub(scroll_x);
            let (before, rest) = visible_line.split_at(cursor_x_pos.min(visible_line.len()));
            let cursor_char = rest.chars().next();
            let after = cursor_char.map(|c| &rest[c.len_utf8()..]).unwrap_or("");

            let mut spans: Vec<Span> = Vec::new();

            if !before.is_empty() {
                spans.extend(highlight_line(before));
            }

            spans.push(Span::styled(
                cursor_char
                    .map(|c| c.to_string())
                    .unwrap_or(" ".to_string()),
                Style::new().reversed(),
            ));

            if !after.is_empty() {
                spans.extend(highlight_line(after));
            }

            styled_lines.push(Line::from(spans));
        } else {
            let visible_line = &line[scroll_x.min(line.len())..];
            let spans = highlight_line(visible_line);
            styled_lines.push(Line::from(spans));
        }
    }

    styled_lines
}

fn highlight_line(text: &str) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut lexer = Token::lexer(text);
    let mut current_pos = 0;

    while let Some(result) = lexer.next() {
        let token_span = lexer.span();
        let token_text = &text[token_span.clone()];

        if current_pos < token_span.start {
            let whitespace = &text[current_pos..token_span.start];
            spans.push(Span::raw(whitespace.to_owned()));
        }

        match result {
            Ok(token) => {
                let style = get_token_style(&token);
                spans.push(Span::styled(token_text.to_owned(), style));
            }
            Err(_) => {
                spans.push(Span::raw(token_text.to_owned()));
            }
        }

        current_pos = token_span.end;
    }

    if current_pos < text.len() {
        spans.push(Span::raw(text[current_pos..].to_owned()));
    }

    spans
}

fn get_token_style(token: &Token) -> Style {
    match token {
        // Keywords.
        Token::Where
        | Token::Filter
        | Token::Project
        | Token::ProjectRename
        | Token::Extend
        | Token::Limit
        | Token::Take
        | Token::Sort
        | Token::Order
        | Token::Top
        | Token::Summarize
        | Token::Distinct
        | Token::Join
        | Token::Union => Style::default()
            .fg(Color::Blue)
            .add_modifier(Modifier::BOLD),

        // Functions.
        Token::Count
        | Token::DCount
        | Token::Sum
        | Token::Min
        | Token::Max
        | Token::Avg
        | Token::ToString
        | Token::ToInt
        | Token::ToLong
        | Token::ToReal
        | Token::ToDecimal
        | Token::ToBool
        | Token::Datetime
        | Token::Now
        | Token::Bin => Style::default().fg(Color::Cyan),

        // Logical operators.
        Token::Or | Token::And | Token::Not => Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),

        // Comparison operators.
        Token::DoubleEq
        | Token::Eq
        | Token::Ne
        | Token::Gte
        | Token::Gt
        | Token::Lte
        | Token::Lt => Style::default().fg(Color::Magenta),

        // Arithmetic operators.
        Token::Plus | Token::Minus | Token::Mul | Token::Div => Style::default().fg(Color::White),

        // String operators.
        Token::Contains
        | Token::StartsWith
        | Token::EndsWith
        | Token::Has
        | Token::HasCs
        | Token::In => Style::default().fg(Color::Green),

        // Literals.
        Token::String(_) => Style::default().fg(Color::Green),
        Token::Integer(_) => Style::default().fg(Color::Yellow),
        Token::Float(_) => Style::default().fg(Color::Yellow),
        Token::Bool(_) => Style::default().fg(Color::Red),
        Token::Null => Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::ITALIC),
        Token::Timespan(_) => Style::default().fg(Color::Cyan),

        // Identifiers.
        Token::Ident(_) => Style::default(),

        // Join keywords.
        Token::Kind | Token::Inner | Token::Outer | Token::Left | Token::Right | Token::On => {
            Style::default().fg(Color::Blue)
        }

        // Modifiers.
        Token::By | Token::Asc | Token::Desc | Token::Nulls | Token::First | Token::Last => {
            Style::default().fg(Color::LightBlue)
        }

        // Special tokens.
        Token::Hint | Token::Partitions | Token::Exists => Style::default().fg(Color::LightMagenta),

        // Punctuation.
        Token::Pipe
        | Token::Dot
        | Token::Comma
        | Token::LParen
        | Token::RParen
        | Token::LBracket
        | Token::RBracket
        | Token::Dollar
        | Token::At => Style::default().fg(Color::Gray),

        // Error.
        Token::Error => Style::default()
            .fg(Color::White)
            .bg(Color::Red)
            .add_modifier(Modifier::BOLD),

        // Comment.
        Token::Comment => Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::ITALIC),
    }
}
