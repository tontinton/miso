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
    lines
        .iter()
        .enumerate()
        .map(|(y, line)| {
            let visible_line = &line[scroll_x.min(line.len())..];
            let cursor_pos = if focused && y == cursor_y {
                Some(cursor_x.saturating_sub(scroll_x))
            } else {
                None
            };
            Line::from(highlight_line(visible_line, cursor_pos))
        })
        .collect()
}

fn highlight_line(text: &str, cursor_pos: Option<usize>) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut lexer = Token::lexer(text);
    let mut pos = 0;

    while let Some(result) = lexer.next() {
        let token_span = lexer.span();

        if pos < token_span.start {
            let whitespace = &text[pos..token_span.start];
            spans.extend(render_text_with_cursor(
                whitespace,
                pos..token_span.start,
                Style::default(),
                cursor_pos,
            ));
        }

        let token_text = &text[token_span.clone()];
        let style = result.map_or(Style::default(), |token| get_token_style(&token));
        spans.extend(render_text_with_cursor(
            token_text,
            token_span.clone(),
            style,
            cursor_pos,
        ));

        pos = token_span.end;
    }

    if pos < text.len() {
        let remaining = &text[pos..];
        spans.extend(render_text_with_cursor(
            remaining,
            pos..text.len(),
            Style::default(),
            cursor_pos,
        ));
    }

    if cursor_pos == Some(text.len()) {
        spans.push(Span::raw(" ".to_string()).reversed());
    }

    spans
}

fn render_text_with_cursor(
    text: &str,
    range: std::ops::Range<usize>,
    style: Style,
    cursor_pos: Option<usize>,
) -> Vec<Span<'static>> {
    match cursor_pos.filter(|&pos| range.contains(&pos)) {
        Some(cursor) => {
            let offset = cursor - range.start;
            let (before, middle, after) = split_at_char_boundary(text, offset);

            let mut spans =
                Vec::with_capacity(before.is_some() as usize + 1 + after.is_some() as usize);

            if let Some(c) = before {
                spans.push(Span::styled(c, style));
            }

            let cursor_style = style.add_modifier(Modifier::REVERSED);
            spans.push(Span::styled(middle, cursor_style));

            if let Some(c) = after {
                spans.push(Span::styled(c, style));
            }

            spans
        }
        None => vec![Span::styled(text.to_string(), style)],
    }
}

fn split_at_char_boundary(text: &str, pos: usize) -> (Option<String>, String, Option<String>) {
    assert!(pos < text.len());

    let (before, rest) = text.split_at(pos);
    let cursor_char = rest.chars().next();
    let after = cursor_char.map(|c| &rest[c.len_utf8()..]);

    (
        (!before.is_empty()).then(|| before.to_string()),
        cursor_char
            .map(|c| c.to_string())
            .unwrap_or_else(|| " ".to_string()),
        after.filter(|s| !s.is_empty()).map(|s| s.to_string()),
    )
}

fn get_token_style(token: &Token) -> Style {
    use Token::*;

    let (color, modifiers) = match token {
        // Keywords.
        Where | Filter | Project | ProjectRename | Extend | Limit | Take | Sort | Order | Top
        | Summarize | Distinct | Join | Union | Kind | Inner | Outer | Left | Right | On | In => {
            (Color::Blue, Modifier::BOLD)
        }

        // Functions.
        Count | DCount | Sum | Min | Max | Avg | ToString | ToInt | ToLong | ToReal | ToDecimal
        | ToBool | Datetime | Now | Bin | Not | Exists => (Color::Cyan, Modifier::empty()),

        // Comparison & arithmetic operators.
        Plus | Minus | Mul | Div | Or | And | DoubleEq | Eq | Ne | Gte | Gt | Lte | Lt => {
            (Color::Magenta, Modifier::empty())
        }

        // String operators.
        Contains | StartsWith | EndsWith | Has | HasCs => (Color::Green, Modifier::empty()),

        // Literals.
        String(_) | Integer(_) | Float(_) => (Color::Yellow, Modifier::empty()),
        Bool(_) => (Color::Red, Modifier::empty()),
        Null => (Color::Gray, Modifier::ITALIC),
        Timespan(_) => (Color::Cyan, Modifier::empty()),

        // Modifiers.
        By | Asc | Desc | Nulls | First | Last => (Color::LightBlue, Modifier::empty()),

        // Special tokens.
        Hint | Partitions => (Color::LightMagenta, Modifier::empty()),

        // Punctuation.
        Pipe | Dot | Comma | LParen | RParen | LBracket | RBracket | Dollar | At => {
            (Color::Gray, Modifier::empty())
        }

        Error => {
            return Style::default()
                .fg(Color::White)
                .bg(Color::Red)
                .add_modifier(Modifier::BOLD);
        }

        Comment => (Color::Gray, Modifier::ITALIC),

        // Identifiers (default).
        _ => return Style::default(),
    };

    Style::default().fg(color).add_modifier(modifiers)
}
