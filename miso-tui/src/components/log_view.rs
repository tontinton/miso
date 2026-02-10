use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Paragraph, Wrap},
};
use serde_json::{Map, Value};

use crate::common::styled_block;

pub enum Msg {
    Key(KeyEvent),
    SetLog(Map<String, Value>),
    Clear,
}

#[derive(Default)]
pub struct LogView {
    rendered: Vec<Line<'static>>,
    scroll_y: usize,
    focused: bool,
    last_height: usize,
}

impl LogView {
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    pub fn update(&mut self, msg: Msg) {
        match msg {
            Msg::SetLog(log) => {
                self.rendered = value_to_text(&Value::Object(log), 0);
                self.scroll_y = 0;
            }
            Msg::Clear => {
                self.rendered.clear();
                self.scroll_y = 0;
            }
            Msg::Key(key) => self.handle_key(key),
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        let max_scroll = self.rendered.len().saturating_sub(self.last_height);
        match key.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll_y = self.scroll_y.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll_y = (self.scroll_y + 1).min(max_scroll);
            }
            KeyCode::PageUp => {
                self.scroll_y = self.scroll_y.saturating_sub(self.last_height / 2);
            }
            KeyCode::PageDown => {
                self.scroll_y = (self.scroll_y + self.last_height / 2).min(max_scroll);
            }
            KeyCode::Home => self.scroll_y = 0,
            KeyCode::End => self.scroll_y = max_scroll,
            _ => {}
        }
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) {
        self.last_height = area.height.saturating_sub(2) as usize;
        let block = styled_block("Log", self.focused, None);
        let text = Text::from(self.rendered.clone());
        let paragraph = Paragraph::new(text)
            .block(block)
            .wrap(Wrap { trim: false })
            .scroll((self.scroll_y as u16, 0));
        frame.render_widget(paragraph, area);
    }
}

fn value_to_text(value: &Value, indent: usize) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    match value {
        Value::Object(map) => {
            lines.push(Line::from(vec![Span::styled("{", Style::new().white())]));
            object_entries_to_text(map, indent, &mut lines);
            lines.push(Line::from(vec![Span::styled("}", Style::new().white())]));
        }
        Value::Array(arr) => {
            lines.push(Line::from(vec![Span::styled("[", Style::new().white())]));
            array_items_to_text(arr, indent, &mut lines);
            lines.push(Line::from(vec![Span::raw(" ".repeat(indent) + "]")]));
        }
        _ => {
            lines.push(Line::from(vec![
                Span::raw(" ".repeat(indent + 2)),
                scalar_span(value),
            ]));
        }
    }
    lines
}

fn object_entries_to_text(map: &Map<String, Value>, indent: usize, lines: &mut Vec<Line<'static>>) {
    for (i, (k, v)) in map.iter().enumerate() {
        let trailing = if i < map.len() - 1 { "," } else { "" };
        let mut spans = vec![
            Span::raw(" ".repeat(indent + 2)),
            Span::styled(format!("\"{}\"", k), Style::new().fg(Color::Cyan).bold()),
            Span::raw(": "),
        ];

        match v {
            Value::Object(inner) => {
                spans.push(Span::raw("{"));
                lines.push(Line::from(spans));
                object_entries_to_text(inner, indent + 2, lines);
                let closing = format!("{}}}{}", " ".repeat(indent + 2), trailing);
                lines.push(Line::from(vec![Span::raw(closing)]));
            }
            Value::Array(arr) => {
                spans.push(Span::raw("["));
                lines.push(Line::from(spans));
                array_items_to_text(arr, indent + 2, lines);
                let closing = format!("{}]{}", " ".repeat(indent + 2), trailing);
                lines.push(Line::from(vec![Span::raw(closing)]));
            }
            _ => {
                spans.push(scalar_span(v));
                if !trailing.is_empty() {
                    spans.push(Span::raw(trailing));
                }
                lines.push(Line::from(spans));
            }
        }
    }
}

fn array_items_to_text(arr: &[Value], indent: usize, lines: &mut Vec<Line<'static>>) {
    for (i, item) in arr.iter().enumerate() {
        let mut sublines = value_to_text(item, indent);
        if i < arr.len() - 1
            && let Some(last) = sublines.last_mut()
        {
            last.spans.push(Span::raw(","));
        }
        lines.extend(sublines);
    }
}

fn scalar_span(value: &Value) -> Span<'static> {
    match value {
        Value::String(s) => Span::styled(format!("\"{}\"", s), Style::new().fg(Color::Green)),
        Value::Number(n) => Span::styled(n.to_string(), Style::new().fg(Color::Yellow)),
        Value::Bool(b) => Span::styled(b.to_string(), Style::new().fg(Color::Magenta)),
        Value::Null => Span::styled("null", Style::new().fg(Color::DarkGray)),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn render(val: serde_json::Value) -> String {
        value_to_text(&val, 0)
            .iter()
            .map(|l| {
                l.spans
                    .iter()
                    .map(|s| s.content.as_ref())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn nested_structure_no_doubled_delimiters() {
        let text = render(json!({"a": {"b": [1, {"c": true}]}, "d": {}}));
        assert!(!text.contains("{{"));
        assert!(!text.contains("}}"));
        assert!(!text.contains("[["));
        assert!(!text.contains("]]"));
        for expected in ["\"a\"", "\"b\"", "\"c\"", "true", "1"] {
            assert!(text.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn all_scalar_types() {
        let text = render(json!({"s": "hello", "n": 42, "b": false, "x": null}));
        for expected in ["\"hello\"", "42", "false", "null"] {
            assert!(text.contains(expected), "missing {expected}");
        }
    }
}
