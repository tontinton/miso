use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, Paragraph, Wrap},
};
use serde_json::{Map, Value};

use crate::{
    common::{BORDER_TYPE, HORIZONTAL},
    components::{Action, Component},
};

#[derive(Debug, Default)]
pub struct LogView {
    log: Option<Map<String, Value>>,
    width: usize,
    focused: bool,
}

impl LogView {
    pub fn set_log(&mut self, log: Map<String, Value>) -> Action {
        self.log = Some(log);
        Action::Redraw
    }
}

/// Convert a JSON `Value` into styled lines of text.
fn value_to_text(value: &Value, indent: usize) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    match value {
        Value::Object(map) => {
            lines.push(Line::from(vec![Span::styled("{", Style::new().white())]));
            for (i, (k, v)) in map.iter().enumerate() {
                let mut spans = vec![
                    Span::raw(" ".repeat(indent + 2)),
                    Span::styled(format!("\"{}\"", k), Style::new().fg(Color::Cyan).bold()),
                    Span::raw(": "),
                ];

                match v {
                    Value::String(s) => {
                        spans.push(Span::styled(
                            format!("\"{}\"", s),
                            Style::new().fg(Color::Green),
                        ));
                    }
                    Value::Number(n) => {
                        spans.push(Span::styled(n.to_string(), Style::new().fg(Color::Yellow)));
                    }
                    Value::Bool(b) => {
                        spans.push(Span::styled(b.to_string(), Style::new().fg(Color::Magenta)));
                    }
                    Value::Null => {
                        spans.push(Span::styled("null", Style::new().fg(Color::DarkGray)));
                    }
                    Value::Array(arr) => {
                        spans.push(Span::raw("["));
                        lines.push(Line::from(spans));
                        for (j, item) in arr.iter().enumerate() {
                            let mut sublines = value_to_text(item, indent + 2);
                            if j < arr.len() - 1
                                && let Some(last) = sublines.last_mut()
                            {
                                last.spans.push(Span::raw(","));
                            }
                            lines.extend(sublines);
                        }
                        let closing = format!("{}]", " ".repeat(indent + 2));
                        lines.push(Line::from(vec![Span::raw(closing)]));
                        if i < map.len() - 1
                            && let Some(last) = lines.last_mut()
                        {
                            last.spans.push(Span::raw(","));
                        }
                        continue;
                    }
                    Value::Object(_) => {
                        spans.push(Span::raw("{"));
                        lines.push(Line::from(spans));
                        lines.extend(value_to_text(v, indent + 2));
                        let closing = format!("{}{}", " ".repeat(indent + 2), "}");
                        lines.push(Line::from(vec![Span::raw(closing)]));
                        if i < map.len() - 1
                            && let Some(last) = lines.last_mut()
                        {
                            last.spans.push(Span::raw(","));
                        }
                        continue;
                    }
                }

                if i < map.len() - 1 {
                    spans.push(Span::raw(","));
                }
                lines.push(Line::from(spans));
            }
            lines.push(Line::from(vec![Span::styled("}", Style::new().white())]));
        }
        Value::Array(arr) => {
            lines.push(Line::from(vec![Span::styled("[", Style::new().white())]));
            for (i, v) in arr.iter().enumerate() {
                let mut sublines = value_to_text(v, indent + 2);
                if i < arr.len() - 1
                    && let Some(last) = sublines.last_mut()
                {
                    last.spans.push(Span::raw(","));
                }
                lines.extend(sublines);
            }
            lines.push(Line::from(vec![Span::raw(" ".repeat(indent) + "]")]));
        }
        Value::String(s) => {
            lines.push(Line::from(vec![
                Span::raw(" ".repeat(indent + 2)),
                Span::styled(format!("\"{}\"", s), Style::new().fg(Color::Green)),
            ]));
        }
        Value::Number(n) => {
            lines.push(Line::from(vec![
                Span::raw(" ".repeat(indent + 2)),
                Span::styled(n.to_string(), Style::new().fg(Color::Yellow)),
            ]));
        }
        Value::Bool(b) => {
            lines.push(Line::from(vec![
                Span::raw(" ".repeat(indent + 2)),
                Span::styled(b.to_string(), Style::new().fg(Color::Magenta)),
            ]));
        }
        Value::Null => {
            lines.push(Line::from(vec![
                Span::raw(" ".repeat(indent + 2)),
                Span::styled("null", Style::new().fg(Color::DarkGray)),
            ]));
        }
    }
    lines
}

impl Component for LogView {
    fn draw(&mut self, frame: &mut Frame, area: Rect) {
        self.width = area.width as usize - 2;

        let mut title_text = "Log".white();
        let mut border_color = Style::new().white();
        if self.focused {
            title_text = title_text.bold();
            border_color = border_color.green();
        }

        let title = Line::from(vec![HORIZONTAL.into(), title_text]);
        let block = Block::bordered()
            .border_type(BORDER_TYPE)
            .border_style(border_color)
            .title_top(title);

        let mut styled_lines: Vec<Line> = Vec::new();
        if let Some(log) = &self.log {
            styled_lines.extend(value_to_text(&Value::Object(log.clone()), 0));
        }

        let text = Text::from(styled_lines);
        let paragraph = Paragraph::new(text).block(block).wrap(Wrap { trim: false });

        frame.render_widget(paragraph, area);
    }

    fn handle_focus_event(&mut self, focus: bool) -> Action {
        self.focused = focus;
        Action::Redraw
    }
}
