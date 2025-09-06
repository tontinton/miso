use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Style, Stylize},
    text::{Line, Span, Text},
    widgets::Paragraph,
};

use crate::components::{Action, Component};

#[derive(Debug, Default)]
pub struct Footer {
    line: String,
    x: usize,
    width: usize,
}

impl Footer {
    pub fn height(&self) -> u16 {
        1
    }

    fn push_char(&mut self, c: char) {
        self.line.insert(self.x, c);
        self.x += 1;
    }

    /// Delete char to the left of cursor.
    fn remove_char(&mut self) {
        if self.x == 0 {
            return;
        }
        self.line.remove(self.x - 1);
        self.x -= 1;
    }

    /// Delete char to the right of cursor.
    fn delete_char(&mut self) {
        if self.x == self.line.len() - 1 {
            return;
        }
        self.line.remove(self.x);
    }

    fn remove_word_before_cursor(&mut self) {
        if self.x == 0 {
            return;
        }

        let mut new_x = self.x;

        while new_x > 0 && self.line.as_bytes()[new_x - 1].is_ascii_whitespace() {
            new_x -= 1;
        }
        while new_x > 0 && !self.line.as_bytes()[new_x - 1].is_ascii_whitespace() {
            new_x -= 1;
        }

        self.line.replace_range(new_x..self.x, "");
        self.x = new_x;
    }
}

impl Component for Footer {
    fn draw(&mut self, frame: &mut Frame, area: Rect) {
        self.width = area.width as usize;

        let (before, rest) = self.line.split_at(self.x);
        let cursor_char = rest.chars().next();
        let after = cursor_char.map(|c| &rest[c.len_utf8()..]).unwrap_or("");

        let mut spans: Vec<Span<'_>> =
            Vec::with_capacity(1 + !before.is_empty() as usize + 1 + !after.is_empty() as usize);
        spans.push(Span::raw(":"));
        if !before.is_empty() {
            spans.push(Span::raw(before));
        }
        spans.push(Span::styled(
            cursor_char
                .map(|c| c.to_string())
                .unwrap_or(" ".to_string()),
            Style::new().reversed(),
        ));
        if !after.is_empty() {
            spans.push(Span::raw(after));
        }

        let query = Text::from(Line::from(spans));
        let paragraph = Paragraph::new(query);

        frame.render_widget(paragraph, area);
    }

    fn handle_key_event(&mut self, key: KeyEvent) -> Action {
        use KeyCode::*;

        let is_ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

        match key.code {
            Enter => {
                if &self.line == "q" {
                    return Action::Exit;
                }
            }
            Backspace => self.remove_char(),
            Delete => self.delete_char(),
            Char('w') if is_ctrl => self.remove_word_before_cursor(),
            Char(c) => self.push_char(c),
            Left if self.x > 0 => {
                self.x -= 1;
            }
            Right if self.x < self.line.len() => {
                self.x += 1;
            }
            Home => {
                self.x = 0;
            }
            End => {
                self.x = self.line.len();
            }
            _ => return Action::None,
        }

        Action::Redraw
    }
}
