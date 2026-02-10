use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Style, Stylize},
    text::{Line, Span, Text},
    widgets::Paragraph,
};

use crate::text_buffer::TextBuffer;

pub enum Msg {
    Key(KeyEvent),
}

pub enum OutMsg {
    Command(String),
}

pub struct Footer {
    buffer: TextBuffer,
}

impl Default for Footer {
    fn default() -> Self {
        Self {
            buffer: TextBuffer::single_line(),
        }
    }
}

impl Footer {
    pub fn height(&self) -> u16 {
        1
    }

    pub fn update(&mut self, msg: Msg) -> Option<OutMsg> {
        let Msg::Key(key) = msg;
        let is_ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

        match key.code {
            KeyCode::Enter => return Some(OutMsg::Command(self.buffer.value())),
            KeyCode::Backspace => self.buffer.remove_char(),
            KeyCode::Delete => self.buffer.delete_char(),
            KeyCode::Char('w') if is_ctrl => self.buffer.remove_word_before_cursor(),
            KeyCode::Char(c) => self.buffer.push_char(c),
            KeyCode::Left => self.buffer.move_left(),
            KeyCode::Right => self.buffer.move_right(),
            KeyCode::Home => self.buffer.move_home(),
            KeyCode::End => self.buffer.move_end(),
            _ => return None,
        }
        None
    }

    pub fn view(&self, frame: &mut Frame, area: Rect) {
        let line_str = self.buffer.first_line();
        let x = self.buffer.x();
        let (before, rest) = line_str.split_at(x);
        let cursor_char = rest.chars().next();
        let after = cursor_char.map(|c| &rest[c.len_utf8()..]).unwrap_or("");

        let mut spans: Vec<Span<'_>> = Vec::new();
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
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    use super::{Footer, Msg, OutMsg};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    #[test]
    fn enter_emits_command() {
        let mut footer = Footer::default();
        footer.update(Msg::Key(key(KeyCode::Char('q'))));
        let out = footer.update(Msg::Key(key(KeyCode::Enter)));
        assert!(matches!(out, Some(OutMsg::Command(ref s)) if s == "q"));
    }
}
