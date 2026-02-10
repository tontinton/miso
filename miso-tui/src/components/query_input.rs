use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::Rect,
    style::Stylize,
    text::{Line, Text},
    widgets::{Padding, Paragraph},
};

use crate::{
    common::{HORIZONTAL, render_horizontal_scrollbar, styled_block},
    components::kql_syntax_highlight::highlight_text_with_cursor,
    text_buffer::TextBuffer,
};

const HORIZONTAL_PADDING: u16 = 1;

pub enum Msg {
    Key(KeyEvent),
}

pub enum OutMsg {
    RunQuery(String),
}

pub struct QueryInput {
    buffer: TextBuffer,
    focused: bool,
}

impl QueryInput {
    pub fn new(input: String) -> Self {
        Self {
            buffer: TextBuffer::new(input, true),
            focused: false,
        }
    }

    pub fn height(&self) -> u16 {
        self.buffer.line_count() as u16 + 2
    }

    pub fn value(&self) -> String {
        self.buffer.value()
    }

    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    pub fn update(&mut self, msg: Msg) -> Option<OutMsg> {
        let Msg::Key(key) = msg;
        let is_ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

        match key.code {
            KeyCode::Enter => self.buffer.add_line(),
            KeyCode::Backspace => self.buffer.remove_char(),
            KeyCode::Delete => self.buffer.delete_char(),
            KeyCode::Char('r') if is_ctrl => return Some(OutMsg::RunQuery(self.buffer.value())),
            KeyCode::Char('d') if is_ctrl => self.buffer.remove_line(),
            KeyCode::Char('w') if is_ctrl => self.buffer.remove_word_before_cursor(),
            KeyCode::Char(c) => self.buffer.push_char(c),
            KeyCode::Left => self.buffer.move_left(),
            KeyCode::Right => self.buffer.move_right(),
            KeyCode::Up => self.buffer.move_up(),
            KeyCode::Down => self.buffer.move_down(),
            KeyCode::Home => self.buffer.move_home(),
            KeyCode::End => self.buffer.move_end(),
            _ => return None,
        }
        None
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) {
        let width = area.width as usize - 2 - HORIZONTAL_PADDING as usize * 2;
        self.buffer.set_width(width);

        let mut bottom_title_text = "Ctrl+R to run".white();
        if self.focused {
            bottom_title_text = bottom_title_text.bold();
        }
        let bottom_title = Line::from(vec![bottom_title_text, HORIZONTAL.into()]);
        let bottom_title_width = bottom_title.width() as u16 + 2;

        let block = styled_block("Query", self.focused, Some(bottom_title))
            .padding(Padding::horizontal(HORIZONTAL_PADDING));

        let styled_lines = highlight_text_with_cursor(
            self.buffer.lines(),
            self.focused,
            self.buffer.y(),
            self.buffer.x(),
            self.buffer.scroll_x(),
        );
        let query = Text::from(styled_lines);
        let paragraph = Paragraph::new(query).block(block);

        frame.render_widget(paragraph, area);

        let max_line_width = self.buffer.max_line_width();
        if max_line_width > width {
            render_horizontal_scrollbar(
                frame,
                area,
                max_line_width,
                self.buffer.x(),
                bottom_title_width,
            );
        }
    }
}
