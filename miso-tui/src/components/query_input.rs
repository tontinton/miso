use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, Padding, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
};

use crate::{
    common::{BORDER_TYPE, HORIZONTAL},
    components::{Action, Component},
};

const HORIZONTAL_PADDING: u16 = 1;

#[derive(Debug, Default)]
pub struct QueryInput {
    lines: Vec<String>,

    focused: bool,

    /// When going up / down, this value stays the same (like in vim).
    /// To get the cursor x position, call self.x().
    raw_x: usize,
    y: usize,

    width: usize,
    scroll_x: usize,
}

impl QueryInput {
    pub fn height(&self) -> u16 {
        // Include the border.
        self.lines.len() as u16 + 2
    }

    fn value(&self) -> String {
        self.lines.join("\n")
    }

    fn x(&self) -> usize {
        self.raw_x.min(self.current_line_width())
    }

    fn current_line_width(&self) -> usize {
        self.lines[self.y].len()
    }

    fn max_line_width(&self) -> usize {
        self.lines.iter().map(|l| l.len()).max().unwrap_or(0)
    }

    fn push_char(&mut self, c: char) {
        if self.x() == self.current_line_width() {
            self.lines[self.y].push(c);
        } else {
            let new_x = self.x();
            self.lines[self.y].insert(new_x, c);
        }
        self.raw_x = self.x() + 1;
    }

    fn add_line(&mut self) {
        let (left, right) = self.lines[self.y].split_at(self.x());
        let (left, right) = (left.to_string(), right.to_string());
        self.lines[self.y] = left;
        self.lines.insert(self.y + 1, right);

        self.raw_x = 0;
        self.y += 1;
    }

    fn remove_char_of_empty_line(&mut self) {
        assert_eq!(self.x(), 0);

        if self.y == 0 {
            // Do nothing.
            return;
        }

        self.raw_x = self.lines[self.y - 1].len();
        let line = self.lines.remove(self.y);
        self.lines[self.y - 1].push_str(&line);
        self.y -= 1;
    }

    /// Delete char to the left of cursor.
    fn remove_char(&mut self) {
        if self.x() == 0 {
            self.remove_char_of_empty_line();
        } else {
            let new_x = self.x() - 1;
            self.lines[self.y].remove(new_x);
            self.raw_x = new_x;
        }
    }

    /// Delete char to the right of cursor.
    fn delete_char(&mut self) {
        let x = self.x();

        if x == self.current_line_width() {
            if self.y + 1 < self.lines.len() {
                let next_line = self.lines.remove(self.y + 1);
                self.lines[self.y].push_str(&next_line);
            }
        } else {
            self.lines[self.y].remove(x);
        }
    }

    fn remove_line(&mut self) {
        if self.lines.len() == 1 {
            self.lines[self.y].clear();
        } else {
            self.lines.remove(self.y);
        }

        if self.y > 0 {
            self.y -= 1;
        }
    }

    fn remove_word_before_cursor(&mut self) {
        if self.x() == 0 {
            self.remove_char_of_empty_line();
            return;
        }

        let x = self.x();
        let mut new_x = x;
        let line = &mut self.lines[self.y];

        while new_x > 0 && line.as_bytes()[new_x - 1].is_ascii_whitespace() {
            new_x -= 1;
        }
        while new_x > 0 && !line.as_bytes()[new_x - 1].is_ascii_whitespace() {
            new_x -= 1;
        }

        line.replace_range(new_x..x, "");
        self.raw_x = new_x;
    }
}

impl Component for QueryInput {
    fn draw(&mut self, frame: &mut Frame, area: Rect) {
        // Exclude border + padding.
        self.width = area.width as usize - 2 - HORIZONTAL_PADDING as usize * 2;

        let mut title_text = "Query".white();
        let mut bottom_title_text = "Ctrl+R to run".white();
        let mut border_color = Style::new().white();
        if self.focused {
            title_text = title_text.bold();
            bottom_title_text = bottom_title_text.bold();
            border_color = border_color.green();
        }

        let title = Line::from(vec![HORIZONTAL.into(), title_text]);
        let bottom_title = Line::from(vec![bottom_title_text, HORIZONTAL.into()]);
        let bottom_title_width = bottom_title.width();
        let block = Block::bordered()
            .border_type(BORDER_TYPE)
            .border_style(border_color)
            .title_top(title)
            .title_bottom(bottom_title.right_aligned())
            .padding(Padding::horizontal(HORIZONTAL_PADDING));

        let mut styled_lines: Vec<Line> = Vec::with_capacity(self.lines.len());

        for (y, line) in self.lines.iter().enumerate() {
            if self.focused && y == self.y {
                let x = self.x();
                let visible_line = &line[self.scroll_x.min(line.len())..];

                let cursor_x = x.saturating_sub(self.scroll_x);
                let (before, rest) = visible_line.split_at(cursor_x.min(visible_line.len()));

                let cursor_char = rest.chars().next();
                let after = cursor_char.map(|c| &rest[c.len_utf8()..]).unwrap_or("");

                let mut spans: Vec<Span<'_>> = Vec::with_capacity(
                    !before.is_empty() as usize + 1 + !after.is_empty() as usize,
                );
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

                styled_lines.push(Line::from(spans));
            } else {
                let visible_line = &line[self.scroll_x.min(line.len())..];
                styled_lines.push(Line::raw(visible_line));
            }
        }

        let query = Text::from(styled_lines);
        let paragraph = Paragraph::new(query).block(block);

        frame.render_widget(paragraph, area);

        let max_line_width = self.max_line_width();
        let show_scrollbar = max_line_width > self.width;
        if show_scrollbar {
            let mut scrollbar_state = ScrollbarState::default()
                .content_length(self.max_line_width())
                .position(self.x());

            let right_margin = bottom_title_width as u16 + 2;
            let left_margin = 1;

            let scrollbar_area = Rect {
                x: area.x + left_margin,
                y: area.y,
                width: area
                    .width
                    .saturating_sub(right_margin)
                    .saturating_sub(left_margin),
                height: area.height,
            };

            let scrollbar = Scrollbar::new(ScrollbarOrientation::HorizontalBottom)
                .thumb_symbol("🬋")
                .track_symbol(None)
                .begin_symbol(None)
                .end_symbol(None);

            frame.render_stateful_widget(scrollbar, scrollbar_area, &mut scrollbar_state);
        }
    }

    fn handle_focus_event(&mut self, focus: bool) -> Action {
        self.focused = focus;
        Action::Redraw
    }

    fn handle_key_event(&mut self, key: KeyEvent) -> Action {
        use KeyCode::*;

        let is_ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

        match key.code {
            Enter => self.add_line(),
            Backspace => self.remove_char(),
            Delete => self.delete_char(),
            Char('r') if is_ctrl => return Action::RunQuery(self.value()),
            Char('d') if is_ctrl => self.remove_line(),
            Char('w') if is_ctrl => self.remove_word_before_cursor(),
            Char(c) => self.push_char(c),
            Left if self.x() > 0 => {
                self.raw_x = self.x() - 1;
            }
            Left if self.y > 0 => {
                self.y -= 1;
                self.raw_x = self.lines[self.y].len();
            }
            Right if self.x() < self.current_line_width() => {
                self.raw_x = self.x() + 1;
            }
            Right if self.y < self.lines.len() - 1 => {
                self.raw_x = 0;
                self.y += 1;
            }
            Up if self.y > 0 => {
                self.y -= 1;
            }
            Down if self.y < self.lines.len() - 1 => {
                self.y += 1;
            }
            Home => {
                self.raw_x = 0;
            }
            End => {
                self.raw_x = self.current_line_width();
            }
            _ => return Action::None,
        }

        if self.x() < self.scroll_x {
            self.scroll_x = self.x().saturating_sub(self.width);
        } else if self.x() >= self.scroll_x + self.width {
            self.scroll_x = self.x() - self.width + 1;
        }

        Action::Redraw
    }
}
