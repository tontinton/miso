use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Margin, Rect},
    style::{Style, Stylize},
    text::Line,
    widgets::{Block, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
};
use serde_json::{Map, Value};

use crate::{
    common::{BORDER_TYPE, HORIZONTAL},
    components::{Action, Component},
    log::Log,
};

#[derive(Debug, Default)]
pub struct ResultsList {
    focused: bool,
    logs: Vec<Map<String, Value>>,
    logs_lens: Vec<usize>,
    max_len: usize,

    width: usize,
    height: usize,
    selected: usize,
    scroll_x: usize,
    scroll_y: usize,
}

impl ResultsList {
    pub fn is_empty(&self) -> bool {
        self.logs.is_empty()
    }

    pub fn push(&mut self, log: Log) -> Action {
        self.logs.push(log.parsed);
        self.logs_lens.push(log.len);
        if self.max_len < log.len {
            self.max_len = log.len
        }
        if self.logs.len() == 1 {
            Action::PreviewLog(self.logs[self.selected].clone())
        } else {
            Action::Redraw
        }
    }

    pub fn clear(&mut self) -> Action {
        self.logs.clear();
        self.logs_lens.clear();
        self.max_len = 0;
        self.selected = 0;
        self.scroll_x = 0;
        self.scroll_y = 0;
        Action::Redraw
    }

    fn ensure_visible(&mut self, visible_height: usize) {
        if self.selected < self.scroll_y {
            self.scroll_y = self.selected;
        } else if self.selected >= self.scroll_y + visible_height {
            self.scroll_y = self.selected + 1 - visible_height;
        }
    }

    pub fn selected_log(&self) -> &Map<String, Value> {
        &self.logs[self.selected]
    }
}

impl Component for ResultsList {
    fn draw(&mut self, frame: &mut Frame, area: Rect) {
        self.width = area.width as usize - 2;
        self.height = area.height as usize - 2;

        let mut title_text = "Results".white();

        let mut bottom_title_text = if self.logs.is_empty() {
            "0".to_string()
        } else {
            format!(" {}/{} ", self.selected + 1, self.logs.len())
        }
        .white();

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
            .title_bottom(bottom_title.right_aligned());

        let inner = block.inner(area);
        let visible_width = inner.width as usize;
        let visible_height = inner.height as usize;

        self.ensure_visible(visible_height);

        let start = self.scroll_y;
        let end = (start + visible_height).min(self.logs.len());

        let lines: Vec<Line> = self.logs[start..end]
            .iter()
            .enumerate()
            .map(|(i, log)| {
                let text = serde_json::to_string(log).unwrap_or_else(|e| e.to_string());
                let line = Line::from(text);
                if self.focused && start + i == self.selected {
                    line.style(Style::default().reversed())
                } else {
                    line
                }
            })
            .collect();

        let paragraph = Paragraph::new(lines)
            .block(block)
            .scroll((0, self.scroll_x as u16));

        frame.render_widget(paragraph, area);

        let show_vertical_scrollbar = self.logs.len() > visible_height;
        if show_vertical_scrollbar {
            let mut scrollbar_state = ScrollbarState::default()
                .content_length(self.logs.len())
                .position(self.selected);

            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .thumb_symbol("▐")
                .track_symbol(None)
                .begin_symbol(None)
                .end_symbol(None);

            frame.render_stateful_widget(
                scrollbar,
                area.inner(Margin {
                    vertical: 1,
                    horizontal: 0,
                }),
                &mut scrollbar_state,
            );
        }

        let show_horizontal_scrollbar = self.max_len > visible_width;
        if show_horizontal_scrollbar {
            let mut scrollbar_state = ScrollbarState::default()
                .content_length(self.max_len)
                .position(self.scroll_x);

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
            Up | Char('k') if self.selected > 0 => {
                self.selected -= 1;
                return Action::PreviewLog(self.selected_log().clone());
            }
            Down | Char('j') if self.selected < self.logs.len().saturating_sub(1) => {
                self.selected += 1;
                return Action::PreviewLog(self.selected_log().clone());
            }
            PageUp | Char('u') if is_ctrl => {
                self.selected = self.selected.saturating_sub(self.height / 2);
                return Action::PreviewLog(self.selected_log().clone());
            }
            PageDown | Char('d') if is_ctrl => {
                self.selected =
                    (self.selected + self.height / 2).min(self.logs.len().saturating_sub(1));
                return Action::PreviewLog(self.selected_log().clone());
            }
            Left | Char('h') => {
                self.scroll_x = self.scroll_x.saturating_sub(5);
            }
            Right | Char('l') => {
                self.scroll_x = self.scroll_x.saturating_add(5);
            }
            Home | Char('^') => {
                self.scroll_x = 0;
            }
            End | Char('$') if self.selected < self.logs.len().saturating_sub(1) => {
                self.scroll_x = self.logs_lens[self.selected].saturating_sub(self.width);
            }
            _ => return Action::None,
        }

        Action::Redraw
    }
}
