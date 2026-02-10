use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::Rect,
    style::{Style, Stylize},
    text::Line,
    widgets::Paragraph,
};
use serde_json::{Map, Value};

use crate::{
    common::{HORIZONTAL, render_horizontal_scrollbar, render_vertical_scrollbar, styled_block},
    log::Log,
};

pub enum Msg {
    Key(KeyEvent),
    PushLog(Log),
    Clear,
}

pub enum OutMsg {
    SelectedLog(Map<String, Value>),
}

#[derive(Default)]
pub struct ResultsList {
    logs: Vec<Map<String, Value>>,
    log_lens: Vec<usize>,
    max_len: usize,
    selected: usize,
    scroll_x: usize,
    scroll_y: usize,
    focused: bool,
    last_area: Rect,
}

impl ResultsList {
    pub fn is_empty(&self) -> bool {
        self.logs.is_empty()
    }

    pub fn selected_log(&self) -> &Map<String, Value> {
        debug_assert!(
            !self.logs.is_empty(),
            "selected_log called on empty ResultsList"
        );
        &self.logs[self.selected]
    }

    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    fn visible_height(&self) -> usize {
        self.last_area.height.saturating_sub(2) as usize
    }

    fn visible_width(&self) -> usize {
        self.last_area.width.saturating_sub(2) as usize
    }

    fn ensure_visible(&mut self) {
        let h = self.visible_height();
        if h == 0 {
            return;
        }
        if self.selected < self.scroll_y {
            self.scroll_y = self.selected;
        } else if self.selected >= self.scroll_y + h {
            self.scroll_y = self.selected + 1 - h;
        }
    }

    pub fn update(&mut self, msg: Msg) -> Option<OutMsg> {
        match msg {
            Msg::PushLog(log) => {
                if log.len > self.max_len {
                    self.max_len = log.len;
                }
                self.log_lens.push(log.len);
                self.logs.push(log.parsed);
                if self.logs.len() == 1 {
                    return Some(OutMsg::SelectedLog(self.logs[0].clone()));
                }
                None
            }
            Msg::Clear => {
                self.logs.clear();
                self.log_lens.clear();
                self.max_len = 0;
                self.selected = 0;
                self.scroll_x = 0;
                self.scroll_y = 0;
                None
            }
            Msg::Key(key) => self.handle_key(key),
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> Option<OutMsg> {
        use KeyCode::*;
        let is_ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let h = self.visible_height();

        match key.code {
            Up | Char('k') if self.selected > 0 => {
                self.selected -= 1;
                self.ensure_visible();
                return Some(OutMsg::SelectedLog(self.selected_log().clone()));
            }
            Down | Char('j') if self.selected < self.logs.len().saturating_sub(1) => {
                self.selected += 1;
                self.ensure_visible();
                return Some(OutMsg::SelectedLog(self.selected_log().clone()));
            }
            PageUp | Char('u') if is_ctrl => {
                self.selected = self.selected.saturating_sub(h / 2);
                self.ensure_visible();
                return Some(OutMsg::SelectedLog(self.selected_log().clone()));
            }
            PageDown | Char('d') if is_ctrl => {
                self.selected = (self.selected + h / 2).min(self.logs.len().saturating_sub(1));
                self.ensure_visible();
                return Some(OutMsg::SelectedLog(self.selected_log().clone()));
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
            End | Char('$') if !self.logs.is_empty() => {
                self.scroll_x = self.log_lens[self.selected].saturating_sub(self.visible_width());
            }
            _ => {}
        }
        None
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) {
        self.last_area = area;

        let bottom_text = if self.logs.is_empty() {
            "0".to_string()
        } else {
            format!(" {}/{} ", self.selected + 1, self.logs.len())
        };

        let mut bottom_title_text = bottom_text.white();
        if self.focused {
            bottom_title_text = bottom_title_text.bold();
        }
        let bottom_title = Line::from(vec![bottom_title_text, HORIZONTAL.into()]);
        let bottom_title_width = bottom_title.width() as u16 + 2;

        let block = styled_block("Results", self.focused, Some(bottom_title));
        let inner = block.inner(area);
        let visible_height = inner.height as usize;
        let visible_width = inner.width as usize;

        let start = self.scroll_y;
        let end = (start + visible_height).min(self.logs.len());

        let lines: Vec<Line> = self.logs[start..end]
            .iter()
            .enumerate()
            .map(|(i, log)| {
                let text = serde_json::to_string(log).unwrap_or_default();
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

        if self.logs.len() > visible_height {
            render_vertical_scrollbar(frame, area, self.logs.len(), self.selected);
        }

        if self.max_len > visible_width {
            render_horizontal_scrollbar(
                frame,
                area,
                self.max_len,
                self.scroll_x,
                bottom_title_width,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use serde_json::{Map, Value};

    use super::{Msg, OutMsg, ResultsList};
    use crate::log::Log;

    fn make_log(key: &str) -> Log {
        let mut map = Map::new();
        map.insert(key.to_string(), Value::String("v".to_string()));
        let len = serde_json::to_string(&map).unwrap_or_default().len();
        Log { parsed: map, len }
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn two_item_list() -> ResultsList {
        let mut rl = ResultsList::default();
        rl.update(Msg::PushLog(make_log("a")));
        rl.update(Msg::PushLog(make_log("b")));
        rl
    }

    #[test]
    fn push_first_emits_selected_then_none() {
        let mut rl = ResultsList::default();
        assert!(matches!(
            rl.update(Msg::PushLog(make_log("a"))),
            Some(OutMsg::SelectedLog(_))
        ));
        assert!(rl.update(Msg::PushLog(make_log("b"))).is_none());
    }

    #[test]
    fn clear_resets() {
        let mut rl = two_item_list();
        rl.update(Msg::Clear);
        assert!(rl.is_empty());
    }

    #[test]
    fn navigation_emits_on_move_none_at_bounds() {
        let mut rl = two_item_list();
        assert!(matches!(
            rl.update(Msg::Key(key(KeyCode::Down))),
            Some(OutMsg::SelectedLog(_))
        ));
        assert!(rl.update(Msg::Key(key(KeyCode::Down))).is_none());
        assert!(matches!(
            rl.update(Msg::Key(key(KeyCode::Up))),
            Some(OutMsg::SelectedLog(_))
        ));
        assert!(rl.update(Msg::Key(key(KeyCode::Up))).is_none());
    }
}
