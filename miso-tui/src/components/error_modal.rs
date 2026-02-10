use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Constraint, Flex, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Text},
    widgets::{Block, Clear, Paragraph, Wrap},
};

use crate::common::{BORDER_TYPE, HORIZONTAL};

pub enum Msg {
    Show(String),
    Key(KeyEvent),
}

pub enum OutMsg {
    Dismissed,
    CopyError(String),
    Exit,
}

#[derive(Default)]
pub struct ErrorModal {
    message: Option<String>,
}

impl ErrorModal {
    pub fn is_visible(&self) -> bool {
        self.message.is_some()
    }

    pub fn update(&mut self, msg: Msg) -> Option<OutMsg> {
        match msg {
            Msg::Show(message) => {
                self.message = Some(message);
                None
            }
            Msg::Key(key) => self.handle_key(key),
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> Option<OutMsg> {
        let is_ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Esc | KeyCode::Enter => {
                self.message = None;
                Some(OutMsg::Dismissed)
            }
            KeyCode::Char('o') if is_ctrl => self.message.clone().map(OutMsg::CopyError),
            KeyCode::Char('q') => Some(OutMsg::Exit),
            KeyCode::Char('c') if is_ctrl => Some(OutMsg::Exit),
            _ => None,
        }
    }

    pub fn view(&self, frame: &mut Frame, area: Rect) {
        let Some(message) = &self.message else {
            return;
        };

        let popup_area = popup_area(area, 60, 40);
        frame.render_widget(Clear, popup_area);

        let title = Line::from(vec![HORIZONTAL.into(), "Error".white().bold()]);
        let hint = Line::from(vec![
            HORIZONTAL.into(),
            "Esc".white().bold(),
            "/".dark_gray(),
            "Enter".white().bold(),
            " close  ".dark_gray(),
            "Ctrl+O".white().bold(),
            " copy".dark_gray(),
        ]);

        let block = Block::bordered()
            .border_type(BORDER_TYPE)
            .border_style(Style::new().fg(Color::Red))
            .title_top(title)
            .title_bottom(hint);

        let text = Text::from(message.as_str());
        let paragraph = Paragraph::new(text)
            .block(block)
            .wrap(Wrap { trim: false })
            .style(Style::new().white());

        frame.render_widget(paragraph, popup_area);
    }
}

fn popup_area(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let vertical = Layout::vertical([Constraint::Percentage(percent_y)]).flex(Flex::Center);
    let horizontal = Layout::horizontal([Constraint::Percentage(percent_x)]).flex(Flex::Center);
    let [area] = vertical.areas(area);
    let [area] = horizontal.areas(area);
    area
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use test_case::test_case;

    use super::{ErrorModal, Msg, OutMsg};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::empty())
    }

    fn visible_modal() -> ErrorModal {
        let mut m = ErrorModal::default();
        m.update(Msg::Show("err".into()));
        m
    }

    #[test]
    fn show_makes_visible() {
        let modal = visible_modal();
        assert!(modal.is_visible());
    }

    #[test_case(key(KeyCode::Esc) ; "esc")]
    #[test_case(key(KeyCode::Enter) ; "enter")]
    fn dismiss(key: KeyEvent) {
        let mut modal = visible_modal();
        let out = modal.update(Msg::Key(key));
        assert!(matches!(out, Some(OutMsg::Dismissed)));
        assert!(!modal.is_visible());
    }

    #[test]
    fn ctrl_o_copies_error() {
        let mut modal = visible_modal();
        let out = modal.update(Msg::Key(KeyEvent::new(
            KeyCode::Char('o'),
            KeyModifiers::CONTROL,
        )));
        assert!(matches!(out, Some(OutMsg::CopyError(ref s)) if s == "err"));
    }

    #[test_case(key(KeyCode::Char('q')) ; "q")]
    #[test_case(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL) ; "ctrl_c")]
    fn exit(key: KeyEvent) {
        let mut modal = visible_modal();
        assert!(matches!(modal.update(Msg::Key(key)), Some(OutMsg::Exit)));
    }
}
