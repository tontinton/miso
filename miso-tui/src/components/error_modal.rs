use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Constraint, Flex, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Text},
    widgets::{Block, Clear, Paragraph, Wrap},
};

use crate::common::{BORDER_TYPE, HORIZONTAL};

use super::{Action, Component};

#[derive(Debug, Default)]
pub struct ErrorModal {
    message: Option<String>,
}

impl ErrorModal {
    pub fn show(&mut self, message: String) {
        self.message = Some(message);
    }

    pub fn is_visible(&self) -> bool {
        self.message.is_some()
    }
}

impl Component for ErrorModal {
    fn draw(&mut self, frame: &mut Frame, area: Rect) {
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

    fn handle_key_event(&mut self, key: KeyEvent) -> Action {
        use KeyCode::*;

        let is_ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

        match key.code {
            Esc | Enter => {
                self.message = None;
                Action::Redraw
            }
            Char('o') if is_ctrl => {
                if let Some(message) = &self.message {
                    Action::CopyToClipboard(message.clone())
                } else {
                    Action::None
                }
            }
            Char('q') => Action::Exit,
            Char('c') if is_ctrl => Action::Exit,
            _ => Action::None,
        }
    }
}

fn popup_area(area: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let vertical = Layout::vertical([Constraint::Percentage(percent_y)]).flex(Flex::Center);
    let horizontal = Layout::horizontal([Constraint::Percentage(percent_x)]).flex(Flex::Center);
    let [area] = vertical.areas(area);
    let [area] = horizontal.areas(area);
    area
}
