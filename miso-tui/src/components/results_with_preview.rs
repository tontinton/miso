use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
};

use crate::components::{Action, Component, log_view::LogView, results_list::ResultsList};

#[derive(Debug, Default)]
enum Mode {
    #[default]
    ListWithPreview,
    OnlyLog,
}

#[derive(Debug, Default)]
pub struct ResultsWithPreview {
    pub results_list: ResultsList,
    pub log_view: LogView,

    mode: Mode,
}

impl Component for ResultsWithPreview {
    fn draw(&mut self, frame: &mut Frame, area: Rect) {
        match self.mode {
            Mode::ListWithPreview if self.results_list.is_empty() => {
                self.results_list.draw(frame, area);
            }
            Mode::ListWithPreview => {
                let layout =
                    &Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]);
                let rects = layout.split(area);

                self.results_list.draw(frame, rects[0]);
                self.log_view.draw(frame, rects[1]);
            }
            Mode::OnlyLog => {
                self.log_view.draw(frame, area);
            }
        }
    }

    fn handle_focus_event(&mut self, focus: bool) -> Action {
        match self.mode {
            Mode::ListWithPreview => Action::Multi(vec![
                self.results_list.handle_focus_event(focus),
                Action::Redraw,
            ]),
            Mode::OnlyLog => Action::Multi(vec![
                self.log_view.handle_focus_event(focus),
                Action::Redraw,
            ]),
        }
    }

    fn handle_key_event(&mut self, key: KeyEvent) -> Action {
        use KeyCode::*;

        let is_ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

        match key.code {
            Char('o') if is_ctrl => {
                return Action::CopyToClipboard(
                    serde_json::to_string(self.results_list.selected_log())
                        .expect("log to be a json"),
                );
            }
            Char('q') => {
                return Action::Exit;
            }
            Char('c') if is_ctrl => {
                return Action::Exit;
            }
            _ => {}
        }

        match self.mode {
            Mode::ListWithPreview => match key.code {
                Enter => {
                    if self.results_list.is_empty() {
                        return Action::None;
                    }

                    self.mode = Mode::OnlyLog;
                    Action::Multi(vec![self.log_view.handle_focus_event(true), Action::Redraw])
                }
                _ => self.results_list.handle_key_event(key),
            },
            Mode::OnlyLog => match key.code {
                Esc | Backspace => {
                    self.mode = Mode::ListWithPreview;
                    Action::Multi(vec![
                        self.log_view.handle_focus_event(false),
                        Action::Redraw,
                    ])
                }
                _ => self.log_view.handle_key_event(key),
            },
        }
    }
}
