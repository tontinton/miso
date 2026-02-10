use crate::{
    components::{log_view, log_view::LogView, results_list, results_list::ResultsList},
    log::Log,
};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
};

pub enum Msg {
    Key(KeyEvent),
    PushLog(Log),
    Clear,
}

pub enum OutMsg {
    CopyLog(String),
    Exit,
}

#[derive(Default)]
enum Mode {
    #[default]
    ListWithPreview,
    OnlyLog,
}

#[derive(Default)]
pub struct ResultsWithPreview {
    results_list: ResultsList,
    log_view: LogView,
    mode: Mode,
    focused: bool,
}

impl ResultsWithPreview {
    pub fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
        match self.mode {
            Mode::ListWithPreview => self.results_list.set_focused(focused),
            Mode::OnlyLog => self.log_view.set_focused(focused),
        }
    }

    pub fn update(&mut self, msg: Msg) -> Option<OutMsg> {
        match msg {
            Msg::PushLog(log) => {
                if let Some(results_list::OutMsg::SelectedLog(l)) =
                    self.results_list.update(results_list::Msg::PushLog(log))
                {
                    self.log_view.update(log_view::Msg::SetLog(l));
                }
                None
            }
            Msg::Clear => {
                self.results_list.update(results_list::Msg::Clear);
                self.log_view.update(log_view::Msg::Clear);
                None
            }
            Msg::Key(key) => self.handle_key(key),
        }
    }

    fn handle_key(&mut self, key: KeyEvent) -> Option<OutMsg> {
        let is_ctrl = key.modifiers.contains(KeyModifiers::CONTROL);

        match key.code {
            KeyCode::Char('o') if is_ctrl => {
                if !self.results_list.is_empty() {
                    let text =
                        serde_json::to_string(self.results_list.selected_log()).unwrap_or_default();
                    return Some(OutMsg::CopyLog(text));
                }
            }
            KeyCode::Char('q') => return Some(OutMsg::Exit),
            KeyCode::Char('c') if is_ctrl => return Some(OutMsg::Exit),
            _ => {}
        }

        match self.mode {
            Mode::ListWithPreview => self.handle_list_key(key),
            Mode::OnlyLog => self.handle_log_key(key),
        }
    }

    fn handle_list_key(&mut self, key: KeyEvent) -> Option<OutMsg> {
        if key.code == KeyCode::Enter && !self.results_list.is_empty() {
            self.mode = Mode::OnlyLog;
            self.results_list.set_focused(false);
            self.log_view.set_focused(true);
            return None;
        }

        if let Some(results_list::OutMsg::SelectedLog(log)) =
            self.results_list.update(results_list::Msg::Key(key))
        {
            self.log_view.update(log_view::Msg::SetLog(log));
        }
        None
    }

    fn handle_log_key(&mut self, key: KeyEvent) -> Option<OutMsg> {
        match key.code {
            KeyCode::Esc | KeyCode::Backspace => {
                self.mode = Mode::ListWithPreview;
                self.log_view.set_focused(false);
                self.results_list.set_focused(true);
            }
            _ => self.log_view.update(log_view::Msg::Key(key)),
        }
        None
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) {
        match self.mode {
            Mode::ListWithPreview if self.results_list.is_empty() => {
                self.results_list.view(frame, area);
            }
            Mode::ListWithPreview => {
                let [left, right] =
                    Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)])
                        .areas(area);
                self.results_list.view(frame, left);
                self.log_view.view(frame, right);
            }
            Mode::OnlyLog => {
                self.log_view.view(frame, area);
            }
        }
    }
}
