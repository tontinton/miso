mod footer;
mod kql_syntax_highlight;
mod log_view;
mod query_input;
mod results_list;
mod results_with_preview;

pub use footer::Footer;
pub use query_input::QueryInput;
pub use results_with_preview::ResultsWithPreview;
use serde_json::{Map, Value};

use std::fmt;

use crossterm::event::KeyEvent;
use ratatui::{Frame, layout::Rect};

#[derive(Debug)]
pub enum Action {
    None,
    Exit,
    Multi(Vec<Action>),
    Redraw,
    RunQuery(String),
    PreviewLog(Map<String, Value>),
    CopyToClipboard(String),
}

pub trait Component: fmt::Debug {
    fn draw(&mut self, frame: &mut Frame, area: Rect);

    #[must_use]
    fn handle_focus_event(&mut self, _focus: bool) -> Action {
        Action::None
    }

    #[must_use]
    fn handle_key_event(&mut self, _key: KeyEvent) -> Action {
        Action::None
    }
}
