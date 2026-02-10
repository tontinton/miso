use std::{
    sync::mpsc::{self, TryRecvError},
    time::Duration,
};

use arboard::Clipboard;
use color_eyre::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Layout},
};

use crate::{
    client::{StreamMessage, query_stream},
    components::{
        error_modal::{self, ErrorModal},
        footer::{self, Footer},
        query_input::{self, QueryInput},
        results_with_preview::{self, ResultsWithPreview},
    },
};

/// How many logs to try to fetch (without blocking) before updating the UI.
const LOGS_CHUNK: usize = 4096;

enum FocusedWindow {
    Results,
    Query,
    Footer,
}

pub struct App {
    results_view: ResultsWithPreview,
    query_input: QueryInput,
    footer: Footer,
    error_modal: ErrorModal,

    focused: FocusedWindow,
    redraw: bool,
    exit: bool,
    clipboard: Option<Clipboard>,

    query_rx: Option<mpsc::Receiver<StreamMessage>>,
}

impl App {
    pub fn new(query: Option<String>) -> Self {
        let input_given = query.is_some();

        let mut app = Self {
            results_view: ResultsWithPreview::default(),
            query_input: QueryInput::new(query.unwrap_or_default()),
            footer: Footer::default(),
            error_modal: ErrorModal::default(),

            focused: FocusedWindow::Query,
            redraw: true,
            exit: false,
            clipboard: Clipboard::new().ok(),

            query_rx: None,
        };

        if input_given {
            app.run_query(app.query_input.value());
        }

        app
    }

    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        self.change_focus(FocusedWindow::Query);

        while !self.exit {
            if self.redraw {
                terminal.draw(|f| self.view(f))?;
                self.redraw = false;
            }

            self.handle_events()?;
        }

        Ok(())
    }

    fn view(&mut self, frame: &mut Frame) {
        let add_footer = matches!(self.focused, FocusedWindow::Footer);

        let mut constraints = vec![
            Constraint::Min(1),
            Constraint::Length(self.query_input.height()),
        ];
        if add_footer {
            constraints.push(Constraint::Length(self.footer.height()));
        }

        let rects = Layout::vertical(constraints).split(frame.area());

        self.results_view.view(frame, rects[0]);
        self.query_input.view(frame, rects[1]);
        if add_footer {
            self.footer.view(frame, rects[2]);
        }

        self.error_modal.view(frame, frame.area());
    }

    fn handle_events(&mut self) -> Result<()> {
        if event::poll(Duration::ZERO)? {
            self.handle_term_event()?;
            return Ok(());
        }

        let mut logs_received = 0;
        while logs_received < LOGS_CHUNK {
            if let Some(rx) = &self.query_rx {
                match rx.try_recv() {
                    Ok(StreamMessage::Log(log)) => {
                        self.push_log(log);
                        logs_received += 1;
                        continue;
                    }
                    Ok(StreamMessage::Error(e)) => {
                        self.query_rx = None;
                        self.error_modal.update(error_modal::Msg::Show(e));
                        self.change_focus(FocusedWindow::Query);
                        self.redraw = true;
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        self.query_rx = None;
                        break;
                    }
                    Err(TryRecvError::Empty) if logs_received > 0 => break,
                    Err(TryRecvError::Empty) => {}
                }
            }

            if event::poll(Duration::from_millis(1))? {
                self.handle_term_event()?;
                break;
            }
        }

        if logs_received > 0 {
            self.redraw = true;
        }

        Ok(())
    }

    fn handle_term_event(&mut self) -> Result<()> {
        match event::read()? {
            Event::Key(key) if key.kind == KeyEventKind::Press => {
                self.route_key(key);
                self.redraw = true;
            }
            Event::Resize(..) => self.redraw = true,
            _ => {}
        }
        Ok(())
    }

    fn route_key(&mut self, key: KeyEvent) {
        if self.error_modal.is_visible() {
            if let Some(out) = self.error_modal.update(error_modal::Msg::Key(key)) {
                self.handle_error_out(out);
            }
            return;
        }

        match key.code {
            KeyCode::Esc => match self.focused {
                FocusedWindow::Footer => self.change_focus(FocusedWindow::Results),
                FocusedWindow::Results => {
                    self.results_view
                        .update(results_with_preview::Msg::Key(key));
                }
                FocusedWindow::Query => {}
            },
            KeyCode::Char(':') => match self.focused {
                FocusedWindow::Results => self.change_focus(FocusedWindow::Footer),
                FocusedWindow::Footer => {
                    self.footer.update(footer::Msg::Key(key));
                }
                FocusedWindow::Query => {
                    self.query_input.update(query_input::Msg::Key(key));
                }
            },
            KeyCode::Tab => match self.focused {
                FocusedWindow::Results => self.change_focus(FocusedWindow::Query),
                FocusedWindow::Query => self.change_focus(FocusedWindow::Results),
                FocusedWindow::Footer => {
                    self.footer.update(footer::Msg::Key(key));
                }
            },
            _ => self.route_to_focused(key),
        }
    }

    fn route_to_focused(&mut self, key: KeyEvent) {
        match self.focused {
            FocusedWindow::Results => {
                if let Some(out) = self
                    .results_view
                    .update(results_with_preview::Msg::Key(key))
                {
                    self.handle_results_out(out);
                }
            }
            FocusedWindow::Query => {
                if let Some(out) = self.query_input.update(query_input::Msg::Key(key)) {
                    self.handle_query_out(out);
                }
            }
            FocusedWindow::Footer => {
                if let Some(out) = self.footer.update(footer::Msg::Key(key)) {
                    self.handle_footer_out(out);
                }
            }
        }
    }

    fn handle_results_out(&mut self, out: results_with_preview::OutMsg) {
        match out {
            results_with_preview::OutMsg::CopyLog(text) => self.copy_to_clipboard(&text),
            results_with_preview::OutMsg::Exit => self.exit = true,
        }
    }

    fn handle_query_out(&mut self, out: query_input::OutMsg) {
        let query_input::OutMsg::RunQuery(q) = out;
        self.run_query(q);
    }

    fn handle_footer_out(&mut self, out: footer::OutMsg) {
        let footer::OutMsg::Command(cmd) = out;
        self.handle_command(&cmd);
    }

    fn handle_error_out(&mut self, out: error_modal::OutMsg) {
        match out {
            error_modal::OutMsg::Dismissed => self.change_focus(FocusedWindow::Query),
            error_modal::OutMsg::CopyError(text) => self.copy_to_clipboard(&text),
            error_modal::OutMsg::Exit => self.exit = true,
        }
    }

    fn handle_command(&mut self, cmd: &str) {
        if cmd == "q" {
            self.exit = true;
        }
    }

    fn push_log(&mut self, log: crate::log::Log) {
        self.results_view
            .update(results_with_preview::Msg::PushLog(log));
    }

    fn run_query(&mut self, query: String) {
        if self.query_rx.is_some() {
            return;
        }

        self.results_view.update(results_with_preview::Msg::Clear);
        self.change_focus(FocusedWindow::Results);

        let (tx, rx) = mpsc::channel();
        self.query_rx = Some(rx);
        std::thread::spawn(move || {
            query_stream(&query, tx);
        });
    }

    fn change_focus(&mut self, target: FocusedWindow) {
        match self.focused {
            FocusedWindow::Results => self.results_view.set_focused(false),
            FocusedWindow::Query => self.query_input.set_focused(false),
            FocusedWindow::Footer => {}
        }

        self.focused = target;

        match self.focused {
            FocusedWindow::Results => self.results_view.set_focused(true),
            FocusedWindow::Query => self.query_input.set_focused(true),
            FocusedWindow::Footer => {}
        }
    }

    fn copy_to_clipboard(&mut self, text: &str) {
        match &mut self.clipboard {
            Some(cb) => {
                if let Err(e) = cb.set_text(text.to_string()) {
                    self.error_modal.update(error_modal::Msg::Show(format!(
                        "Failed to copy to clipboard: {e}"
                    )));
                    self.change_focus(FocusedWindow::Query);
                }
            }
            None => {
                self.error_modal.update(error_modal::Msg::Show(
                    "Clipboard not available".to_string(),
                ));
                self.change_focus(FocusedWindow::Query);
            }
        }
    }
}
