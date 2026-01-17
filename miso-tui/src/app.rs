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
    components::{Action, Component, ErrorModal, Footer, QueryInput, ResultsWithPreview},
};

macro_rules! act {
    ($self:expr, $action:expr) => {{
        let action = $action;
        $self.handle_action(action);
    }};
}

/// How many logs to try to fetch (without blocking) before updating the UI.
const LOGS_CHUNK: usize = 4096;

#[derive(Debug)]
enum FocusedWindow {
    Results,
    Query,
    Footer,
}

pub struct App {
    results_view: ResultsWithPreview,
    query_input_view: QueryInput,
    footer_view: Footer,
    error_modal: ErrorModal,

    focused: FocusedWindow,
    redraw: bool,
    exit: bool,
    clipboard: Clipboard,

    query_rx: Option<mpsc::Receiver<StreamMessage>>,
}

impl App {
    pub fn new(query: Option<String>) -> Self {
        let input_given = query.is_some();

        let mut app = Self {
            results_view: ResultsWithPreview::default(),
            query_input_view: QueryInput::new(query.unwrap_or("".to_string())),
            footer_view: Footer::default(),
            error_modal: ErrorModal::default(),

            focused: FocusedWindow::Query,
            redraw: true,
            exit: false,
            clipboard: Clipboard::new().expect("failed to init clipboard"),

            query_rx: None,
        };

        if input_given {
            app.handle_action(Action::RunQuery(app.query_input_view.value()));
        }

        app
    }
}

impl App {
    pub fn run(&mut self, terminal: &mut DefaultTerminal) -> Result<()> {
        act!(
            self,
            match self.focused {
                FocusedWindow::Query => self.query_input_view.handle_focus_event(true),
                FocusedWindow::Results => self.results_view.handle_focus_event(true),
                FocusedWindow::Footer => self.footer_view.handle_focus_event(true),
            }
        );

        while !self.exit {
            if self.redraw {
                terminal.draw(|frame| self.draw(frame))?;
                self.redraw = false;
            }
            self.handle_events()?;
        }

        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame) {
        let add_footer = matches!(self.focused, FocusedWindow::Footer);

        let mut constraints = vec![
            Constraint::Min(1),
            Constraint::Length(self.query_input_view.height()),
        ];
        if add_footer {
            constraints.push(Constraint::Length(self.footer_view.height()));
        }

        let vertical = &Layout::vertical(constraints);
        let rects = vertical.split(frame.area());

        self.results_view.draw(frame, rects[0]);
        self.query_input_view.draw(frame, rects[1]);
        if add_footer {
            self.footer_view.draw(frame, rects[2]);
        }

        self.error_modal.draw(frame, frame.area());
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
                        act!(self, self.results_view.results_list.push(log));
                        logs_received += 1;
                        continue;
                    }
                    Ok(StreamMessage::Error(error)) => {
                        self.handle_action(Action::ShowError(error));
                        self.query_rx = None;
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        self.query_rx = None;
                        break;
                    }
                    Err(TryRecvError::Empty) if logs_received > 0 => {
                        break;
                    }
                    Err(TryRecvError::Empty) => {}
                }
            }

            if event::poll(Duration::from_millis(1))? {
                self.handle_term_event()?;
                break;
            }
        }

        Ok(())
    }

    fn handle_term_event(&mut self) -> Result<()> {
        match event::read()? {
            Event::Key(key_event) if key_event.kind == KeyEventKind::Press => {
                self.handle_key_event(key_event)
            }
            Event::Resize(..) => {
                self.redraw = true;
            }
            _ => {}
        };
        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        // Handle error modal keys first
        if self.error_modal.is_visible() {
            act!(self, self.error_modal.handle_key_event(key_event));
            return;
        }

        match key_event.code {
            KeyCode::Esc => match self.focused {
                FocusedWindow::Results => {
                    act!(self, self.results_view.handle_key_event(key_event));
                }
                FocusedWindow::Query => {
                    act!(self, self.query_input_view.handle_key_event(key_event));
                }
                FocusedWindow::Footer => {
                    self.focused = FocusedWindow::Results;
                    self.redraw = true;
                }
            },
            KeyCode::Char(':') => match self.focused {
                FocusedWindow::Results => {
                    self.focused = FocusedWindow::Footer;
                    self.redraw = true;
                }
                FocusedWindow::Footer => {
                    act!(self, self.footer_view.handle_key_event(key_event));
                }
                FocusedWindow::Query => {
                    act!(self, self.query_input_view.handle_key_event(key_event));
                }
            },
            KeyCode::Tab => match self.focused {
                FocusedWindow::Results => {
                    act!(self, self.results_view.handle_focus_event(false));
                    self.focused = FocusedWindow::Query;
                    act!(self, self.query_input_view.handle_focus_event(true));
                }
                FocusedWindow::Query => {
                    act!(self, self.query_input_view.handle_focus_event(false));
                    self.focused = FocusedWindow::Results;
                    act!(self, self.results_view.handle_focus_event(true));
                }
                FocusedWindow::Footer => {
                    act!(self, self.footer_view.handle_key_event(key_event))
                }
            },
            _ => match self.focused {
                FocusedWindow::Results => {
                    act!(self, self.results_view.handle_key_event(key_event));
                }
                FocusedWindow::Query => {
                    act!(self, self.query_input_view.handle_key_event(key_event));
                }
                FocusedWindow::Footer => {
                    act!(self, self.footer_view.handle_key_event(key_event));
                }
            },
        }
    }

    fn handle_action(&mut self, action: Action) {
        match action {
            Action::None => {}
            Action::Exit => self.exit(),
            Action::Multi(actions) => {
                for a in actions {
                    self.handle_action(a);
                }
            }
            Action::Redraw => self.redraw = true,
            Action::RunQuery(query) => {
                if self.query_rx.is_some() {
                    return;
                }

                act!(self, self.results_view.results_list.clear());

                act!(self, self.query_input_view.handle_focus_event(false));
                self.focused = FocusedWindow::Results;
                act!(self, self.results_view.handle_focus_event(true));

                let (tx, rx) = mpsc::channel();
                self.query_rx = Some(rx);
                std::thread::spawn(move || {
                    query_stream(&query, tx);
                });
            }
            Action::PreviewLog(log) => {
                act!(self, self.results_view.log_view.set_log(log));
            }
            Action::CopyToClipboard(text) => {
                if let Err(e) = self.copy_to_clipboard(&text) {
                    self.handle_action(Action::ShowError(format!(
                        "Failed to copy to clipboard: {e}"
                    )));
                }
            }
            Action::ShowError(message) => {
                self.error_modal.show(message);
                act!(self, self.query_input_view.handle_focus_event(true));
                self.focused = FocusedWindow::Query;
                act!(self, self.results_view.handle_focus_event(false));
                self.redraw = true;
            }
        }
    }

    fn copy_to_clipboard(&mut self, text: &str) -> Result<()> {
        self.clipboard.set_text(text.to_string())?;
        Ok(())
    }

    fn exit(&mut self) {
        self.exit = true;
    }
}
