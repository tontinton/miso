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
    client::query_stream,
    components::{Action, Component, Footer, QueryInput, ResultsWithPreview},
    log::Log,
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

    focused: FocusedWindow,
    redraw: bool,
    exit: bool,
    clipboard: Clipboard,

    query_rx: Option<mpsc::Receiver<Log>>,
}

impl Default for App {
    fn default() -> Self {
        Self {
            results_view: ResultsWithPreview::default(),
            query_input_view: QueryInput::default(),
            footer_view: Footer::default(),

            focused: FocusedWindow::Query,
            redraw: true,
            exit: false,
            clipboard: Clipboard::new().expect("failed to init clipboard"),

            query_rx: None,
        }
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
                    Ok(log) => {
                        act!(self, self.results_view.results_list.push(log));
                        logs_received += 1;
                        continue;
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
                    if let Err(e) = query_stream(&query, tx) {
                        eprintln!("Query stream error: {e}");
                    }
                });
            }
            Action::PreviewLog(log) => {
                act!(self, self.results_view.log_view.set_log(log));
            }
            Action::CopyToClipboard(text) => {
                if let Err(e) = self.copy_to_clipboard(&text) {
                    eprintln!("Failed to copy to clipboard: {e}");
                }
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
