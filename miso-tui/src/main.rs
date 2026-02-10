mod app;
mod client;
mod common;
mod components;
mod log;
mod text_buffer;

use std::env::args;

use color_eyre::Result;

use crate::app::App;

fn main() -> Result<()> {
    color_eyre::install()?;
    let mut terminal = ratatui::init();
    let app_result = App::new(args().nth(1)).run(&mut terminal);
    ratatui::restore();
    app_result
}
