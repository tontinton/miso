use miso_workflow_types::{
    expr::Expr,
    log::{LogItem, LogIter},
};
use tracing::warn;

use super::{interpreter::LogInterpreter, try_next_with_partial_passthrough};

pub struct FilterIter {
    input: LogIter,
    expr: Expr,
}

impl FilterIter {
    pub fn new(input: LogIter, expr: Expr) -> Self {
        Self { input, expr }
    }
}

impl Iterator for FilterIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(log) = try_next_with_partial_passthrough!(self.input) {
            let interpreter = LogInterpreter { log: &log };
            let keep = match interpreter.eval(&self.expr) {
                Ok(v) => v.to_bool(),
                Err(e) => {
                    warn!("Filter failed: {e}");
                    false
                }
            };

            if !keep {
                continue;
            }

            return Some(LogItem::Log(log));
        }
        None
    }
}
