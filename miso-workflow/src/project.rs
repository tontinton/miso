use color_eyre::eyre::Result;
use miso_workflow_types::{
    log::{Log, LogItem, LogIter},
    project::{ProjectAst, ProjectField},
};
use tracing::warn;

use super::{
    interpreter::{Val, ident},
    try_next_with_partial_passthrough,
};

struct ProjectInterpreter<'a> {
    log: &'a Log,
}

impl<'a> ProjectInterpreter<'a> {
    fn new(log: &'a Log) -> Self {
        Self { log }
    }
}

impl<'a> ProjectInterpreter<'a> {
    fn eval(&self, ast: &'a ProjectAst) -> Result<Val<'a>> {
        Ok(match ast {
            ProjectAst::Id(name) => ident(self.log, name)?,
            ProjectAst::Lit(value) => Val::borrowed(value),
            ProjectAst::Cast(ty, expr) => self.eval(expr)?.cast(*ty)?,
            ProjectAst::Mul(l, r) => self.eval(l)?.mul(&self.eval(r)?)?.into(),
            ProjectAst::Div(l, r) => self.eval(l)?.div(&self.eval(r)?)?.into(),
            ProjectAst::Plus(l, r) => self.eval(l)?.add(&self.eval(r)?)?.into(),
            ProjectAst::Minus(l, r) => self.eval(l)?.sub(&self.eval(r)?)?.into(),
        })
    }
}

pub struct ProjectIter {
    input: LogIter,
    project_fields: Vec<ProjectField>,
    extend: bool,
}

impl ProjectIter {
    pub fn new_project(input: LogIter, project_fields: Vec<ProjectField>) -> Self {
        Self {
            input,
            project_fields,
            extend: false,
        }
    }

    pub fn new_extend(input: LogIter, project_fields: Vec<ProjectField>) -> Self {
        Self {
            input,
            project_fields,
            extend: true,
        }
    }
}

impl Iterator for ProjectIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        let mut log = try_next_with_partial_passthrough!(self.input)?;
        let mut output = Log::new();

        {
            let interpreter = ProjectInterpreter::new(&log);

            for field in &self.project_fields {
                match interpreter.eval(&field.from) {
                    Ok(Val(None)) => {} // Skip.
                    Ok(v) => {
                        let owned = v.0.unwrap().into_owned();
                        output.insert(field.to.clone(), owned);
                    }
                    Err(e) => {
                        warn!("Project failed: {e}");
                        continue;
                    }
                };
            }
        }

        if self.extend {
            log.extend(output);
        } else {
            log = output;
        }

        Some(LogItem::Log(log))
    }
}
