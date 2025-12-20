use std::fmt;

use miso_workflow_types::{field::Field, project::ProjectField, sort::Sort};

use super::{Workflow, WorkflowStep};

impl fmt::Display for WorkflowStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let display_step = DisplayableWorkflowStep {
            step: self,
            indent: 0,
            section: None,
        };
        write!(f, "{display_step}")
    }
}

#[derive(Clone, Copy)]
enum DisplayableSection {
    Start,
    Middle,
    End,

    Single,
}

struct DisplayableWorkflowStep<'a> {
    step: &'a WorkflowStep,
    indent: usize,
    section: Option<DisplayableSection>,
}

impl fmt::Display for DisplayableWorkflowStep<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut pre = String::new();

        if let Some(section) = self.section {
            let c = match (section, self.indent == 0) {
                (_, false) => '│',
                (DisplayableSection::Single, _) => '─',
                (DisplayableSection::Start, true) => '┌',
                (DisplayableSection::Middle, true) => '├',
                (DisplayableSection::End, true) => '└',
            };
            pre.push(c);

            for i in 1..=self.indent {
                pre.push(' ');
                if i == self.indent {
                    let c = match section {
                        DisplayableSection::Start | DisplayableSection::Middle => '├',
                        DisplayableSection::Single | DisplayableSection::End => '└',
                    };
                    pre.push(c);
                    pre.push('─');
                } else {
                    let c = match section {
                        DisplayableSection::Start | DisplayableSection::Middle => '│',
                        DisplayableSection::Single | DisplayableSection::End => ' ',
                    };
                    pre.push(c);
                    pre.push(' ');
                }
            }

            pre.push('─');
            match self.step {
                WorkflowStep::Join(..) | WorkflowStep::Union(..) => pre.push('┬'),
                _ => pre.push('─'),
            }
            pre.push('─');
        }

        match self.step {
            WorkflowStep::Scan(scan) if scan.dynamic_filter_rx.is_some() => {
                write!(
                    f,
                    "{}Scan({}.{}){{{}}}[→DF]",
                    pre, scan.connector_name, scan.collection, scan.handle
                )
            }
            WorkflowStep::Scan(scan) => write!(
                f,
                "{}Scan({}.{}){{{}}}",
                pre, scan.connector_name, scan.collection, scan.handle
            ),
            WorkflowStep::Filter(expr) => write!(f, "{pre}Filter({expr})"),
            WorkflowStep::Project(projects) => {
                write!(f, "{pre}Project")?;
                fmt_projects(f, projects)
            }
            WorkflowStep::Extend(projects) => {
                write!(f, "{pre}Extend")?;
                fmt_projects(f, projects)
            }
            WorkflowStep::Rename(renames) => {
                write!(f, "{pre}Rename")?;
                fmt_rename(f, renames)
            }
            WorkflowStep::Expand(expand) => write!(f, "{pre}Expand({expand})"),
            WorkflowStep::Limit(limit) => write!(f, "{pre}Limit({limit})"),
            WorkflowStep::MuxLimit(limit) => write!(f, "{pre}MuxLimit({limit})"),
            WorkflowStep::Sort(sorts) => {
                write!(f, "{pre}Sort")?;
                fmt_sorts(f, sorts)
            }
            WorkflowStep::TopN(sorts, limit) => {
                write!(f, "{pre}TopN({limit})")?;
                fmt_sorts(f, sorts)
            }
            WorkflowStep::MuxTopN(sorts, limit) => {
                write!(f, "{pre}MuxTopN({limit})")?;
                fmt_sorts(f, sorts)
            }
            WorkflowStep::Summarize(summarize) => write!(f, "{pre}Summarize({summarize})"),
            WorkflowStep::MuxSummarize(summarize) => {
                write!(f, "{pre}MuxSummarize({summarize})")
            }
            WorkflowStep::Union(workflow) => {
                let display_steps = DisplayableWorkflowSteps {
                    steps: &workflow.steps,
                    indent: self.indent + 1,
                };
                write!(f, "{pre}Union\n{display_steps}")
            }
            WorkflowStep::Join(join, workflow) => {
                let dynamic_filter_tx = match workflow.steps.first() {
                    Some(WorkflowStep::Scan(scan)) if scan.dynamic_filter_tx.is_some() => {
                        "[DF→]".to_string()
                    }
                    _ => String::new(),
                };

                let display_steps = DisplayableWorkflowSteps {
                    steps: &workflow.steps,
                    indent: self.indent + 1,
                };
                write!(f, "{pre}Join({join}){dynamic_filter_tx}\n{display_steps}")
            }
            WorkflowStep::Count => write!(f, "{pre}Count"),
            WorkflowStep::MuxCount => write!(f, "{pre}MuxCount"),
            WorkflowStep::Tee(_) => write!(f, "{pre}Tee"),
        }
    }
}

fn fmt_sorts(f: &mut fmt::Formatter<'_>, sorts: &[Sort]) -> fmt::Result {
    write!(f, "(")?;
    for (i, sort) in sorts.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{sort}")?;
    }
    write!(f, ")")
}

fn fmt_projects(f: &mut fmt::Formatter<'_>, projects: &[ProjectField]) -> fmt::Result {
    write!(f, "(")?;
    for (i, pf) in projects.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{pf}")?;
    }
    write!(f, ")")
}

fn fmt_rename(f: &mut fmt::Formatter<'_>, renames: &[(Field, Field)]) -> fmt::Result {
    write!(f, "(")?;
    for (i, (from, to)) in renames.iter().enumerate() {
        if i > 0 {
            write!(f, ", ")?;
        }
        write!(f, "{to}={from}")?;
    }
    write!(f, ")")
}

impl fmt::Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", DisplayableWorkflowSteps::new(&self.steps))
    }
}

pub struct DisplayableWorkflowSteps<'a> {
    steps: &'a [WorkflowStep],
    indent: usize,
}

impl<'a> DisplayableWorkflowSteps<'a> {
    pub fn new(steps: &'a [WorkflowStep]) -> Self {
        Self { steps, indent: 0 }
    }
}

impl fmt::Display for DisplayableWorkflowSteps<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, step) in self.steps.iter().enumerate() {
            let section = if self.steps.len() == 1 {
                DisplayableSection::Single
            } else if i == 0 {
                DisplayableSection::Start
            } else if i == self.steps.len() - 1 {
                DisplayableSection::End
            } else {
                DisplayableSection::Middle
            };

            if i > 0 {
                writeln!(f)?;
            }

            let display_step = DisplayableWorkflowStep {
                step,
                indent: self.indent,
                section: Some(section),
            };
            write!(f, "{display_step}")?;
        }
        Ok(())
    }
}
