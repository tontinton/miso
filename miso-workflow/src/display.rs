use std::fmt;

use miso_workflow_types::sort::Sort;

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
            WorkflowStep::Filter(..) => write!(f, "{pre}Filter"),
            WorkflowStep::Project(..) => write!(f, "{pre}Project"),
            WorkflowStep::Extend(..) => write!(f, "{pre}Extend"),
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

impl fmt::Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}",
            DisplayableWorkflowSteps {
                steps: &self.steps,
                indent: 0,
            }
        )
    }
}

struct DisplayableWorkflowSteps<'a> {
    steps: &'a Vec<WorkflowStep>,
    indent: usize,
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
