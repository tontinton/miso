use miso_common::watch::Watch;
use miso_workflow::{WorkflowStep, scan::Scan};

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct DynamicFilter {
    max_distinct_values: u32,
}

impl DynamicFilter {
    pub fn new(max_distinct_values: u32) -> Self {
        Self {
            max_distinct_values,
        }
    }
}

impl Optimization for DynamicFilter {
    fn pattern(&self) -> Pattern {
        pattern!(Scan [^Union Project Extend]*? Join)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(left_scan) = &steps[0] else {
            return None;
        };
        let WorkflowStep::Join(join, workflow) = &steps[steps.len() - 1] else {
            return None;
        };
        let WorkflowStep::Scan(right_scan) = &workflow.steps[0] else {
            return None;
        };

        let (left_join_field, right_join_field) = &join.on;

        let left_steps_after_scan = if steps.len() > 2 {
            &steps[1..steps.len() - 1]
        } else {
            &[]
        };
        let right_steps_after_scan = if workflow.steps.len() > 1 {
            &workflow.steps[1..workflow.steps.len()]
        } else {
            &[]
        };

        let left_dcount = calculate_max_distinct_count(
            left_join_field.to_string(),
            left_scan,
            left_steps_after_scan,
        )
        .unwrap_or(self.max_distinct_values);
        let right_dcount = calculate_max_distinct_count(
            right_join_field.to_string(),
            right_scan,
            right_steps_after_scan,
        )
        .unwrap_or(self.max_distinct_values);

        if left_dcount >= self.max_distinct_values && right_dcount >= self.max_distinct_values {
            return None;
        }

        let mut left_scan = left_scan.clone();
        let mut workflow = workflow.clone();
        let WorkflowStep::Scan(right_scan) = &mut workflow.steps[0] else {
            return None;
        };

        let watch = Watch::default();
        right_scan.dynamic_filter_tx = Some(watch.clone());

        if left_dcount < self.max_distinct_values {
            if left_dcount > right_dcount {
                left_scan.dynamic_filter_rx = Some(watch);
            } else {
                right_scan.dynamic_filter_rx = Some(watch);
            }
        } else if right_dcount < self.max_distinct_values {
            left_scan.dynamic_filter_rx = Some(watch);
        } else {
            panic!("just checked that the two distinct counts are more than max");
        }

        let mut steps = steps.to_vec();
        *steps.first_mut().unwrap() = WorkflowStep::Scan(left_scan);
        *steps.last_mut().unwrap() = WorkflowStep::Join(join.clone(), workflow);

        Some(steps)
    }
}

fn calculate_max_distinct_count(
    join_field: String,
    scan: &Scan,
    steps_after_scan: &[WorkflowStep],
) -> Option<u32> {
    let mut dcount: Option<u32> = None;
    let mut prev_dcount: Option<u32> = None;
    let mut fields = vec![join_field];

    for step in steps_after_scan.iter().rev() {
        match step {
            WorkflowStep::Count => dcount = Some(1),

            WorkflowStep::Limit(limit) | WorkflowStep::TopN(.., limit) => {
                dcount = Some(dcount.map_or(*limit, |d: u32| d.min(*limit)));
            }

            WorkflowStep::Summarize(summarize) => {
                if prev_dcount.is_some() {
                    // Seems a bit more complicated, don't optimize this for now.
                    return None;
                }
                prev_dcount = dcount.take();
                fields = summarize.by.iter().map(|x| x.to_string()).collect();
            }

            WorkflowStep::Sort(..) | WorkflowStep::Filter(..) => {}

            // Unsupported (need to think about project & extend):
            _ => return None,
        }
    }

    let dcounts: Vec<_> = fields
        .iter()
        .flat_map(|f| scan.get_field_stats(f)?.distinct_count)
        .collect();

    if dcounts.len() == fields.len() {
        dcount = dcounts
            .into_iter()
            .try_fold(1u32, |acc, x| acc.checked_mul(x))
            .map(|summarize_dc| dcount.map_or(summarize_dc, |limit_dc| summarize_dc.min(limit_dc)))
            .or(dcount);
    }

    prev_dcount.map_or(dcount, |pdc| Some(dcount.map_or(pdc, |dc| dc.min(pdc))))
}
