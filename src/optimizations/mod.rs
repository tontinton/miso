use push_filter_into_scan::PushFilterIntoScan;
use push_limit_into_scan::PushLimitIntoScan;
use smallvec::SmallVec;

use crate::workflow::{WorkflowStep, WorkflowStepKind};

mod push_filter_into_scan;
mod push_limit_into_scan;

pub type Pattern = SmallVec<[WorkflowStepKind; 4]>;

#[macro_export]
macro_rules! pattern {
    ($($step:ident) -> +) => {
        smallvec::smallvec![$($crate::workflow::WorkflowStepKind::$step),+]
    };
}

pub trait Optimization: Send + Sync {
    fn pattern(&self) -> Pattern;
    fn apply(&self, steps: &[WorkflowStep]) -> Option<Vec<WorkflowStep>>;
}

pub struct Optimizer {
    optimizations: Vec<Box<dyn Optimization>>,
}

fn to_kind(steps: &[WorkflowStep]) -> Vec<WorkflowStepKind> {
    steps.iter().map(|x| x.kind()).collect()
}

impl Default for Optimizer {
    fn default() -> Self {
        Self {
            optimizations: vec![Box::new(PushFilterIntoScan), Box::new(PushLimitIntoScan)],
        }
    }
}

impl Optimizer {
    pub fn optimize(&self, mut steps: Vec<WorkflowStep>) -> Vec<WorkflowStep> {
        let mut kinded_steps = to_kind(&steps);
        let mut optimized_in_loop = true;

        while optimized_in_loop {
            optimized_in_loop = false;

            for optimization in &self.optimizations {
                let pattern = optimization.pattern();
                let Some(matched_index) = kinded_steps
                    .windows(pattern.len())
                    .position(|window| window == pattern.as_slice())
                else {
                    continue;
                };

                let range = matched_index..matched_index + pattern.len();
                let Some(new_steps) = optimization.apply(&steps[range.clone()]) else {
                    continue;
                };

                steps.splice(range, new_steps);
                kinded_steps = to_kind(&steps);
                optimized_in_loop = true;
            }
        }

        steps
    }
}