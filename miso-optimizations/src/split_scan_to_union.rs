//! Splits a single scan into parallel scans for connectors that support sharding.
//!
//! Some connectors (like partitioned data sources) can provide multiple "splits" -
//! independent chunks that can be read in parallel. This optimization converts
//! a single scan into a union of scans, one per split. The union merges results
//! as each split finishes, enabling parallel reads.
//!
//! Example (connector with 3 splits):
//!   scan
//! becomes:
//!   scan[split=0] | union (scan[split=1]) | union (scan[split=2])
//!
//! If the connector returns no splits, we leave the scan unchanged.

use miso_workflow::{Workflow, WorkflowStep};

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct SplitScanIntoUnion;

impl Optimization for SplitScanIntoUnion {
    fn pattern(&self) -> Pattern {
        pattern!(Scan)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Scan(scan) = steps[0].clone() else {
            return OptimizationResult::Unchanged;
        };

        let splits = scan.connector.get_splits();
        if splits.is_empty() {
            return OptimizationResult::Changed(vec![WorkflowStep::Scan(scan)]);
        }

        let steps = splits
            .into_iter()
            .enumerate()
            .map(|(i, split)| {
                let mut scan_with_split = scan.clone();
                scan_with_split.split = Some(split.into());
                let step = WorkflowStep::Scan(scan_with_split);
                if i == 0 {
                    step
                } else {
                    WorkflowStep::Union(Workflow::new(vec![step]))
                }
            })
            .collect();

        OptimizationResult::Changed(steps)
    }
}
