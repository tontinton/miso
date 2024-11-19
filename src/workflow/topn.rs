use std::{cmp::Ordering, collections::BinaryHeap};

use futures_util::StreamExt;

use color_eyre::{eyre::bail, Result};
use tracing::info;
use vrl::{core::Value, value::KeyString};

use crate::{
    log::{Log, LogStream},
    workflow::sort::SortOrder,
};

use super::{
    sort::{NullsOrder, Sort},
    vrl_utils::partial_cmp_values,
};

#[derive(Debug)]
struct Sortable {
    log: Log,
    value: Value,
    smallest: bool,
    nulls: NullsOrder,
}

impl Ord for Sortable {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.value, &other.value, &self.nulls) {
            (Value::Null, Value::Null, _) => Ordering::Equal,
            (Value::Null, _, NullsOrder::First) => Ordering::Less,
            (_, Value::Null, NullsOrder::First) => Ordering::Greater,
            (Value::Null, _, NullsOrder::Last) => Ordering::Greater,
            (_, Value::Null, NullsOrder::Last) => Ordering::Less,
            _ => {
                let ord = partial_cmp_values(&self.value, &other.value)
                    .expect("types should have been validated");
                if !self.smallest {
                    ord.reverse()
                } else {
                    ord
                }
            }
        }
    }
}

impl PartialOrd for Sortable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Sortable {}

impl PartialEq for Sortable {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

pub async fn topn_stream(sort: Sort, limit: u64, mut input_stream: LogStream) -> Result<Vec<Log>> {
    info!(
        "Collecting top {} sorted by '{}' (order: {:?}, nulls: {:?})",
        limit, sort.by, sort.order, sort.nulls,
    );

    let by: KeyString = sort.by.into();
    let nulls = sort.nulls;
    let smallest = sort.order == SortOrder::Asc;

    let mut tracked_type = None;
    let mut heap = BinaryHeap::new();
    let mut number_of_nulls = 0;

    while let Some(log) = input_stream.next().await {
        let Some(value) = log.get(&by) else {
            continue;
        };

        if value != &Value::Null {
            let value_type = std::mem::discriminant(value);
            if let Some(t) = tracked_type {
                if t != value_type {
                    bail!(
                        "Cannot sort over differing types: {:?} != {:?}",
                        t,
                        value_type
                    );
                }
            } else {
                tracked_type = Some(value_type);
            }
        } else {
            number_of_nulls += 1;
        }

        let sortable = Sortable {
            value: value.clone(),
            log,
            smallest,
            nulls: nulls.clone(),
        };

        if heap.len() < limit as usize {
            heap.push(sortable);
        } else {
            let bottom_of_top = heap.peek().unwrap();
            if sortable.cmp(bottom_of_top) == Ordering::Less {
                heap.pop();
                heap.push(sortable);
            }
        }

        if nulls == NullsOrder::First && number_of_nulls >= limit {
            // Optimization: when doing nulls first, once we get N amount of nulls, no reason to
            // continue.
            break;
        }
    }

    let mut logs = Vec::with_capacity(heap.len());
    while let Some(sortable) = heap.pop() {
        logs.push(sortable.log);
    }
    Ok(logs)
}
