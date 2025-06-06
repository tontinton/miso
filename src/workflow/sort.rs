use std::{cmp::Ordering, fmt};

use color_eyre::eyre::{bail, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

use crate::log::{Log, LogStream};

use super::serde_json_utils::partial_cmp_values;

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum NullsOrder {
    #[default]
    Last,
    First,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Sort {
    pub by: String,

    #[serde(default)]
    pub order: SortOrder,

    #[serde(default)]
    pub nulls: NullsOrder,
}

#[derive(Debug)]
pub struct SortConfig {
    by: Vec<String>,
    sort_orders: Vec<SortOrder>,
    nulls_orders: Vec<NullsOrder>,
}

impl SortConfig {
    pub fn new(sorts: Vec<Sort>) -> Self {
        let mut by = Vec::with_capacity(sorts.len());
        let mut sort_orders = Vec::with_capacity(sorts.len());
        let mut nulls_orders = Vec::with_capacity(sorts.len());

        for sort in sorts {
            by.push(sort.by);
            sort_orders.push(sort.order);
            nulls_orders.push(sort.nulls);
        }

        Self {
            by,
            sort_orders,
            nulls_orders,
        }
    }
}

impl fmt::Display for SortOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SortOrder::Asc => write!(f, "asc"),
            SortOrder::Desc => write!(f, "desc"),
        }
    }
}

impl fmt::Display for NullsOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NullsOrder::Last => write!(f, "last"),
            NullsOrder::First => write!(f, "first"),
        }
    }
}

impl fmt::Display for Sort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (order: {}, nulls: {})",
            self.by, self.order, self.nulls
        )
    }
}

pub fn cmp_logs(a: &Log, b: &Log, config: &SortConfig) -> Option<Ordering> {
    for ((key, sort_order), nulls_order) in config
        .by
        .iter()
        .zip(&config.sort_orders)
        .zip(&config.nulls_orders)
    {
        let a_val = a.get(key).unwrap_or(&Value::Null);
        let b_val = b.get(key).unwrap_or(&Value::Null);
        let mut any_null = true;
        let ordering = match (a_val, b_val, nulls_order) {
            (Value::Null, Value::Null, _) => Ordering::Equal,
            (Value::Null, _, NullsOrder::First) => Ordering::Less,
            (_, Value::Null, NullsOrder::First) => Ordering::Greater,
            (Value::Null, _, NullsOrder::Last) => Ordering::Greater,
            (_, Value::Null, NullsOrder::Last) => Ordering::Less,
            _ => {
                any_null = false;
                partial_cmp_values(a_val, b_val)?
            }
        };

        if ordering == Ordering::Equal {
            continue;
        }

        if any_null {
            return Some(ordering);
        }

        return Some(if *sort_order == SortOrder::Asc {
            ordering
        } else {
            ordering.reverse()
        });
    }

    Some(Ordering::Equal)
}

async fn collect_logs(by: &[String], mut input_stream: LogStream) -> Result<Vec<Log>> {
    let mut tracked_types = vec![None; by.len()];

    let mut logs = Vec::new();
    while let Some(log) = input_stream.next().await {
        for (tracked_type, key) in tracked_types.iter_mut().zip(by) {
            if let Some(value) = log.get(key) {
                if value != &Value::Null {
                    let value_type = std::mem::discriminant(value);
                    if let Some(t) = tracked_type {
                        if *t != value_type {
                            bail!(
                                "cannot sort over differing types (key '{}'): {:?} != {:?}",
                                key,
                                *t,
                                value_type
                            );
                        }
                    } else {
                        *tracked_type = Some(value_type);
                    }
                }
            }
        }

        logs.push(log);
    }

    Ok(logs)
}

pub async fn sort_stream(sorts: Vec<Sort>, input_stream: LogStream) -> Result<Vec<Log>> {
    info!(
        "Sorting by {}",
        sorts
            .iter()
            .map(|sort| sort.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let config = SortConfig::new(sorts);
    let mut logs = collect_logs(&config.by, input_stream).await?;

    logs.sort_unstable_by(|a, b| {
        cmp_logs(a, b, &config).expect("types should have been validated")
    });

    Ok(logs)
}
