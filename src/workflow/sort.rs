use color_eyre::eyre::{bail, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use tracing::info;
use vrl::{core::Value, value::KeyString};

use crate::{
    log::{Log, LogStream},
    workflow::vrl_utils::partial_cmp_values,
};

#[derive(Debug, Deserialize, Default, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "snake_case")]
pub enum NullsOrder {
    #[default]
    Last,
    First,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Sort {
    pub by: String,

    #[serde(default)]
    pub order: SortOrder,

    #[serde(default)]
    pub nulls: NullsOrder,
}

async fn collect_logs(by: &KeyString, mut input_stream: LogStream) -> Result<Vec<Log>> {
    let mut tracked_type = None;

    let mut logs = Vec::new();
    while let Some(log) = input_stream.next().await {
        if let Some(value) = log.get(by) {
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
            }
        }

        logs.push(log);
    }

    Ok(logs)
}

pub async fn sort_stream(sort: Sort, input_stream: LogStream) -> Result<Vec<Log>> {
    info!(
        "Sorting by '{}' (order: {:?}, nulls: {:?})",
        sort.by, sort.order, sort.nulls
    );

    let by: KeyString = sort.by.into();
    let nulls_order = sort.nulls;

    let mut logs = collect_logs(&by, input_stream).await?;

    logs.sort_unstable_by(|a, b| {
        let a_val = a.get(&by).unwrap_or(&Value::Null);
        let b_val = b.get(&by).unwrap_or(&Value::Null);
        let order = match (a_val, b_val, nulls_order.clone()) {
            (Value::Null, _, NullsOrder::First) => std::cmp::Ordering::Less,
            (_, Value::Null, NullsOrder::First) => std::cmp::Ordering::Greater,
            (Value::Null, _, NullsOrder::Last) => std::cmp::Ordering::Greater,
            (_, Value::Null, NullsOrder::Last) => std::cmp::Ordering::Less,
            _ => partial_cmp_values(a_val, b_val).expect("Types should be comparable"),
        };

        if sort.order == SortOrder::Asc {
            order
        } else {
            order.reverse()
        }
    });

    Ok(logs)
}
