use async_stream::try_stream;
use color_eyre::eyre::{bail, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use tracing::info;
use vrl::{core::Value, value::KeyString};

use crate::{
    log::{Log, LogStream, LogTryStream},
    workflow::vrl_utils::partial_cmp_values,
};

#[derive(Debug, Deserialize, Default, PartialEq)]
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

#[derive(Debug, Deserialize)]
pub struct Sort {
    by: String,

    #[serde(default)]
    order: SortOrder,

    #[serde(default)]
    nulls: NullsOrder,
}

async fn collect_logs(by: &KeyString, mut input_stream: LogStream) -> Result<Vec<Log>> {
    let mut tracked_type = None;

    let mut logs = Vec::new();
    while let Some(log) = input_stream.next().await {
        if let Some(value) = log.get(by) {
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

        logs.push(log);
    }

    Ok(logs)
}

pub fn sort_stream(sort: Sort, input_stream: LogStream) -> Result<LogTryStream> {
    info!(
        "Sorting by '{}' (order: {:?}, nulls: {:?})",
        sort.by, sort.order, sort.nulls
    );

    Ok(Box::pin(try_stream! {
        let by: KeyString = sort.by.into();
        let nulls_order = sort.nulls;

        let mut logs = collect_logs(&by, input_stream).await?;

        logs.sort_unstable_by(|a, b| {
            let a_val = a.get(&by).unwrap_or(&Value::Null);
            let b_val = b.get(&by).unwrap_or(&Value::Null);
            let order = match (a_val, b_val, nulls_order.clone()) {
                (Value::Null, _, NullsOrder::First) => std::cmp::Ordering::Less,
                (_, Value::Null, NullsOrder::First) => {
                    std::cmp::Ordering::Greater
                }
                (Value::Null, _, NullsOrder::Last) => {
                    std::cmp::Ordering::Greater
                }
                (_, Value::Null, NullsOrder::Last) => std::cmp::Ordering::Less,
                _ => partial_cmp_values(a_val, b_val)
                    .expect("Types should be comparable"),
            };

            if sort.order == SortOrder::Asc {
                order
            } else {
                order.reverse()
            }
        });

        for log in logs {
            yield log;
        }
    }))
}
