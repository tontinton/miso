use std::collections::BTreeMap;

use miso_workflow_types::query::QueryStep;

pub mod config;
pub mod http_server;
pub mod query_status;
pub mod query_to_workflow;

pub(crate) const VIEWS_CONNECTOR_NAME: &str = "views";
pub(crate) type ViewsMap = BTreeMap<String, Vec<QueryStep>>;
