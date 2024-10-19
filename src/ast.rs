use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", untagged)]
pub enum FilterItem {
    /// Filter items that contain the input word (2nd arg) in the input field (1st arg).
    Term(/*field=*/ String, /*word=*/ String),
    Filter(FilterTree),
}

#[derive(Debug, Deserialize)]
pub struct FilterTree {
    /// All filters must apply. Similar to ES must.
    pub and: Option<Vec<FilterItem>>,

    /// One of the filters must apply. Similar to ES should.
    pub or: Option<Vec<FilterItem>>,
}

#[derive(Debug, Deserialize)]
pub struct QueryAst {
    /// Similar to ES bool.
    pub filter: Option<FilterTree>,
}
