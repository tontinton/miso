use collection_macros::btreemap;

use crate::workflow::{
    filter::FilterAst,
    project::{ProjectAst, ProjectField},
    sort::{NullsOrder, Sort, SortOrder},
    summarize::{Aggregation, GroupAst, Summarize},
    Workflow, WorkflowStep as S,
};

use super::Optimizer;

fn check(optimizer: Optimizer, input: Vec<S>, expected: Vec<S>) {
    let result = optimizer.optimize(input);
    assert_eq!(result, expected);
}

fn check_default(input: Vec<S>, expected: Vec<S>) {
    check(Optimizer::default(), input, expected);
}

#[test]
fn smoke() {
    check_default(vec![], vec![]);
}

#[test]
fn sort_limit_into_topn() {
    let sort1 = vec![];
    let sort2 = vec![Sort {
        by: "a".to_string(),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }];
    check_default(
        vec![
            S::Sort(sort1.clone()),
            S::Limit(10),
            S::Sort(sort2.clone()),
            S::Limit(20),
        ],
        vec![S::TopN(sort1, 10), S::TopN(sort2, 20)],
    );
}

#[test]
fn limit_limit_into_limit() {
    check_default(
        vec![S::Limit(25), S::Limit(10), S::Limit(20), S::Limit(15)],
        vec![S::Limit(10)],
    );
}

#[test]
fn topn_limit_into_topn() {
    check_default(
        vec![
            S::Limit(1),
            S::TopN(vec![], 25),
            S::Limit(10),
            S::TopN(vec![], 15),
            S::Limit(35),
            S::MuxTopN(vec![], 7),
            S::Limit(4),
        ],
        vec![
            S::Limit(1),
            S::TopN(vec![], 10),
            S::TopN(vec![], 15),
            S::MuxTopN(vec![], 4),
        ],
    );
}

#[test]
fn filter_before_sort() {
    let sort1 = S::Sort(vec![]);
    let sort2 = S::Sort(vec![Sort {
        by: "a".to_string(),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }]);
    let filter1 = S::Filter(FilterAst::Eq(
        Box::new(FilterAst::Id("a".to_string())),
        Box::new(FilterAst::Lit(serde_json::Value::String("b".to_string()))),
    ));

    check_default(
        vec![sort1.clone(), sort2.clone(), filter1.clone()],
        vec![filter1, sort1, sort2],
    );
}

#[test]
fn merge_filters() {
    let ast1 = FilterAst::Eq(
        Box::new(FilterAst::Id("a".to_string())),
        Box::new(FilterAst::Lit(serde_json::Value::String("b".to_string()))),
    );
    let ast2 = FilterAst::Ne(
        Box::new(FilterAst::Id("c".to_string())),
        Box::new(FilterAst::Lit(serde_json::Value::String("d".to_string()))),
    );

    check_default(
        vec![S::Filter(ast1.clone()), S::Filter(ast2.clone())],
        vec![S::Filter(FilterAst::And(vec![ast1, ast2]))],
    );
}

#[test]
fn remove_sorts_before_count() {
    check_default(
        vec![S::Sort(vec![]), S::Sort(vec![]), S::Sort(vec![]), S::Count],
        vec![S::Count],
    );
}

#[test]
fn dont_remove_sorts_before_limit_before_count() {
    check_default(
        vec![
            S::Sort(vec![]),
            S::Project(vec![]),
            S::Limit(10),
            S::Sort(vec![]),
            S::Count,
        ],
        vec![S::Sort(vec![]), S::Project(vec![]), S::Limit(10), S::Count],
    );
}

#[test]
fn filter_into_union() {
    let filter = S::Filter(FilterAst::Eq(
        Box::new(FilterAst::Id("a".to_string())),
        Box::new(FilterAst::Lit(serde_json::Value::String("b".to_string()))),
    ));

    check_default(
        vec![S::Union(Workflow::new(vec![])), filter.clone()],
        vec![filter.clone(), S::Union(Workflow::new(vec![filter]))],
    );
}

#[test]
fn project_into_union() {
    let project = S::Project(vec![ProjectField {
        from: ProjectAst::Id("a".to_string()),
        to: "b".to_string(),
    }]);

    check_default(
        vec![S::Union(Workflow::new(vec![])), project.clone()],
        vec![project.clone(), S::Union(Workflow::new(vec![project]))],
    );
}

#[test]
fn extend_into_union() {
    let extend = S::Extend(vec![ProjectField {
        from: ProjectAst::Id("a".to_string()),
        to: "b".to_string(),
    }]);

    check_default(
        vec![S::Union(Workflow::new(vec![])), extend.clone()],
        vec![extend.clone(), S::Union(Workflow::new(vec![extend]))],
    );
}

#[test]
fn limit_into_union() {
    let limit = S::Limit(1);
    check_default(
        vec![S::Union(Workflow::new(vec![])), limit.clone()],
        vec![
            limit.clone(),
            S::Union(Workflow::new(vec![limit])),
            S::MuxLimit(1),
        ],
    );
}

#[test]
fn topn_into_union() {
    let sorts = vec![Sort {
        by: "a".to_string(),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }];
    let topn = S::TopN(sorts.clone(), 1);
    check_default(
        vec![S::Union(Workflow::new(vec![])), topn.clone()],
        vec![
            topn.clone(),
            S::Union(Workflow::new(vec![topn])),
            S::MuxTopN(sorts, 1),
        ],
    );
}

#[test]
fn summarize_into_union() {
    let original = S::Summarize(Summarize {
        aggs: btreemap! {
            "c".to_string() => Aggregation::Count,
            "s".to_string() => Aggregation::Sum("y".to_string()),
        },
        by: vec![GroupAst::Id("x".to_string())],
    });
    let post = S::MuxSummarize(Summarize {
        aggs: btreemap! {
            "c".to_string() => Aggregation::Sum("c".to_string()),
            "s".to_string() => Aggregation::Sum("s".to_string()),
        },
        by: vec![GroupAst::Id("x".to_string())],
    });

    check_default(
        vec![S::Union(Workflow::new(vec![])), original.clone()],
        vec![
            original.clone(),
            S::Union(Workflow::new(vec![original.clone()])),
            post,
        ],
    );
}

#[test]
fn reorder_filter_before_sort() {
    let filter = S::Filter(FilterAst::Eq(
        Box::new(FilterAst::Id("a".to_string())),
        Box::new(FilterAst::Lit(serde_json::Value::String("b".to_string()))),
    ));
    check_default(
        vec![S::Sort(vec![]), S::Sort(vec![]), filter.clone()],
        vec![filter, S::Sort(vec![]), S::Sort(vec![])],
    );
}

#[test]
fn reorder_filter_before_mux() {
    let filter = S::Filter(FilterAst::Eq(
        Box::new(FilterAst::Id("a".to_string())),
        Box::new(FilterAst::Lit(serde_json::Value::String("b".to_string()))),
    ));
    check_default(vec![S::MuxCount, filter.clone()], vec![filter, S::MuxCount]);
}
