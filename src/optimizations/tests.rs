use crate::workflow::{
    filter::FilterAst,
    sort::{NullsOrder, Sort, SortOrder},
    WorkflowStep as S,
};

use super::Optimizer;

async fn check(optimizer: Optimizer, input: Vec<S>, expected: Vec<S>) {
    let result = optimizer.optimize(input).await;
    assert_eq!(result, expected);
}

async fn check_default(input: Vec<S>, expected: Vec<S>) {
    check(Optimizer::default(), input, expected).await;
}

#[tokio::test]
async fn smoke() {
    check_default(vec![], vec![]).await;
}

#[tokio::test]
async fn sort_limit_into_topn() {
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
    )
    .await;
}

#[tokio::test]
async fn limit_limit_into_limit() {
    check_default(
        vec![S::Limit(25), S::Limit(10), S::Limit(20), S::Limit(15)],
        vec![S::Limit(10)],
    )
    .await;
}

#[tokio::test]
async fn topn_limit_into_topn() {
    check_default(
        vec![
            S::Limit(1),
            S::TopN(vec![], 25),
            S::Limit(10),
            S::TopN(vec![], 15),
            S::Limit(35),
        ],
        vec![S::Limit(1), S::TopN(vec![], 10), S::TopN(vec![], 15)],
    )
    .await;
}

#[tokio::test]
async fn filter_before_sort() {
    let sort1 = S::Sort(vec![]);
    let sort2 = S::Sort(vec![Sort {
        by: "a".to_string(),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }]);
    let filter1 = S::Filter(FilterAst::Eq("a".to_string(), "b".to_string()));
    let filter2 = S::Filter(FilterAst::Ne("c".to_string(), "d".to_string()));

    check_default(
        vec![
            sort1.clone(),
            sort2.clone(),
            filter1.clone(),
            filter2.clone(),
        ],
        vec![filter1, filter2, sort1, sort2],
    )
    .await;
}

#[tokio::test]
async fn remove_sorts_before_count() {
    check_default(
        vec![S::Sort(vec![]), S::Sort(vec![]), S::Sort(vec![]), S::Count],
        vec![S::Count],
    )
    .await;
}

#[tokio::test]
async fn dont_remove_sorts_before_limit_before_count() {
    check_default(
        vec![
            S::Sort(vec![]),
            S::Project(vec![]),
            S::Limit(10),
            S::Sort(vec![]),
            S::Count,
        ],
        vec![S::Sort(vec![]), S::Project(vec![]), S::Limit(10), S::Count],
    )
    .await;
}
