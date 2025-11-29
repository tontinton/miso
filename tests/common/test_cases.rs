#[derive(Clone, Copy)]
pub struct TestCase {
    pub query: &'static str,
    pub expected: &'static str,
    pub count: usize,
    pub name: &'static str,
}

pub const BASE_PREDICATE_PUSHDOWN_TESTS: &[TestCase] = &[
    // Basic filters
    TestCase {
        query: r#"test.stack | where acceptedAnswerId == 12446"#,
        expected: r#"test.stack"#,
        count: 1,
        name: "filter_eq",
    },
    TestCase {
        query: r#"test.stack | where body has_cs "isn't""#,
        expected: r#"test.stack"#,
        count: 1,
        name: "filter_has_cs",
    },
    TestCase {
        query: r#"test.stack | where acceptedAnswerId in (12446, 31)"#,
        expected: r#"test.stack"#,
        count: 2,
        name: "filter_in",
    },
    TestCase {
        query: r#"test.stack | where questionId >= 4 and questionId < 15"#,
        expected: r#"test.stack"#,
        count: 8,
        name: "filter_range",
    },
    TestCase {
        query: r#"test.stack | where questionId == 4 or questionId == 6 or questionId == 11"#,
        expected: r#"test.stack"#,
        count: 5,
        name: "filter_multiple_or",
    },
    TestCase {
        query: r#"test.stack | where exists(answerId)"#,
        expected: r#"test.stack"#,
        count: 2,
        name: "filter_exists",
    },
    // Projections
    TestCase {
        query: r#"test.stack | project acceptedAnswerId"#,
        expected: r#"test.stack"#,
        count: 10,
        name: "project",
    },
    // Basic aggregations and counts
    TestCase {
        query: r#"test.stack | count"#,
        expected: r#"test.stack"#,
        count: 1,
        name: "count",
    },
    TestCase {
        query: r#"test.stack | distinct user"#,
        expected: r#"test.stack"#,
        count: 5,
        name: "distinct",
    },
    // Complex aggregations - comprehensive test with all agg types
    TestCase {
        query: r#"
    test.stack
    | summarize minQuestionId=min(questionId),
                maxQuestionId=max(questionId),
                avgQuestionId=avg(questionId),
                dcountUser=dcount(user),
                cifQuestionId=countif(exists(questionId)),
                sumQuestionId=sum(questionId),
                minTimestamp=min(@time),
                maxTimestamp=max(@time),
                c=count()
      by bin(answerId, 5)
    "#,
        expected: r#"test.stack"#,
        count: 2,
        name: "summarize_all_agg_types_with_binning",
    },
    TestCase {
        query: r#"test.stack | summarize c=count() by bin(questionId, 2), user"#,
        expected: r#"test.stack"#,
        count: 8,
        name: "summarize_multiple_groupby",
    },
    // Top-N and sorting
    TestCase {
        query: r#"test.stack | summarize minQuestionId=min(questionId) by user | top 3 by minQuestionId"#,
        expected: r#"test.stack | top 3 by minQuestionId"#,
        count: 3,
        name: "summarize_then_topn",
    },
    TestCase {
        query: r#"test.stack | top 5 by questionId | summarize minQuestionId=min(questionId) by user"#,
        expected: r#"test.stack | summarize minQuestionId=min(questionId) by user"#,
        count: 3,
        name: "topn_then_summarize",
    },
    TestCase {
        query: r#"test.stack | sort by @time desc | take 3"#,
        expected: r#"test.stack"#,
        count: 3,
        name: "topn_desc",
    },
    TestCase {
        query: r#"test.stack | sort by @time asc | take 3"#,
        expected: r#"test.stack"#,
        count: 3,
        name: "topn_asc",
    },
    // Union operations
    TestCase {
        query: r#"test.stack | union (test.stack_mirror)"#,
        expected: r#"test.stack"#,
        count: 20,
        name: "union_same_schema",
    },
    TestCase {
        query: r#"test.stack | union (test.hdfs)"#,
        expected: r#"test.stack | union (test.hdfs)"#,
        count: 20,
        name: "union_different_timestamp_field",
    },
    TestCase {
        query: r#"
    test.stack
    | union (test.stack_mirror)
    | where acceptedAnswerId < 100
    | top 1 by acceptedAnswerId
    "#,
        expected: r#"test.stack"#,
        count: 1,
        name: "union_with_filter_and_topn",
    },
];
