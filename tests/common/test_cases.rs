#[derive(Clone, Copy, PartialEq)]
pub enum TestConnector {
    Elastic,
    Quickwit,
    Splunk,
}

#[derive(Clone)]
pub struct TestCase {
    pub query: &'static str,
    pub expected: Expected,
    pub count: usize,
    pub name: &'static str,
}

#[derive(Clone)]
pub enum Expected {
    Default(&'static str),
    Override {
        default: &'static str,
        overrides: &'static [(&'static [TestConnector], &'static str)],
    },
}

impl Expected {
    pub fn for_connector(self, c: TestConnector) -> &'static str {
        match self {
            Expected::Default(v) => v,
            Expected::Override { default, overrides } => overrides
                .iter()
                .find(|(cs, _)| cs.contains(&c))
                .map(|(_, v)| *v)
                .unwrap_or(default),
        }
    }
}

macro_rules! expected {
    ($default:expr $(,)?) => {
        Expected::Default($default)
    };

    (
        $default:expr,
        $(
            $($conn:ident)|+ => $value:expr
        ),+ $(,)?
    ) => {
        Expected::Override {
            default: $default,
            overrides: &[
                $(
                    (
                        &[
                            $( TestConnector::$conn ),+
                        ],
                        $value
                    )
                ),+
            ],
        }
    };
}

pub const BASE_PREDICATE_PUSHDOWN_TESTS: &[TestCase] = &[
    // Basic filters
    TestCase {
        query: r#"test.stack | where acceptedAnswerId == 12446"#,
        expected: expected!("test.stack"),
        count: 1,
        name: "filter_eq",
    },
    TestCase {
        query: r#"test.stack | where questionId != 4"#,
        expected: expected!("test.stack"),
        count: 8,
        name: "filter_ne",
    },
    TestCase {
        query: r#"test.stack | where acceptedAnswerId in (12446, 31)"#,
        expected: expected!("test.stack"),
        count: 2,
        name: "filter_in",
    },
    TestCase {
        query: r#"test.stack | where questionId >= 4 and questionId < 15"#,
        expected: expected!("test.stack"),
        count: 8,
        name: "filter_range",
    },
    TestCase {
        query: r#"test.stack | where questionId == 4 or questionId == 6 or questionId == 11"#,
        expected: expected!("test.stack"),
        count: 5,
        name: "filter_multiple_or",
    },
    TestCase {
        query: r#"test.stack | where exists(answerId)"#,
        expected: expected!("test.stack"),
        count: 2,
        name: "filter_exists",
    },
    TestCase {
        query: r#"test.stack | where not(exists(answerId))"#,
        expected: expected!("test.stack"),
        count: 8,
        name: "filter_not_exists",
    },
    TestCase {
        query: r#"test.stack | where not(questionId == 4)"#,
        expected: expected!("test.stack"),
        count: 8,
        name: "filter_not",
    },
    TestCase {
        query: r#"test.stack | where (questionId > 10 and questionId < 15) or questionId == 4"#,
        expected: expected!("test.stack"),
        count: 6,
        name: "filter_nested_and_or",
    },
    // True negative test
    TestCase {
        query: r#"test.stack | where questionId == 99999"#,
        expected: expected!("test.stack"),
        count: 0,
        name: "filter_no_match",
    },
    // String predicates - has/has_cs
    TestCase {
        query: r#"test.stack | where body has_cs "This""#,
        expected: expected!(
            "test.stack",
            Elastic => r#"test.stack | where body has_cs "This""#,
        ),
        count: 1,
        name: "filter_has_cs_uppercase",
    },
    TestCase {
        query: r#"test.stack | where body has_cs "this""#,
        expected: expected!(
            "test.stack",
            Elastic => r#"test.stack | where body has_cs "this""#,
        ),
        count: 4,
        name: "filter_has_cs_lowercase",
    },
    TestCase {
        query: r#"test.stack | where body has "This""#,
        expected: expected!(
            "test.stack",
            Quickwit => r#"test.stack | where body has "This""#,
        ),
        count: 4,
        name: "filter_has_uppercase",
    },
    TestCase {
        query: r#"test.stack | where body has "this""#,
        expected: expected!(
            "test.stack",
            Quickwit => r#"test.stack | where body has "this""#,
        ),
        count: 4,
        name: "filter_has_lowercase",
    },
    TestCase {
        query: r#"test.stack | where body has "code""#,
        expected: expected!(
            "test.stack",
            Quickwit => r#"test.stack | where body has "code""#,
        ),
        count: 1,
        name: "filter_has_word_boundary",
    },
    // String predicates - startswith and contains
    TestCase {
        query: r#"test.stack | where title startswith "Calculate""#,
        expected: expected!("test.stack"),
        count: 2,
        name: "filter_startswith",
    },
    TestCase {
        query: r#"test.stack | where body contains "DateTime""#,
        expected: expected!(r#"test.stack | where body contains "DateTime""#),
        count: 3,
        name: "filter_contains",
    },
    // Chained filters
    TestCase {
        query: r#"test.stack | where questionId > 4 | where exists(acceptedAnswerId)"#,
        expected: expected!("test.stack"),
        count: 5,
        name: "filter_chained_with_exists",
    },
    // Projections
    TestCase {
        query: r#"test.stack | project acceptedAnswerId"#,
        expected: expected!(
            "test.stack",
            Splunk => "test.stack | project acceptedAnswerId",
        ),
        count: 10,
        name: "project",
    },
    // Basic aggregations and counts
    TestCase {
        query: r#"test.stack | count"#,
        expected: expected!("test.stack"),
        count: 1,
        name: "count",
    },
    TestCase {
        query: r#"test.stack | distinct user"#,
        expected: expected!(
            "test.stack",
            Splunk => "test.stack | distinct user",
        ),
        count: 5,
        name: "distinct",
    },
    // Complex aggregations
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
        expected: expected!(
            "test.stack",
            // Splunk doesn't support binning in stats pushdown
            Splunk => r#"
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
        ),
        count: 2,
        name: "summarize_all_agg_types_with_binning",
    },
    TestCase {
        query: r#"test.stack | summarize c=count() by bin(questionId, 2), user"#,
        expected: expected!(
            "test.stack",
            // Splunk doesn't support binning in stats pushdown
            Splunk => "test.stack | summarize c=count() by bin(questionId, 2), user",
        ),
        count: 8,
        name: "summarize_multiple_groupby",
    },
    TestCase {
        query: r#"test.stack | summarize c=count() by u=user"#,
        expected: expected!("test.stack"),
        count: 5,
        name: "summarize_with_aliased_by_field",
    },
    // Top-N and sorting
    TestCase {
        query: r#"test.stack | summarize minQuestionId=min(questionId) by user | top 3 by minQuestionId"#,
        expected: expected!(
            "test.stack",
            Elastic | Quickwit => "test.stack | top 3 by minQuestionId",
        ),
        count: 3,
        name: "summarize_then_topn",
    },
    TestCase {
        query: r#"test.stack | top 5 by questionId | summarize minQuestionId=min(questionId) by user"#,
        expected: expected!(
            "test.stack",
            Elastic | Quickwit => "test.stack | summarize minQuestionId=min(questionId) by user",
        ),
        count: 3,
        name: "topn_then_summarize",
    },
    TestCase {
        query: r#"test.stack | summarize c=count() by user | top 3 by c"#,
        expected: expected!(
            "test.stack",
            Elastic | Quickwit => "test.stack | top 3 by c",
        ),
        count: 3,
        name: "summarize_count_then_topn",
    },
    TestCase {
        query: r#"test.stack | sort by @time desc | take 3"#,
        expected: expected!("test.stack"),
        count: 3,
        name: "topn_desc",
    },
    TestCase {
        query: r#"test.stack | sort by @time asc | take 3"#,
        expected: expected!("test.stack"),
        count: 3,
        name: "topn_asc",
    },
    TestCase {
        query: r#"test.stack | top 5 by questionId | top 3 by questionId"#,
        expected: expected!("test.stack"),
        count: 3,
        name: "topn_after_topn",
    },
    // Union operations
    TestCase {
        query: r#"test.stack | union (test.stack_mirror)"#,
        expected: expected!("test.stack"),
        count: 20,
        name: "union_same_schema",
    },
    TestCase {
        query: r#"test.stack | union (test.hdfs)"#,
        expected: expected!(
            "test.stack",
            Elastic | Quickwit => "test.stack | union (test.hdfs)",
        ),
        count: 20,
        name: "union_different_timestamp_field",
    },
    TestCase {
        query: r#"
    test.stack
    | union (test.stack_mirror)
    | where acceptedAnswerId == 31
    | top 2 by acceptedAnswerId
    "#,
        expected: expected!("test.stack"),
        count: 2,
        name: "union_with_filter_and_topn",
    },
];
