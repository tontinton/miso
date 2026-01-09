use hashbrown::HashMap;
use miso_common::hashmap;
use miso_workflow::{Workflow, WorkflowStep as S, display::DisplayableWorkflowSteps};
use miso_workflow_types::{
    expr::Expr,
    json,
    project::ProjectField,
    sort::{NullsOrder, Sort, SortOrder},
    summarize::{Aggregation, Summarize},
    value::Value,
};
use test_case::test_case;

use super::Optimizer;
use crate::test_utils::*;

fn check(optimizer: Optimizer, input: Vec<S>, expected: Vec<S>) {
    let result = optimizer.optimize(input.clone());
    assert_eq!(
        result,
        expected,
        "\nInput:\n{}\nOptimized:\n{}\n!=\nExpected:\n{}\n",
        DisplayableWorkflowSteps::new(&input),
        DisplayableWorkflowSteps::new(&result),
        DisplayableWorkflowSteps::new(&expected),
    );
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
    let sort1 = vec![Sort {
        by: field("b"),
        order: SortOrder::Desc,
        nulls: NullsOrder::First,
    }];
    let sort2 = vec![Sort {
        by: field("a"),
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
    let sort1 = S::Sort(vec![Sort {
        by: field("b"),
        order: SortOrder::Desc,
        nulls: NullsOrder::First,
    }]);
    let sort2 = S::Sort(vec![Sort {
        by: field("a"),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }]);
    let filter1 = S::Filter(Expr::Eq(
        Box::new(Expr::Field(field("a"))),
        Box::new(Expr::Literal(Value::String("b".to_string()))),
    ));

    check_default(
        vec![sort1.clone(), sort2.clone(), filter1.clone()],
        vec![filter1, sort1, sort2],
    );
}

#[test]
fn merge_filters() {
    let ast1 = Expr::Eq(
        Box::new(Expr::Field(field("a"))),
        Box::new(Expr::Literal(Value::String("b".to_string()))),
    );
    let ast2 = Expr::Ne(
        Box::new(Expr::Field(field("c"))),
        Box::new(Expr::Literal(Value::String("d".to_string()))),
    );

    check_default(
        vec![S::Filter(ast1.clone()), S::Filter(ast2.clone())],
        vec![S::Filter(Expr::And(Box::new(ast1), Box::new(ast2)))],
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
fn remove_sorts_before_summarize() {
    let summarize = Summarize {
        aggs: hashmap! { field("c") => Aggregation::Count },
        by: vec![Expr::Field(field("x"))],
    };
    check_default(
        vec![
            S::Sort(vec![]),
            S::Sort(vec![]),
            S::Summarize(summarize.clone()),
        ],
        vec![S::Summarize(summarize)],
    );
}

#[test]
fn remove_redundant_steps_before_count() {
    check_default(
        vec![
            S::Project(vec![]),
            S::Extend(vec![]),
            S::Rename(vec![]),
            S::Count,
        ],
        vec![S::Count],
    );
}

#[test]
fn remove_redundant_steps_before_summarize() {
    let summarize = Summarize {
        aggs: hashmap! { field("c") => Aggregation::Count },
        by: vec![Expr::Field(field("x"))],
    };
    check_default(
        vec![
            S::Project(vec![]),
            S::Extend(vec![]),
            S::Rename(vec![]),
            S::Summarize(summarize.clone()),
        ],
        vec![S::Summarize(summarize)],
    );
}

#[test]
fn dont_remove_sorts_before_limit_before_count() {
    let sort = S::Sort(vec![Sort {
        by: field("a"),
        order: SortOrder::Asc,
        nulls: NullsOrder::First,
    }]);

    check_default(
        vec![
            sort.clone(),
            S::Project(vec![]),
            S::Limit(10),
            sort.clone(),
            S::Count,
        ],
        vec![sort, S::Project(vec![]), S::Limit(10), S::Count],
    );
}

#[test]
fn filter_into_union() {
    let filter = S::Filter(Expr::Eq(
        Box::new(Expr::Field(field("a"))),
        Box::new(Expr::Literal(Value::String("b".to_string()))),
    ));

    check_default(
        vec![S::Union(Workflow::new(vec![])), filter.clone()],
        vec![filter.clone(), S::Union(Workflow::new(vec![filter]))],
    );
}

#[test]
fn project_into_union() {
    let project = S::Project(vec![ProjectField {
        from: Expr::Field(field("a")),
        to: field("b"),
    }]);

    check_default(
        vec![S::Union(Workflow::new(vec![])), project.clone()],
        vec![project.clone(), S::Union(Workflow::new(vec![project]))],
    );
}

#[test]
fn extend_into_union() {
    let extend = S::Extend(vec![ProjectField {
        from: Expr::Field(field("a")),
        to: field("b"),
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
        by: field("a"),
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
        aggs: hashmap! {
            field("c") => Aggregation::Count,
            field("s") => Aggregation::Sum(field("y")),
            field("d") => Aggregation::DCount(field("x")),
            field("dd") => Aggregation::DCount(field("z")),
        },
        by: vec![Expr::Field(field("x"))],
    });

    let partial = S::Summarize(Summarize {
        aggs: hashmap! {
            field("c") => Aggregation::Count,
            field("s") => Aggregation::Sum(field("y")),
        },
        by: vec![Expr::Field(field("x")), Expr::Field(field("z"))],
    });

    let post = S::MuxSummarize(Summarize {
        aggs: hashmap! {
            field("c") => Aggregation::Sum(field("c")),
            field("s") => Aggregation::Sum(field("s")),
            field("d") => Aggregation::DCount(field("x")),
            field("dd") => Aggregation::DCount(field("z")),
        },
        by: vec![Expr::Field(field("x"))],
    });

    check_default(
        vec![S::Union(Workflow::new(vec![])), original.clone()],
        vec![
            partial.clone(),
            S::Union(Workflow::new(vec![partial])),
            post,
        ],
    );
}

#[test]
fn reorder_filter_before_sort() {
    let sort = S::Sort(vec![Sort {
        by: field("a"),
        order: SortOrder::Asc,
        nulls: NullsOrder::First,
    }]);
    let filter = S::Filter(Expr::Eq(
        Box::new(Expr::Field(field("a"))),
        Box::new(Expr::Literal(Value::String("b".to_string()))),
    ));
    check_default(
        vec![sort.clone(), sort.clone(), filter.clone()],
        vec![filter, sort.clone(), sort],
    );
}

#[test_case(
    S::Project(vec![rename_project("a", "b")]),
    S::Filter(Expr::Eq(Box::new(Expr::Field(field("a"))), Box::new(Expr::Literal(string_val("test"))))),
    vec![
        S::Filter(Expr::Eq(Box::new(Expr::Field(field("b"))), Box::new(Expr::Literal(string_val("test"))))),
        S::Project(vec![rename_project("a", "b")])
    ]
    ; "rename project through filter"
)]
#[test_case(
    S::Rename(vec![(field("b"), field("a"))]),
    S::Filter(Expr::Eq(Box::new(Expr::Field(field("a"))), Box::new(Expr::Literal(string_val("test"))))),
    vec![
        S::Filter(Expr::Eq(Box::new(Expr::Field(field("b"))), Box::new(Expr::Literal(string_val("test"))))),
        S::Rename(vec![(field("b"), field("a"))])
    ]
    ; "rename through filter"
)]
#[test_case(
    S::Project(vec![literal_project("c", int_val(50))]),
    S::Filter(Expr::Eq(Box::new(Expr::Field(field("c"))), Box::new(Expr::Literal(int_val(50))))),
    vec![
        S::Filter(Expr::Eq(Box::new(Expr::Literal(int_val(50))), Box::new(Expr::Literal(int_val(50))))),
        S::Project(vec![literal_project("c", int_val(50))])
    ]
    ; "literal through filter"
)]
#[test_case(
    S::Project(vec![rename_project("a", "b"), literal_project("c", int_val(50))]),
    S::Filter(Expr::Eq(Box::new(Expr::Field(field("a"))), Box::new(Expr::Field(field("c"))))),
    vec![
        S::Filter(Expr::Eq(Box::new(Expr::Field(field("b"))), Box::new(Expr::Literal(int_val(50))))),
        S::Project(vec![rename_project("a", "b"), literal_project("c", int_val(50))])
    ]
    ; "mixed rename project and literal through filter"
)]
#[test_case(
    S::Extend(vec![rename_project("a", "b")]),
    S::Filter(Expr::Eq(Box::new(Expr::Field(field("a"))), Box::new(Expr::Literal(string_val("test"))))),
    vec![
        S::Filter(Expr::Eq(Box::new(Expr::Field(field("b"))), Box::new(Expr::Literal(string_val("test"))))),
        S::Extend(vec![rename_project("a", "b")])
    ]
    ; "extend through filter"
)]
#[test_case(
    S::Project(vec![rename_project("a", "b")]),
    S::Sort(vec![sort_asc(field("a"))]),
    vec![
        S::Sort(vec![sort_asc(field("b"))]),
        S::Project(vec![rename_project("a", "b")])
    ]
    ; "rename project through sort asc"
)]
#[test_case(
    S::Project(vec![rename_project("a", "b")]),
    S::Sort(vec![sort_desc(field("a"))]),
    vec![
        S::Sort(vec![sort_desc(field("b"))]),
        S::Project(vec![rename_project("a", "b")])
    ]
    ; "rename project through sort desc"
)]
#[test_case(
    S::Rename(vec![(field("b"), field("a"))]),
    S::Sort(vec![sort_desc(field("a"))]),
    vec![
        S::Sort(vec![sort_desc(field("b"))]),
        S::Rename(vec![(field("b"), field("a"))])
    ]
    ; "rename through sort desc"
)]
#[test_case(
    S::Project(vec![literal_project("c", int_val(50))]),
    S::Sort(vec![sort_asc(field("c"))]),
    vec![S::Project(vec![literal_project("c", int_val(50))])]
    ; "literal sort removed"
)]
#[test_case(
    S::Project(vec![rename_project("a", "b")]),
    S::TopN(vec![sort_desc(field("a"))], 10),
    vec![
        S::TopN(vec![sort_desc(field("b"))], 10),
        S::Project(vec![rename_project("a", "b")])
    ]
    ; "rename project through topn"
)]
#[test_case(
    S::Rename(vec![(field("b"), field("a"))]),
    S::TopN(vec![sort_desc(field("a"))], 10),
    vec![
        S::TopN(vec![sort_desc(field("b"))], 10),
        S::Rename(vec![(field("b"), field("a"))])
    ]
    ; "rename through topn"
)]
#[test_case(
    S::Project(vec![literal_project("c", int_val(100))]),
    S::TopN(vec![sort_asc(field("c"))], 3),
    vec![S::Project(vec![literal_project("c", int_val(100))])]
    ; "literal topn removed"
)]
#[test_case(
    S::Project(vec![rename_project("a", "b")]),
    S::Limit(100),
    vec![S::Limit(100), S::Project(vec![rename_project("a", "b")])]
    ; "rename project through limit"
)]
#[test_case(
    S::Rename(vec![(field("b"), field("a"))]),
    S::Limit(100),
    vec![S::Limit(100), S::Rename(vec![(field("b"), field("a"))])]
    ; "rename through limit"
)]
#[test_case(
    S::Project(vec![literal_project("x", int_val(42))]),
    S::Limit(50),
    vec![S::Limit(50), S::Project(vec![literal_project("x", int_val(42))])]
    ; "literal through limit"
)]
#[test_case(
    S::Extend(vec![rename_project("a", "b")]),
    S::Limit(25),
    vec![S::Limit(25), S::Extend(vec![rename_project("a", "b")])]
    ; "extend through limit"
)]
fn test_project_propagation_through_next_step(first_step: S, second_step: S, expected: Vec<S>) {
    check_default(vec![first_step, second_step], expected);
}

#[test_case(
    vec![
        S::Project(vec![rename_project("a", "b")]),
        S::Limit(1),
        S::Project(vec![rename_project("c", "a")]),
    ],
    vec![
        S::Limit(1),
        S::Project(vec![rename_project("c", "b")])
    ]
    ; "rename project through project"
)]
#[test_case(
    vec![
        S::Rename(vec![(field("b"), field("a"))]),
        S::Limit(1),
        S::Project(vec![rename_project("c", "a")]),
    ],
    vec![
        S::Limit(1),
        S::Project(vec![rename_project("c", "b")])
    ]
    ; "rename through project"
)]
#[test_case(
    vec![
        S::Project(vec![rename_project("a", "b")]),
        S::Limit(1),
        S::Extend(vec![rename_project("c", "a")]),
    ],
    vec![
        S::Limit(1),
        S::Extend(vec![rename_project("c", "b")]),
        S::Project(vec![rename_project("a", "b")])
    ]
    ; "rename project through extend"
)]
#[test_case(
    vec![
        S::Rename(vec![(field("b"), field("a"))]),
        S::Limit(1),
        S::Extend(vec![rename_project("c", "a")]),
    ],
    vec![
        S::Limit(1),
        S::Extend(vec![rename_project("c", "b")]),
        S::Rename(vec![(field("b"), field("a"))])
    ]
    ; "rename through extend"
)]
#[test_case(
    vec![
        S::Project(vec![literal_project("c", int_val(50))]),
        S::Limit(1),
        S::Project(vec![
            rename_project("d", "c"),
            rename_project("e", "f")
        ]),
    ],
    vec![
        S::Limit(1),
        S::Project(vec![
            literal_project("d", int_val(50)),
            rename_project("e", "f")
        ]),
    ]
    ; "literal through project"
)]
#[test_case(
    vec![
        S::Project(vec![literal_project("c", int_val(50))]),
        S::Limit(1),
        S::Extend(vec![rename_project("d", "c")]),
    ],
    vec![
        S::Limit(1),
        S::Extend(vec![literal_project("d", int_val(50))]),
        S::Project(vec![literal_project("c", int_val(50))])
    ]
    ; "literal through extend"
)]
#[test_case(
    vec![
        S::Extend(vec![rename_project("a", "b")]),
        S::Limit(1),
        S::Project(vec![rename_project("c", "a")]),
    ],
    vec![
        S::Limit(1),
        S::Project(vec![rename_project("c", "b")])
    ]
    ; "extend rename project through project"
)]
#[test_case(
    vec![
        S::Extend(vec![rename_project("a", "b")]),
        S::Limit(1),
        S::Extend(vec![rename_project("c", "a")]),
    ],
    vec![
        S::Limit(1),
        S::Extend(vec![rename_project("c", "b")]),
        S::Extend(vec![rename_project("a", "b")])
    ]
    ; "extend rename project through extend"
)]
#[test_case(
    vec![
        S::Project(vec![
            rename_project("a", "b"),
            literal_project("c", int_val(10)),
        ]),
        S::Limit(1),
        S::Project(vec![
            rename_project("d", "a"),
            rename_project("e", "c"),
        ]),
    ],
    vec![
        S::Limit(1),
        S::Project(vec![
            rename_project("d", "b"),
            literal_project("e", int_val(10)),
        ]),
    ]
    ; "mixed rename project and literal through project"
)]
#[test_case(
    vec![
        S::Project(vec![
            rename_project("a", "b"),
            literal_project("c", int_val(10)),
        ]),
        S::Limit(1),
        S::Extend(vec![
            rename_project("d", "a"),
            rename_project("e", "c"),
        ]),
    ],
    vec![
        S::Limit(1),
        S::Extend(vec![
            rename_project("d", "b"),
            literal_project("e", int_val(10)),
        ]),
        S::Project(vec![
            rename_project("a", "b"),
            literal_project("c", int_val(10)),
        ]),
    ]
    ; "mixed rename project and literal through extend"
)]
fn test_project_propagation_through_project(input: Vec<S>, expected: Vec<S>) {
    check_default(input, expected);
}

#[test_case(
    vec![rename_project("a", "b")],
    Aggregation::Sum(field("a")),
    "total",
    vec![Expr::Field(field("c"))],
    vec![
        S::Summarize(Summarize {
            aggs: {
                let mut map = HashMap::new();
                map.insert(field("total"), Aggregation::Sum(field("b")));
                map
            },
            by: vec![Expr::Field(field("c"))],
        })
    ]
    ; "rename project sum aggregation"
)]
#[test_case(
    vec![rename_project("a", "b")],
    Aggregation::Min(field("a")),
    "min_val",
    vec![Expr::Field(field("d"))],
    vec![
        S::Summarize(Summarize {
            aggs: {
                let mut map = HashMap::new();
                map.insert(field("min_val"), Aggregation::Min(field("b")));
                map
            },
            by: vec![Expr::Field(field("d"))],
        })
    ]
    ; "rename project min aggregation"
)]
#[test_case(
    vec![rename_project("a", "b")],
    Aggregation::Max(field("a")),
    "max_val",
    vec![Expr::Field(field("e"))],
    vec![
        S::Summarize(Summarize {
            aggs: {
                let mut map = HashMap::new();
                map.insert(field("max_val"), Aggregation::Max(field("b")));
                map
            },
            by: vec![Expr::Field(field("e"))],
        })
    ]
    ; "rename project max aggregation"
)]
#[test_case(
    vec![rename_project("a", "b")],
    Aggregation::DCount(field("a")),
    "unique_count",
    vec![Expr::Field(field("f"))],
    vec![
        S::Summarize(Summarize {
            aggs: hashmap! { field("unique_count") => Aggregation::DCount(field("b")) },
            by: vec![Expr::Field(field("f"))],
        })
    ]
    ; "rename project dcount aggregation"
)]
fn test_project_propagation_rename_through_summarize(
    project_fields: Vec<ProjectField>,
    aggregation: Aggregation,
    agg_name: &str,
    by_clause: Vec<Expr>,
    expected: Vec<S>,
) {
    let input = vec![
        S::Project(project_fields),
        S::Summarize(Summarize {
            aggs: {
                let mut map = HashMap::new();
                map.insert(field(agg_name), aggregation);
                map
            },
            by: by_clause,
        }),
    ];
    check_default(input, expected);
}

#[test_case(
    literal_project("x", int_val(10)),
    "total",
    Aggregation::Sum(field("x")),
    vec![Expr::Field(field("c"))],
    vec![
        S::Summarize(Summarize {
            aggs: hashmap! { field("total") => Aggregation::Count },
            by: vec![Expr::Field(field("c"))],
        }),
        S::Project(vec![
            ProjectField {
                from: Expr::Mul(
                    Box::new(Expr::Field(field("total"))),
                    Box::new(Expr::Literal(int_val(10))),
                ),
                to: field("total"),
            },
            noop_project("c"),
        ])
    ]
    ; "literal sum becomes count times literal"
)]
#[test_case(
    literal_project("x", int_val(10)),
    "min_x",
    Aggregation::Min(field("x")),
    vec![Expr::Field(field("c"))],
    vec![
        S::Summarize(Summarize {
            aggs: HashMap::new(),
            by: vec![Expr::Field(field("c"))],
        }),
        S::Project(vec![
            ProjectField {
                from: Expr::Literal(int_val(10)),
                to: field("min_x"),
            },
            noop_project("c"),
        ])
    ]
    ; "literal min becomes extend with literal"
)]
#[test_case(
    literal_project("x", int_val(42)),
    "unique_x",
    Aggregation::DCount(field("x")),
    vec![Expr::Field(field("d"))],
    vec![
        S::Summarize(Summarize {
            aggs: HashMap::new(),
            by: vec![Expr::Field(field("d"))],
        }),
        S::Project(vec![
            ProjectField {
                from: Expr::Literal(int_val(1)),
                to: field("unique_x"),
            },
            noop_project("d"),
        ])
    ]
    ; "literal dcount becomes extend with 1"
)]
fn test_project_propagation_literal_through_summarize(
    project_field: ProjectField,
    agg_name: &str,
    agg: Aggregation,
    by: Vec<Expr>,
    expected: Vec<S>,
) {
    let input = vec![
        S::Project(vec![project_field]),
        S::Summarize(Summarize {
            aggs: hashmap! { field(agg_name) => agg },
            by,
        }),
    ];
    check_default(input, expected);
}

#[test_case(
    S::Project(vec![rename_project("a", "b")]),
    Aggregation::Sum(field("a")),
    "total",
    S::MuxSummarize,
    vec![
        S::MuxSummarize(Summarize {
            aggs: hashmap! { field("total") => Aggregation::Sum(field("b")) },
            by: vec![Expr::Field(field("c"))],
        })
    ]
    ; "rename project through mux_summarize"
)]
#[test_case(
    S::Rename(vec![(field("b"), field("a"))]),
    Aggregation::Sum(field("a")),
    "total",
    S::MuxSummarize,
    vec![
        S::MuxSummarize(Summarize {
            aggs: hashmap! { field("total") => Aggregation::Sum(field("b")) },
            by: vec![Expr::Field(field("c"))],
        })
    ]
    ; "rename through mux_summarize"
)]
#[test_case(
    S::Project(vec![rename_project("a", "b")]),
    Aggregation::Count,
    "cnt",
    S::Summarize,
    vec![
        S::Summarize(Summarize {
            aggs: hashmap! { field("cnt") => Aggregation::Count },
            by: vec![Expr::Field(field("c"))],
        })
    ]
    ; "rename project through summarize with count"
)]
#[test_case(
    S::Rename(vec![(field("b"), field("a"))]),
    Aggregation::Count,
    "cnt",
    S::Summarize,
    vec![
        S::Summarize(Summarize {
            aggs: hashmap! { field("cnt") => Aggregation::Count },
            by: vec![Expr::Field(field("c"))],
        })
    ]
    ; "rename through summarize with count"
)]
fn test_project_propagation_summarize_variants(
    step: S,
    aggregation: Aggregation,
    agg_name: &str,
    summarize_constructor: fn(Summarize) -> S,
    expected: Vec<S>,
) {
    let input = vec![
        step,
        summarize_constructor(Summarize {
            aggs: hashmap! { field(agg_name) => aggregation },
            by: vec![Expr::Field(field("c"))],
        }),
    ];
    check_default(input, expected);
}

#[test_case(
    vec![
        S::Project(vec![rename_project("a", "b")]),
        S::Filter(Expr::Gt(Box::new(Expr::Field(field("a"))), Box::new(Expr::Literal(int_val(0))))),
        S::Sort(vec![sort_desc(field("a"))])
    ],
    vec![
        S::Filter(Expr::Gt(Box::new(Expr::Field(field("b"))), Box::new(Expr::Literal(int_val(0))))),
        S::Sort(vec![sort_desc(field("b"))]),
        S::Project(vec![rename_project("a", "b")])
    ]
    ; "rename project through filter and sort"
)]
#[test_case(
    vec![
        S::Rename(vec![(field("b"), field("a"))]),
        S::Filter(Expr::Gt(Box::new(Expr::Field(field("a"))), Box::new(Expr::Literal(int_val(0))))),
        S::Sort(vec![sort_desc(field("a"))])
    ],
    vec![
        S::Filter(Expr::Gt(Box::new(Expr::Field(field("b"))), Box::new(Expr::Literal(int_val(0))))),
        S::Sort(vec![sort_desc(field("b"))]),
        S::Rename(vec![(field("b"), field("a"))])
    ]
    ; "rename through filter and sort"
)]
#[test_case(
    vec![
        S::Project(vec![rename_project("a", "b")]),
        S::Filter(Expr::Lt(Box::new(Expr::Field(field("a"))), Box::new(Expr::Literal(int_val(100))))),
        S::TopN(vec![sort_asc(field("a"))], 5),
        S::Limit(3)
    ],
    vec![
        S::Filter(Expr::Lt(Box::new(Expr::Field(field("b"))), Box::new(Expr::Literal(int_val(100))))),
        S::TopN(vec![sort_asc(field("b"))], 3),
        S::Project(vec![rename_project("a", "b")])
    ]
    ; "rename project through filter, topn, and limit"
)]
#[test_case(
    vec![
        S::Rename(vec![(field("b"), field("a"))]),
        S::Filter(Expr::Lt(Box::new(Expr::Field(field("a"))), Box::new(Expr::Literal(int_val(100))))),
        S::TopN(vec![sort_asc(field("a"))], 5),
        S::Limit(3)
    ],
    vec![
        S::Filter(Expr::Lt(Box::new(Expr::Field(field("b"))), Box::new(Expr::Literal(int_val(100))))),
        S::TopN(vec![sort_asc(field("b"))], 3),
        S::Rename(vec![(field("b"), field("a"))])
    ]
    ; "rename through filter, topn, and limit"
)]
#[test_case(
    vec![
        S::Project(vec![literal_project("x", int_val(50))]),
        S::Filter(Expr::Eq(Box::new(Expr::Field(field("x"))), Box::new(Expr::Literal(int_val(50))))),
        S::Sort(vec![sort_asc(field("x"))])
    ],
    vec![
        S::Filter(Expr::Eq(Box::new(Expr::Literal(int_val(50))), Box::new(Expr::Literal(int_val(50))))),
        S::Project(vec![literal_project("x", int_val(50))])
    ]
    ; "literal through filter with sort removed"
)]
#[test_case(
    vec![
        S::Project(vec![rename_project("a", "b")]),
        S::Expand(expand(vec![field("a")])),
        S::Sort(vec![sort_desc(field("a"))])
    ],
    vec![
        S::Expand(expand(vec![field("b")])),
        S::Sort(vec![sort_desc(field("b"))]),
        S::Project(vec![rename_project("a", "b")])
    ]
    ; "rename project through expand and sort"
)]
#[test_case(
    vec![
        S::Rename(vec![(field("b"), field("a"))]),
        S::Expand(expand(vec![field("a")])),
        S::Sort(vec![sort_asc(field("a"))])
    ],
    vec![
        S::Expand(expand(vec![field("b")])),
        S::Sort(vec![sort_asc(field("b"))]),
        S::Rename(vec![(field("b"), field("a"))])
    ]
    ; "rename through expand and sort"
)]
#[test_case(
    vec![
        S::Project(vec![rename_project("a", "b")]),
        S::Filter(Expr::Eq(Box::new(Expr::Field(field("a"))), Box::new(Expr::Literal(int_val(10))))),
        S::Expand(expand(vec![field("a")])),
        S::Limit(2)
    ],
    vec![
        S::Filter(Expr::Eq(Box::new(Expr::Field(field("b"))), Box::new(Expr::Literal(int_val(10))))),
        S::Expand(expand(vec![field("b")])),
        S::Limit(2),
        S::Project(vec![rename_project("a", "b")])
    ]
    ; "rename project through filter, expand, and limit"
)]
#[test_case(
    vec![
        S::Rename(vec![(field("b"), field("a"))]),
        S::Filter(Expr::Eq(Box::new(Expr::Field(field("a"))), Box::new(Expr::Literal(int_val(10))))),
        S::Expand(expand(vec![field("a")])),
        S::Limit(2)
    ],
    vec![
        S::Filter(Expr::Eq(Box::new(Expr::Field(field("b"))), Box::new(Expr::Literal(int_val(10))))),
        S::Expand(expand(vec![field("b")])),
        S::Limit(2),
        S::Rename(vec![(field("b"), field("a"))])
    ]
    ; "rename through filter, expand, and limit"
)]
#[test_case(
    vec![
        S::Project(vec![literal_project("x", int_val(50))]),
        S::Expand(expand(vec![field("x")])),
        S::Filter(Expr::Eq(Box::new(Expr::Field(field("x"))), Box::new(Expr::Literal(int_val(50))))),
    ],
    vec![
        S::Filter(Expr::Eq(Box::new(Expr::Literal(int_val(50))), Box::new(Expr::Literal(int_val(50))))),
        S::Project(vec![literal_project("x", int_val(50))])
    ]
    ; "literal through filter with expand removed"
)]
fn test_project_propagation_multi_step(input: Vec<S>, expected: Vec<S>) {
    check_default(input, expected);
}

#[test]
fn test_project_propagation_drop_unused_field_through_summarize() {
    let literal_value = int_val(42);
    let input = vec![
        S::Project(vec![
            literal_project("x", literal_value.clone()),
            rename_project("unused", "unused2"),
        ]),
        S::Summarize(Summarize {
            aggs: {
                let mut map = HashMap::new();
                map.insert(field("max_x"), Aggregation::Max(field("x")));
                map
            },
            by: vec![Expr::Field(field("c"))],
        }),
    ];

    let expected = vec![
        S::Summarize(Summarize {
            aggs: HashMap::new(),
            by: vec![Expr::Field(field("c"))],
        }),
        S::Project(vec![
            ProjectField {
                from: Expr::Literal(literal_value),
                to: field("max_x"),
            },
            noop_project("c"),
        ]),
    ];

    check_default(input, expected);
}

#[test]
fn test_project_propagation_rename_by_clause_field_through_summarize() {
    let summarize = Summarize {
        aggs: HashMap::new(),
        by: vec![Expr::Field(field("z"))],
    };
    let project_input = vec![
        S::Project(vec![rename_project("z", "c")]),
        S::Summarize(summarize.clone()),
    ];
    let rename_input = vec![
        S::Rename(vec![(field("c"), field("z"))]),
        S::Summarize(summarize),
    ];

    let expected = vec![
        S::Summarize(Summarize {
            aggs: HashMap::new(),
            by: vec![Expr::Field(field("c"))],
        }),
        S::Project(vec![rename_project("z", "c")]),
    ];

    check_default(project_input, expected.clone());
    check_default(rename_input, expected.clone());
}

#[test]
fn test_project_propagation_rename_summarize_by_bin() {
    let summarize = Summarize {
        aggs: HashMap::new(),
        by: vec![Expr::Bin(
            Box::new(Expr::Field(field("x"))),
            Box::new(Expr::Literal(int_val(2))),
        )],
    };
    let project_input = vec![
        S::Project(vec![rename_project("x", "z")]),
        S::Summarize(summarize.clone()),
    ];
    let rename_input = vec![
        S::Rename(vec![(field("z"), field("x"))]),
        S::Summarize(summarize),
    ];

    let expected = vec![
        S::Summarize(Summarize {
            aggs: HashMap::new(),
            by: vec![Expr::Bin(
                Box::new(Expr::Field(field("z"))),
                Box::new(Expr::Literal(int_val(2))),
            )],
        }),
        S::Project(vec![rename_project("x", "z")]),
    ];

    check_default(project_input, expected.clone());
    check_default(rename_input, expected.clone());
}

#[test]
fn test_project_propagation_complex_expression_no_optimization() {
    let complex_expr = Expr::Plus(
        Box::new(Expr::Field(field("b"))),
        Box::new(Expr::Field(field("c"))),
    );

    let input = vec![
        S::Project(vec![project_field("a", complex_expr.clone())]),
        S::Filter(Expr::Gt(
            Box::new(Expr::Field(field("a"))),
            Box::new(Expr::Literal(int_val(10))),
        )),
    ];

    let expected = vec![
        S::Project(vec![project_field("a", complex_expr)]),
        S::Filter(Expr::Gt(
            Box::new(Expr::Field(field("a"))),
            Box::new(Expr::Literal(int_val(10))),
        )),
    ];

    check_default(input, expected);
}

#[test]
fn test_project_propagation_partial_optimization_with_mixed_expressions() {
    let complex_expr = Expr::Plus(
        Box::new(Expr::Field(field("c"))),
        Box::new(Expr::Field(field("e"))),
    );

    let input = vec![
        S::Project(vec![
            rename_project("a", "b"),
            project_field("d", complex_expr.clone()),
        ]),
        S::Filter(Expr::Eq(
            Box::new(Expr::Field(field("a"))),
            Box::new(Expr::Literal(string_val("test"))),
        )),
    ];

    let expected = vec![
        S::Project(vec![project_field("d", complex_expr)]),
        S::Filter(Expr::Eq(
            Box::new(Expr::Field(field("b"))),
            Box::new(Expr::Literal(string_val("test"))),
        )),
        S::Project(vec![rename_project("a", "b")]),
    ];

    check_default(input, expected);
}

#[test]
fn remove_no_op_filter_where_true() {
    check_default(
        vec![S::Filter(Expr::Literal(Value::Bool(true))), S::Limit(10)],
        vec![S::Limit(10)],
    );
}

#[test]
fn no_op_filter_keeps_where_false() {
    let filter = S::Filter(Expr::Literal(Value::Bool(false)));
    check_default(vec![filter.clone()], vec![filter]);
}

#[test]
fn const_fold_in_filter_simple_arith() {
    let filter = S::Filter(Expr::Gt(
        Box::new(Expr::Field(field("x"))),
        Box::new(Expr::Minus(
            Box::new(Expr::Mul(
                Box::new(Expr::Literal(json!(50))),
                Box::new(Expr::Literal(json!(10))),
            )),
            Box::new(Expr::Literal(json!(2))),
        )),
    ));

    check_default(
        vec![filter],
        vec![S::Filter(Expr::Gt(
            Box::new(Expr::Field(field("x"))),
            Box::new(Expr::Literal(json!(498))),
        ))],
    );
}

#[test]
fn const_fold_in_filter_nested() {
    let filter = S::Filter(Expr::Eq(
        Box::new(Expr::Field(field("x"))),
        Box::new(Expr::Mul(
            Box::new(Expr::Plus(
                Box::new(Expr::Literal(json!(1))),
                Box::new(Expr::Literal(json!(2))),
            )),
            Box::new(Expr::Plus(
                Box::new(Expr::Literal(json!(3))),
                Box::new(Expr::Literal(json!(4))),
            )),
        )),
    ));

    check_default(
        vec![filter],
        vec![S::Filter(Expr::Eq(
            Box::new(Expr::Field(field("x"))),
            Box::new(Expr::Literal(json!(21))),
        ))],
    );
}

#[test_case(S::Project as fn(Vec<ProjectField>) -> S)]
#[test_case(S::Extend as fn(Vec<ProjectField>) -> S)]
fn const_fold_in_fields_simple(ctor: fn(Vec<ProjectField>) -> S) {
    let step = ctor(vec![ProjectField {
        from: Expr::Mul(
            Box::new(Expr::Plus(
                Box::new(Expr::Literal(json!(2))),
                Box::new(Expr::Literal(json!(3))),
            )),
            Box::new(Expr::Literal(json!(4))),
        ),
        to: field("b"),
    }]);

    check_default(
        vec![step],
        vec![ctor(vec![ProjectField {
            from: Expr::Literal(json!(20)),
            to: field("b"),
        }])],
    );
}

#[test_case(S::Project as fn(Vec<ProjectField>) -> S)]
#[test_case(S::Extend as fn(Vec<ProjectField>) -> S)]
fn const_fold_in_fields_partial(ctor: fn(Vec<ProjectField>) -> S) {
    let step = ctor(vec![ProjectField {
        from: Expr::Plus(
            Box::new(Expr::Field(field("a"))),
            Box::new(Expr::Plus(
                Box::new(Expr::Literal(json!(10))),
                Box::new(Expr::Literal(json!(3))),
            )),
        ),
        to: field("c"),
    }]);

    check_default(
        vec![step.clone()],
        vec![ctor(vec![ProjectField {
            from: Expr::Plus(
                Box::new(Expr::Field(field("a"))),
                Box::new(Expr::Literal(json!(13))),
            ),
            to: field("c"),
        }])],
    );
}

#[test]
fn const_fold_no_fold_when_partial_eval_fails_keeps_expr() {
    let bad = S::Filter(Expr::Div(
        Box::new(Expr::Literal(json!(1))),
        Box::new(Expr::Literal(json!(0))),
    ));

    check_default(vec![bad.clone()], vec![bad]);
}

#[test]
fn const_fold_only_affects_first_matched_step() {
    let filter_then_project = vec![
        S::Filter(Expr::Gt(
            Box::new(Expr::Field(field("x"))),
            Box::new(Expr::Plus(
                Box::new(Expr::Literal(json!(400))),
                Box::new(Expr::Literal(json!(98))),
            )),
        )),
        S::Project(vec![ProjectField {
            from: Expr::Plus(
                Box::new(Expr::Literal(json!(1))),
                Box::new(Expr::Literal(json!(1))),
            ),
            to: field("b"),
        }]),
    ];

    let expected = vec![
        S::Filter(Expr::Gt(
            Box::new(Expr::Field(field("x"))),
            Box::new(Expr::Literal(json!(498))),
        )),
        S::Project(vec![ProjectField {
            from: Expr::Literal(json!(2)),
            to: field("b"),
        }]),
    ];

    check_default(filter_then_project, expected);
}
