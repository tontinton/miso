use std::str::FromStr;

use miso_workflow_types::{
    expr::{CastType, Expr},
    field::Field,
    field_unwrap,
    join::JoinType,
    query::{QueryStep, ScanKind},
    sort::{NullsOrder, SortOrder},
    summarize::Aggregation,
    value::Value,
};
use test_case::test_case;
use time::{Duration, OffsetDateTime, UtcOffset};

use crate::parse;

macro_rules! parse_unwrap {
    ($expr:expr) => {
        parse($expr).unwrap_or_else(|e| panic!("failed to parse query {:?}: {:?}", $expr, e))
    };
}

fn secs_to_dt(ms: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(ms)
        .unwrap()
        .to_offset(UtcOffset::UTC)
}

#[test]
fn test_simple_scan() {
    let query = "connector.table";
    let result = parse_unwrap!(query);

    assert_eq!(result.len(), 1);
    match &result[0] {
        QueryStep::Scan(ScanKind::Collection {
            connector,
            collection,
        }) => {
            assert_eq!(connector, "connector");
            assert_eq!(collection, "table");
        }
        _ => panic!("Expected Scan step"),
    }
}

#[test]
fn test_scan_with_filter() {
    let query = "connector.table | where field1 == \"value\"";
    let result = parse_unwrap!(query);

    assert_eq!(result.len(), 2);

    match &result[0] {
        QueryStep::Scan(ScanKind::Collection {
            connector,
            collection,
        }) => {
            assert_eq!(connector, "connector");
            assert_eq!(collection, "table");
        }
        _ => panic!("Expected Scan step"),
    }

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(left, right) => {
                assert!(matches!(**left, Expr::Field(_)));
                assert!(matches!(**right, Expr::Literal(Value::String(_))));
            }
            _ => panic!("Expected equality expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test_case("connector.table | where field1 > 10", "Gt")]
#[test_case("connector.table | where field1 < 10", "Lt")]
#[test_case("connector.table | where field1 >= 10", "Gte")]
#[test_case("connector.table | where field1 <= 10", "Lte")]
#[test_case("connector.table | where field1 != 10", "Ne")]
fn test_filter_with_different_operators(query: &str, op_name: &str) {
    let result = parse_unwrap!(query);
    assert_eq!(result.len(), 2);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Gt(_, _) if op_name == "Gt" => (),
            Expr::Lt(_, _) if op_name == "Lt" => (),
            Expr::Gte(_, _) if op_name == "Gte" => (),
            Expr::Lte(_, _) if op_name == "Lte" => (),
            Expr::Ne(_, _) if op_name == "Ne" => (),
            _ => panic!("Expected {} expression for query: {}", op_name, query),
        },
        _ => panic!("Expected Filter step for query: {}", query),
    }
}

#[test]
fn test_filter_with_between_operator() {
    let query = "connector.table | where field1 between (50 .. 55)";
    let result = parse_unwrap!(query);
    assert_eq!(result.len(), 2);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::And(left, right) => {
                match left.as_ref() {
                    Expr::Gte(field, val) => {
                        assert!(matches!(field.as_ref(), Expr::Field(_)));
                        assert!(matches!(val.as_ref(), Expr::Literal(_)));
                    }
                    _ => panic!("Expected Gte expression on left side of And"),
                }

                match right.as_ref() {
                    Expr::Lte(field, val) => {
                        assert!(matches!(field.as_ref(), Expr::Field(_)));
                        assert!(matches!(val.as_ref(), Expr::Literal(_)));
                    }
                    _ => panic!("Expected Lte expression on right side of And"),
                }
            }
            _ => panic!("Expected And expression for between query: {}", query),
        },
        _ => panic!("Expected Filter step for query: {}", query),
    }
}

#[test]
fn test_filter_with_between_float_range() {
    let query = "connector.table | where temperature between (98.6 .. 102.5)";
    let result = parse_unwrap!(query);
    assert_eq!(result.len(), 2);

    match &result[1] {
        QueryStep::Filter(expr) => {
            assert!(matches!(expr, Expr::And(_, _)));
        }
        _ => panic!("Expected Filter step with between expression"),
    }
}

#[test]
fn test_filter_with_between_and_other_conditions() {
    let query = "connector.table | where field1 between (10 .. 20) and field2 == 5";
    let result = parse_unwrap!(query);
    assert_eq!(result.len(), 2);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::And(left, right) => {
                assert!(matches!(left.as_ref(), Expr::And(_, _)));
                assert!(matches!(right.as_ref(), Expr::Eq(_, _)));
            }
            _ => panic!("Expected nested And expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_filter_with_not_between_operator() {
    let query = "connector.table | where field1 !between (50 .. 55)";
    let result = parse_unwrap!(query);
    assert_eq!(result.len(), 2);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Or(left, right) => {
                match left.as_ref() {
                    Expr::Lt(field, val) => {
                        assert!(matches!(field.as_ref(), Expr::Field(_)));
                        assert!(matches!(val.as_ref(), Expr::Literal(_)));
                    }
                    _ => panic!("Expected Lt expression on left side of Or"),
                }

                match right.as_ref() {
                    Expr::Gt(field, val) => {
                        assert!(matches!(field.as_ref(), Expr::Field(_)));
                        assert!(matches!(val.as_ref(), Expr::Literal(_)));
                    }
                    _ => panic!("Expected Gt expression on right side of Or"),
                }
            }
            _ => panic!("Expected Or expression for !between query: {}", query),
        },
        _ => panic!("Expected Filter step for query: {}", query),
    }
}

#[test_case("connector.table | where field1 contains \"test\"", "contains")]
#[test_case("connector.table | where field1 startswith \"test\"", "startswith")]
#[test_case("connector.table | where field1 endswith \"test\"", "endswith")]
#[test_case("connector.table | where field1 has \"test\"", "has")]
#[test_case("connector.table | where field1 has_cs \"test\"", "has_cs")]
fn test_filter_with_text_operations(query: &str, op_name: &str) {
    let result = parse_unwrap!(query);
    assert_eq!(result.len(), 2);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Contains(_, _) if op_name == "contains" => (),
            Expr::StartsWith(_, _) if op_name == "startswith" => (),
            Expr::EndsWith(_, _) if op_name == "endswith" => (),
            Expr::Has(_, _) if op_name == "has" => (),
            Expr::HasCs(_, _) if op_name == "has_cs" => (),
            _ => panic!("Expected {} expression for query: {}", op_name, query),
        },
        _ => panic!("Expected Filter step for query: {}", query),
    }
}

#[test]
fn test_filter_with_logical_operations() {
    let query = "connector.table | where field1 == \"value\" and field2 > 10";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::And(left, right) => {
                assert!(matches!(**left, Expr::Eq(_, _)));
                assert!(matches!(**right, Expr::Gt(_, _)));
            }
            _ => panic!("Expected And expression"),
        },
        _ => panic!("Expected Filter step"),
    }

    let query = "connector.table | where field1 == \"value\" or field2 > 10";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Or(left, right) => {
                assert!(matches!(**left, Expr::Eq(_, _)));
                assert!(matches!(**right, Expr::Gt(_, _)));
            }
            _ => panic!("Expected Or expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_filter_with_in_expression() {
    let query = "connector.table | where field1 in (\"a\", \"b\", \"c\")";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::In(field_expr, values) => {
                assert!(matches!(**field_expr, Expr::Field(_)));
                assert_eq!(values.len(), 3);
            }
            _ => panic!("Expected In expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test_case("field1"; "regular field")]
#[test_case("@time"; "special field")]
fn test_filter_with_exists(field: &str) {
    let query = &format!("connector.table | where exists({field})");
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Exists(_) => (),
            _ => panic!("Expected Exists expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_filter_with_not() {
    let query = "connector.table | where not(field1 == \"value\")";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Not(inner) => {
                assert!(matches!(**inner, Expr::Eq(_, _)));
            }
            _ => panic!("Expected Not expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test_case(
    "connector.table | where tostring(field1) == \"test\"",
    CastType::String
)]
#[test_case("connector.table | where toint(field1) == 42", CastType::Int)]
#[test_case("connector.table | where tolong(field1) == 42", CastType::Int)]
#[test_case("connector.table | where toreal(field1) == 3.14", CastType::Float)]
#[test_case("connector.table | where todecimal(field1) == 3.14", CastType::Float)]
#[test_case("connector.table | where tobool(field1) == true", CastType::Bool)]
fn test_filter_with_cast_operations(query: &str, expected_cast_type: CastType) {
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(left, _) => match &**left {
                Expr::Cast(cast_type, _) => {
                    assert_eq!(*cast_type, expected_cast_type);
                }
                _ => panic!("Expected Cast expression in left side"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_filter_with_bin_operation() {
    let query = "connector.table | where bin(field1, 10) == 5";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(left, _) => match &**left {
                Expr::Bin(_, _) => (),
                _ => panic!("Expected Bin expression"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_project() {
    let query = "connector.table | project field1, field2 = field3 + 1";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Project(fields) => {
            assert_eq!(fields.len(), 2);

            assert_eq!(fields[0].to, field_unwrap!("field1"));
            assert!(matches!(fields[0].from, Expr::Field(_)));

            assert_eq!(fields[1].to, field_unwrap!("field2"));
            assert!(matches!(fields[1].from, Expr::Plus(_, _)));
        }
        _ => panic!("Expected Project step"),
    }
}

#[test]
fn test_project_unnamed() {
    let query = "connector.table | project field3 + 1, field3 + 1, field3, Column2 = 5, field5";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Project(fields) => {
            assert_eq!(fields.len(), 5);

            assert_eq!(fields[0].to, field_unwrap!("Column1"));
            assert_eq!(fields[1].to, field_unwrap!("Column2"));
            assert_eq!(fields[2].to, field_unwrap!("field3"));
            assert_eq!(fields[3].to, field_unwrap!("Column21"));
            assert_eq!(fields[4].to, field_unwrap!("field5"));
        }
        _ => panic!("Expected Project step"),
    }
}

#[test]
fn test_extend() {
    let query = "connector.table | extend newfield = field1 + field2";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Extend(fields) => {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].to, field_unwrap!("newfield"));
            assert!(matches!(fields[0].from, Expr::Plus(_, _)));
        }
        _ => panic!("Expected Extend step"),
    }
}

#[test_case("connector.table | limit 100", 100)]
#[test_case("connector.table | take 50", 50)]
fn test_limit(query: &str, expected_limit: u64) {
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Limit(n) => {
            assert_eq!(*n, expected_limit);
        }
        _ => panic!("Expected Limit step"),
    }
}

#[test]
fn test_sort() {
    let query = "connector.table | sort by field1 asc, field2 desc";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Sort(sorts) => {
            assert_eq!(sorts.len(), 2);

            assert_eq!(sorts[0].by, field_unwrap!("field1"));
            assert_eq!(sorts[0].order, SortOrder::Asc);

            assert_eq!(sorts[1].by, field_unwrap!("field2"));
            assert_eq!(sorts[1].order, SortOrder::Desc);
        }
        _ => panic!("Expected Sort step"),
    }
}

#[test]
fn test_sort_with_nulls() {
    let query = "connector.table | sort by field1 asc nulls first, field2 desc nulls last";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Sort(sorts) => {
            assert_eq!(sorts.len(), 2);

            assert_eq!(sorts[0].nulls, NullsOrder::First);
            assert_eq!(sorts[1].nulls, NullsOrder::Last);
        }
        _ => panic!("Expected Sort step"),
    }
}

#[test]
fn test_top() {
    let query = "connector.table | top 10 by field1 desc";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Top(sorts, limit) => {
            assert_eq!(*limit, 10);
            assert_eq!(sorts.len(), 1);
            assert_eq!(sorts[0].by, field_unwrap!("field1"));
            assert_eq!(sorts[0].order, SortOrder::Desc);
        }
        _ => panic!("Expected Top step"),
    }
}

#[test]
fn test_summarize() {
    let query = "connector.table | summarize cnt = count(), total = sum(field1) by field2";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Summarize(summarize) => {
            assert_eq!(summarize.aggs.len(), 2);
            assert_eq!(summarize.by.len(), 1);

            assert!(matches!(
                summarize.aggs.get(&field_unwrap!("cnt")),
                Some(Aggregation::Count)
            ));
            assert!(matches!(
                summarize.aggs.get(&field_unwrap!("total")),
                Some(Aggregation::Sum(_))
            ));

            assert!(matches!(summarize.by[0], Expr::Field(_)));
        }
        _ => panic!("Expected Summarize step"),
    }
}

#[test]
fn test_summarize_unnamed_aggregations() {
    let query = "connector.table | summarize count(), sum(field1), avg(field2)";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Summarize(summarize) => {
            assert_eq!(summarize.aggs.len(), 3);
            let keys: Vec<String> = summarize.aggs.keys().map(|f| f.to_string()).collect();
            assert!(keys.contains(&"count_".to_string()));
            assert!(keys.contains(&"sum_field1".to_string()));
            assert!(keys.contains(&"avg_field2".to_string()));
        }
        _ => panic!("Expected Summarize step"),
    }
}

#[test]
fn test_summarize_unnamed_aggregations_with_duplicates() {
    let query = "connector.table | summarize count(), count(), sum(field1)";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Summarize(summarize) => {
            assert_eq!(summarize.aggs.len(), 3);
            let keys: Vec<String> = summarize.aggs.keys().map(|f| f.to_string()).collect();
            assert!(keys.contains(&"count_".to_string()));
            assert!(keys.contains(&"count_1".to_string()));
            assert!(keys.contains(&"sum_field1".to_string()));
        }
        _ => panic!("Expected Summarize step"),
    }
}

#[test]
fn test_summarize_unnamed_with_named_conflicts() {
    let query = "connector.table | summarize count_ = sum(field1), count(), count()";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Summarize(summarize) => {
            assert_eq!(summarize.aggs.len(), 3);
            let keys: Vec<String> = summarize.aggs.keys().map(|f| f.to_string()).collect();
            assert!(keys.contains(&"count_".to_string()));
            assert!(keys.contains(&"count_1".to_string()));
            assert!(keys.contains(&"count_2".to_string()));
        }
        _ => panic!("Expected Summarize step"),
    }
}

#[test_case("count()", "Count")]
#[test_case("dcount(field1)", "DCount")]
#[test_case("sum(field1)", "Sum")]
#[test_case("min(field1)", "Min")]
#[test_case("max(field1)", "Max")]
fn test_summarize_aggregations(agg_expr: &str, agg_name: &str) {
    let query = format!("connector.table | summarize result = {}", agg_expr);
    let result = parse_unwrap!(&query);

    match &result[1] {
        QueryStep::Summarize(summarize) => {
            let agg = summarize.aggs.get(&field_unwrap!("result")).unwrap();
            match agg {
                Aggregation::Count if agg_name == "Count" => (),
                Aggregation::DCount(_) if agg_name == "DCount" => (),
                Aggregation::Sum(_) if agg_name == "Sum" => (),
                Aggregation::Min(_) if agg_name == "Min" => (),
                Aggregation::Max(_) if agg_name == "Max" => (),
                _ => panic!("Expected {} aggregation", agg_name),
            }
        }
        _ => panic!("Expected Summarize step"),
    }
}

#[test]
fn test_distinct() {
    let query = "connector.table | distinct field1, field2";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Distinct(fields) => {
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0], field_unwrap!("field1"));
            assert_eq!(fields[1], field_unwrap!("field2"));
        }
        _ => panic!("Expected Distinct step"),
    }
}

#[test]
fn test_count() {
    let query = "connector.table | count";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Count => (),
        _ => panic!("Expected Count step"),
    }
}

#[test]
fn test_union() {
    let query = "connector.table | union (other.table | where field1 > 10)";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Union(sub_query) => {
            assert_eq!(sub_query.len(), 2);
            assert!(matches!(sub_query[0], QueryStep::Scan(..)));
            assert!(matches!(sub_query[1], QueryStep::Filter(..)));
        }
        _ => panic!("Expected Union step"),
    }
}

#[test_case("$left.field1 == $right.field2", "field1", "field2")]
#[test_case("$right.field1 == $left.field2", "field2", "field1")]
#[test_case("some.field", "some.field", "some.field")]
fn test_join(join: &str, expected_left: &str, expected_right: &str) {
    let query = format!(
        "connector.table | join kind=inner (other.table) on {}",
        join
    );
    let result = parse_unwrap!(&query);

    match &result[1] {
        QueryStep::Join(join, sub_query) => {
            assert_eq!(join.type_, JoinType::Inner);
            assert_eq!(join.partitions, 1);
            assert_eq!(join.on.0, field_unwrap!(expected_left));
            assert_eq!(join.on.1, field_unwrap!(expected_right));
            assert_eq!(sub_query.len(), 1);
            assert!(matches!(sub_query[0], QueryStep::Scan(..)));
        }
        _ => panic!("Expected Join step"),
    }
}

#[test_case("inner", JoinType::Inner)]
#[test_case("outer", JoinType::Outer)]
#[test_case("left", JoinType::Left)]
#[test_case("right", JoinType::Right)]
fn test_join_types(join_type_str: &str, expected_type: JoinType) {
    let query = format!(
        "connector.table | join kind={} (other.table) on $left.field1 == $right.field2",
        join_type_str
    );
    let result = parse_unwrap!(&query);

    match &result[1] {
        QueryStep::Join(join, _) => {
            assert_eq!(join.type_, expected_type);
        }
        _ => panic!("Expected Join step"),
    }
}

#[test]
fn test_join_with_partitions() {
    let query = "connector.table | join kind=inner hint.partitions=4 (other.table) on $left.field1 == $right.field2";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Join(join, _) => {
            assert_eq!(join.partitions, 4);
        }
        _ => panic!("Expected Join step"),
    }
}

#[test]
fn test_field_with_array_access() {
    let query = "connector.table | where field1[0] == \"value\"";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(left, _) => match &**left {
                Expr::Field(field) => {
                    assert_eq!(field, &field_unwrap!("field1[0]"));
                }
                _ => panic!("Expected Field expression"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_nested_field_access() {
    let query = "connector.table | where field1.subfield == \"value\"";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(left, _) => match &**left {
                Expr::Field(field) => {
                    assert_eq!(field, &field_unwrap!("field1.subfield"));
                }
                _ => panic!("Expected Field expression"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_complex_pipeline() {
    let query = r#"
            connector.table 
            | where field1 > 10 and field2 contains "test"
            | extend newfield = field1 + field2
            | project field1, newfield, calculated = field3 * 2
            | sort by field1 asc
            | limit 100
        "#;

    let result = parse_unwrap!(query);
    assert_eq!(result.len(), 6);

    assert!(matches!(result[0], QueryStep::Scan(..)));
    assert!(matches!(result[1], QueryStep::Filter(_)));
    assert!(matches!(result[2], QueryStep::Extend(_)));
    assert!(matches!(result[3], QueryStep::Project(_)));
    assert!(matches!(result[4], QueryStep::Sort(_)));
    assert!(matches!(result[5], QueryStep::Limit(_)));
}

#[test_case("field1 == 42", "integer")]
#[test_case("field1 == 3.14", "float")]
#[test_case("field1 == true", "boolean")]
#[test_case("field1 == false", "boolean")]
#[test_case("field1 == null", "null")]
#[test_case("field1 == \"string\"", "string")]
#[test_case("field1 == datetime(2020-01-01)", "timestamp")]
#[test_case("field1 == 1h", "timespan")]
fn test_literal_values(condition: &str, literal_type: &str) {
    let query = format!("connector.table | where {}", condition);
    let result = parse_unwrap!(&query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(_, right) => match &**right {
                Expr::Literal(Value::Int(_)) if literal_type == "integer" => {}
                Expr::Literal(Value::Float(_)) if literal_type == "float" => {}
                Expr::Literal(Value::Bool(_)) if literal_type == "boolean" => {}
                Expr::Literal(Value::Null) if literal_type == "null" => {}
                Expr::Literal(Value::String(_)) if literal_type == "string" => {}
                Expr::Literal(Value::Timestamp(_)) if literal_type == "timestamp" => {}
                Expr::Literal(Value::Timespan(_)) if literal_type == "timespan" => {}
                _ => panic!("Expected {} literal", literal_type),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_arithmetic_expressions() {
    let query = "connector.table | extend result = field1 + field2 * field3 - field4 / 2";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Extend(fields) => {
            // This tests operator precedence: should be field1 + (field2 * field3) - (field4 / 2).
            match &fields[0].from {
                Expr::Minus(left, right) => {
                    assert!(matches!(**left, Expr::Plus(_, _)));
                    assert!(matches!(**right, Expr::Div(_, _)));
                }
                _ => panic!("Expected minus expression at top level"),
            }
        }
        _ => panic!("Expected Extend step"),
    }
}

fn assert_negated_literal(expr: &Expr, expected_value: f64) {
    match expr {
        Expr::Minus(left, right) => {
            assert!(
                matches!(**left, Expr::Literal(Value::Int(0))),
                "Expected left side to be 0"
            );
            match &**right {
                Expr::Literal(Value::Int(i)) => {
                    assert_eq!(*i as f64, expected_value, "Expected right side to match");
                }
                Expr::Literal(Value::Float(f)) => {
                    assert_eq!(*f, expected_value, "Expected right side to match");
                }
                _ => panic!("Expected numeric literal on right side"),
            }
        }
        _ => panic!("Expected minus expression for negation"),
    }
}

#[test_case("x == -5", 5.0, "integer")]
#[test_case("y == -3.15", 3.15, "float")]
fn test_negative_literal(condition: &str, expected_value: f64, _literal_type: &str) {
    let query = format!("connector.table | where {}", condition);
    let result = parse_unwrap!(&query);

    match &result[1] {
        QueryStep::Filter(Expr::Eq(_, right)) => {
            assert_negated_literal(right, expected_value);
        }
        _ => panic!("Expected Eq filter"),
    }
}

#[test]
fn test_negative_literal_in_arithmetic() {
    let query = "connector.table | where result == -5 * 2";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(Expr::Eq(_, right)) => {
            // Should be parsed as (0 - 5) * 2
            match &**right {
                Expr::Mul(left, _right) => {
                    assert_negated_literal(left, 5.0);
                }
                _ => panic!("Expected multiplication expression"),
            }
        }
        _ => panic!("Expected Eq filter"),
    }
}

#[test_case("invalid syntax")]
#[test_case("connector.table | where")]
#[test_case("connector.table | limit -1")]
#[test_case("connector.table | project")]
#[test_case("connector.table | sort by")]
fn test_error_cases(query: &str) {
    let result = parse(query);
    assert!(
        result.is_err(),
        "Expected error for query: {} ({:?})",
        query,
        result
    );
}

#[test]
fn test_keywords_as_identifiers() {
    let query = "connector.table | where in == \"test\"";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(left, _) => match &**left {
                Expr::Field(field) => {
                    assert_eq!(field, &field_unwrap!("in"));
                }
                _ => panic!("Expected Field expression"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_parentheses_in_expressions() {
    let query = "connector.table | where (field1 + field2) * field3 == 100";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(left, _) => match &**left {
                Expr::Mul(inner_left, _) => {
                    assert!(matches!(**inner_left, Expr::Plus(_, _)));
                }
                _ => panic!("Expected multiplication with parenthesized addition"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test_case("datetime()"; "current_time")]
#[test_case("now()"; "now_function")]
#[test_case("datetime(2015-12-31)"; "date_only")]
#[test_case("datetime(2015-12-31 23:59:59)"; "date_with_time")]
#[test_case("datetime(2015-12-31 23:59:59.999)"; "date_with_millis")]
#[test_case("datetime(2015-12-31T23:59:59Z)"; "iso8601_utc")]
#[test_case("datetime(2015-12-31T23:59:59+02:00)"; "iso8601_with_offset")]
#[test_case("datetime(2015-12-31T23:59:59.999Z)"; "iso8601_with_millis")]
#[test_case("datetime(Thu, 31 Dec 2015 23:59:59 GMT)"; "rfc2822")]
#[test_case("datetime(2015-12-31T23:59:59.999+00:00)"; "rfc3339")]
fn test_datetime_parsing(datetime_expr: &str) {
    let query = format!("connector.table | where field1 == {}", datetime_expr);
    let result = parse_unwrap!(&query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(_, right) => match &**right {
                Expr::Literal(Value::Timestamp(_)) => {}
                _ => panic!("Expected datetime to parse to Timestamp"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_datetime_current_time() {
    let query = "connector.table | where field1 == datetime()";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(_, right) => match &**right {
                Expr::Literal(Value::Timestamp(dt)) => {
                    // Should be a reasonable timestamp (after 2020-01-01 and before 2050-01-01).
                    let year_2020 = secs_to_dt(1577836800); // 2020-01-01 00:00:00 UTC
                    let year_2050 = secs_to_dt(2524608000); // 2050-01-01 00:00:00 UTC
                    assert!(
                        dt > &year_2020 && dt < &year_2050,
                        "Timestamp {} not in reasonable range",
                        dt
                    );
                }
                _ => panic!("datetime() should return a Number"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_datetime_null() {
    let query = "connector.table | where field1 == datetime(null)";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(_, right) => match &**right {
                Expr::Literal(Value::Null) => {}
                _ => panic!("datetime(null) should return Null"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test_case("2015-12-31", secs_to_dt(1451520000); "date_only_2015")]
#[test_case("2020-01-01", secs_to_dt(1577836800); "date_only_2020")]
#[test_case("1970-01-01", secs_to_dt(0); "epoch_start")]
fn test_datetime_specific_dates(date_str: &str, expected_dt: OffsetDateTime) {
    let query = format!("connector.table | where field1 == datetime({})", date_str);
    let result = parse_unwrap!(&query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(_, right) => match &**right {
                Expr::Literal(Value::Timestamp(dt)) => {
                    assert_eq!(
                        dt, &expected_dt,
                        "Date {} should convert to {}, got {}",
                        date_str, dt, expected_dt
                    );
                }
                _ => panic!("datetime with string should return Number"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_datetime_with_time() {
    let query = "connector.table | where field1 == datetime(2020-01-01 12:30:45)";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(_, right) => match &**right {
                Expr::Literal(Value::Timestamp(dt)) => {
                    // 2020-01-01 00:00:00 UTC = 1577836800
                    // + 12 hours = 12 * 60 * 60 = 43200
                    // + 30 minutes = 30 * 60 = 1800
                    // + 45 seconds
                    let expected = 1577836800 + 43200 + 1800 + 45;
                    assert_eq!(dt, &secs_to_dt(expected));
                }
                _ => panic!("datetime with time should return Number"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test]
fn test_datetime_with_milliseconds() {
    let query = "connector.table | where field1 == datetime(2020-01-01 00:00:00.500)";
    let result = parse_unwrap!(query);

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::Eq(_, right) => match &**right {
                Expr::Literal(Value::Timestamp(dt)) => {
                    let expected =
                        secs_to_dt(1577836800).saturating_add(Duration::milliseconds(500));
                    assert_eq!(dt, &expected);
                }
                _ => panic!("datetime with milliseconds should return Number"),
            },
            _ => panic!("Expected Eq expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test_case("datetime(invalid)"; "invalid_format")]
#[test_case("datetime(2020-13-01)"; "invalid_month")]
#[test_case("datetime(2020-01-32)"; "invalid_day")]
#[test_case("datetime(2020-01-01 25:00:00)"; "invalid_hour")]
#[test_case("datetime(2020-01-01 12:60:00)"; "invalid_minute")]
#[test_case("datetime(2020-01-01 12:30:61)"; "invalid_second")]
fn test_datetime_invalid_formats(datetime_expr: &str) {
    let query = format!("connector.table | where field1 == {}", datetime_expr);
    let result = parse(&query);
    assert!(
        result.is_err(),
        "Expected parse error for invalid datetime format"
    );
}

#[test]
fn test_datetime_in_complex_expression() {
    let query = r#"
            connector.table 
            | where timestamp > datetime(2020-01-01) and timestamp < datetime()
            | project timestamp, age = datetime() - timestamp
        "#;
    let result = parse_unwrap!(query);

    assert_eq!(result.len(), 3);
    assert!(matches!(result[0], QueryStep::Scan(..)));
    assert!(matches!(result[1], QueryStep::Filter(_)));
    assert!(matches!(result[2], QueryStep::Project(_)));

    match &result[1] {
        QueryStep::Filter(expr) => match expr {
            Expr::And(left, right) => match (&**left, &**right) {
                (Expr::Gt(_, dt1), Expr::Lt(_, dt2)) => {
                    assert!(matches!(&**dt1, Expr::Literal(Value::Timestamp(_))));
                    assert!(matches!(&**dt2, Expr::Literal(Value::Timestamp(_))));
                }
                _ => panic!("Expected Gt and Lt expressions with datetime literals"),
            },
            _ => panic!("Expected And expression"),
        },
        _ => panic!("Expected Filter step"),
    }
}

#[test_case(
    "case(x > 10, \"high\", x > 5, \"medium\", \"low\")";
    "basic_case_with_three_clauses"
)]
#[test_case(
    "case(a == 1, \"one\", a == 2, \"two\", \"other\")";
    "integer_comparisons"
)]
#[test_case(
    "case(field1 == true, \"yes\", field1 == false, \"no\", \"unknown\")";
    "boolean_case"
)]
#[test_case(
    "case(field1 > datetime(2020-01-01), \"recent\", field1 > datetime(2010-01-01), \"old\", \"ancient\")";
    "datetime_case"
)]
fn test_case_expression(expr_str: &str) {
    let query = format!("connector.table | extend result = {}", expr_str);
    let result = parse_unwrap!(&query);

    match &result[1] {
        QueryStep::Extend(fields) => {
            assert_eq!(fields.len(), 1, "Expected one field in extend");
            match &fields[0].from {
                Expr::Case(arms, else_expr) => {
                    assert!(
                        !arms.is_empty(),
                        "Expected at least one (predicate, then) pair"
                    );

                    for (pred, then_expr) in arms {
                        assert!(
                            matches!(
                                pred,
                                Expr::Gt(_, _)
                                    | Expr::Lt(_, _)
                                    | Expr::Eq(_, _)
                                    | Expr::Ne(_, _)
                                    | Expr::Gte(_, _)
                                    | Expr::Lte(_, _)
                                    | Expr::Literal(_)
                                    | Expr::Field(_)
                            ),
                            "Invalid predicate expression in CASE: {:?}",
                            pred
                        );
                        assert!(
                            matches!(then_expr, Expr::Literal(_)),
                            "THEN expression must be a literal, got: {:?}",
                            then_expr
                        );
                    }

                    assert!(
                        matches!(&**else_expr, Expr::Literal(_)),
                        "Expected else expression to be a literal"
                    );
                }
                other => panic!("Expected Expr::Case, got {:?}", other),
            }
        }
        _ => panic!("Expected Extend step"),
    }
}

#[test]
fn test_parse_error_includes_line_and_column() {
    let query = "connector.table | where field1 == \"value\"\n| invalid_operator field2";
    let result = parse(query);

    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(!errors.is_empty());
    assert_eq!(
        errors
            .into_iter()
            .map(|e| (e.line, e.column))
            .collect::<Vec<_>>(),
        vec![(2, 3)]
    );
}

#[test]
fn test_join_condition_validation() {
    let query = "connector.table | join kind=inner (other.table) on $left.field1 == $left.field2";
    let result = parse(query);

    assert!(result.is_err());
    let errors = result.unwrap_err();

    let has_join_error = errors
        .iter()
        .any(|e| e.message.contains("$left") && e.message.contains("$right"));
    assert!(has_join_error, "Should have error about join condition");
}

#[test_case(
    "connector.table1 | join (connector.table2 | where) on $left. == $right.field2 | project field1",
    3;
    "malformed join subquery and condition"
)]
#[test_case(
    "connector.table | summarize invalid_agg =, sum_field = sum(field1) by field2",
    1;
    "malformed aggregation expressions"
)]
#[test_case(
    "connector.table | where field1 ++ field2 ** field3 -- field4 == 1",
    3;
    "invalid operators"
)]
// TODO:
// #[test_case(
//     "connector.table | where | project | limit invalid | sort by field1",
//     3;
//     "multiple malformed query steps"
// )]
// #[test_case(
//     "connector.table | where field1 == && field2 == 42 || field3 == \"test\"",
//     1;
//     "malformed expression with missing operand"
// )]
// #[test_case(
//     "connector.table | where exists( | project field1, field2 | where tostring(field3 == 5",
//     2;
//     "malformed function calls"
// )]
// #[test_case(
//     "connector.table | project field1 = field2 +, invalid_field =, field3 = field4 * 2",
//     2;
//     "mixed valid and invalid project expressions"
// )]
// #[test_case(
//     "connector.table | where ((field1 + field2) * (field3 +)) && field4 == 1",
//     1;
//     "malformed nested parentheses"
// )]
// #[test_case(
//     "connector.table | invalid_step syntax | where | project = | limit abc | sort by | count",
//     3;
//     "completely malformed pipeline"
// )]
// #[test_case(
//     "connector.table | where field1 in (1, 2, invalid +, 4, broken syntax, 6)",
//     2;
//     "malformed expressions in list"
// )]
fn test_error_recovery_collects_multiple_errors(query: &str, expected_num_errors: usize) {
    let result = parse(query);
    dbg!(&result);

    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert_eq!(
        expected_num_errors,
        errors.len(),
        "Expected {} errors, got {}. Errors: {:?}",
        expected_num_errors,
        errors.len(),
        errors
    );
}
