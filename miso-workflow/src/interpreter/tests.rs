use std::str::FromStr;

use miso_workflow_types::{field::Field, field_unwrap, json, log::Log, value::Value};

use crate::interpreter::{get_field_value, insert_field_value, string_ops};

#[test]
fn get_field_value_simple() {
    let mut log = Log::new();
    log.insert("foo".to_string(), json!(123));

    let f = field_unwrap!("foo");
    assert_eq!(get_field_value(&log, &f), Some(&json!(123)));
}

#[test]
fn get_field_value_nested_object() {
    let mut log = Log::new();
    log.insert("outer".to_string(), json!({ "inner": 42 }));

    let f = field_unwrap!("outer.inner");
    assert_eq!(get_field_value(&log, &f), Some(&json!(42)));
}

#[test]
fn get_field_value_array_index() {
    let mut log = Log::new();
    log.insert("items".to_string(), json!([10, 20, 30]));

    let f = field_unwrap!("items[1]");
    assert_eq!(get_field_value(&log, &f), Some(&json!(20)));
}

#[test]
fn get_field_value_mixed_object_array() {
    let mut log = Log::new();
    log.insert("outer".to_string(), json!({ "list": [ {"x": 5} ] }));

    let f = field_unwrap!("outer.list[0].x");
    assert_eq!(get_field_value(&log, &f), Some(&json!(5)));
}

#[test]
fn get_field_value_invalid_path() {
    let log = Log::new();
    let f = field_unwrap!("missing");
    assert_eq!(get_field_value(&log, &f), None);
}

#[test]
fn insert_field_value_creates_path() {
    let mut log = Log::new();
    let f = field_unwrap!("outer.inner");
    insert_field_value(&mut log, &f, json!(99));

    assert_eq!(get_field_value(&log, &f), Some(&json!(99)));
}

#[test]
fn insert_field_value_creates_array() {
    let mut log = Log::new();
    let f = field_unwrap!("items[2]");
    insert_field_value(&mut log, &f, json!("third"));

    assert_eq!(get_field_value(&log, &f), Some(&json!("third")));
    assert_eq!(
        get_field_value(&log, &field_unwrap!("items[0]")),
        Some(&Value::Null)
    );
}

#[test]
fn insert_field_value_overwrites_existing() {
    let mut log = Log::new();
    log.insert("key".to_string(), json!("old"));

    let f = field_unwrap!("key");
    insert_field_value(&mut log, &f, json!("new"));

    assert_eq!(get_field_value(&log, &f), Some(&json!("new")));
}

#[test]
fn insert_field_value_mixed_object_array_path() {
    let mut log = Log::new();
    let f = field_unwrap!("outer.list[1].x");
    insert_field_value(&mut log, &f, json!(123));

    assert_eq!(get_field_value(&log, &f), Some(&json!(123)));
}

#[test]
fn roundtrip_insert_and_get() {
    let mut log = Log::new();
    let f = field_unwrap!("alpha.beta[2].gamma");

    insert_field_value(&mut log, &f, json!(true));
    assert_eq!(get_field_value(&log, &f), Some(&json!(true)));
}

#[test]
fn test_extract_basic() {
    // extract("code: (\\d+)", 1, "error code: 123") -> "123"
    let result = string_ops::extract("error code: 123", "code: (\\d+)", 1).unwrap();
    assert_eq!(result, Some(Value::String("123".to_string())));
}

#[test]
fn test_extract_no_match() {
    // extract("(\\d+)", 1, "no numbers") -> null
    let result = string_ops::extract("no numbers", "(\\d+)", 1).unwrap();
    assert_eq!(result, Some(Value::Null));
}

#[test]
fn test_extract_invalid_group() {
    // extract("(\\d+)", 5, "123") -> null (group doesn't exist)
    let result = string_ops::extract("123", "(\\d+)", 5).unwrap();
    assert_eq!(result, Some(Value::Null));
}

#[test]
fn test_extract_group_zero() {
    // extract("\\d+", 0, "abc 123 def") -> "123" (entire match)
    let result = string_ops::extract("abc 123 def", "\\d+", 0).unwrap();
    assert_eq!(result, Some(Value::String("123".to_string())));
}

#[test]
fn test_extract_multiple_groups() {
    // Pattern with multiple groups, extract group 2
    let result = string_ops::extract("user=john, id=42", "user=(\\w+), id=(\\d+)", 2).unwrap();
    assert_eq!(result, Some(Value::String("42".to_string())));
}

#[test]
fn test_extract_negative_group() {
    // Negative group should error
    let result = string_ops::extract("test", "(\\w+)", -1);
    assert!(result.is_err());
}

#[test]
fn test_extract_invalid_regex() {
    // Invalid regex should error
    let result = string_ops::extract("test", "([", 1);
    assert!(result.is_err());
}
