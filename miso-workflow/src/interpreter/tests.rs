use std::str::FromStr;

use miso_workflow_types::{field::Field, field_unwrap, json, log::Log, value::Value};

use crate::interpreter::{get_field_value, insert_field_value};

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
