use std::str::FromStr;

use crate::{
    field::{Field, FieldAccess},
    field_unwrap,
};

#[test]
fn test_fieldaccess_from_str_no_array() {
    let fa = FieldAccess::from_str("foo").expect("field access");
    assert_eq!(fa.name, "foo");
    assert_eq!(fa.arr_indices, Vec::<usize>::new());
    assert_eq!(fa.to_string(), "foo");
}

#[test]
fn test_fieldaccess_from_str_with_array() {
    let fa = FieldAccess::from_str("foo[0][42]").expect("field access");
    assert_eq!(fa.name, "foo");
    assert_eq!(fa.arr_indices, vec![0, 42]);
    assert_eq!(fa.to_string(), "foo[0][42]");
}

#[test]
fn test_field_from_str_simple() {
    let field = field_unwrap!("foo.bar");
    assert_eq!(field.len(), 2);
    assert_eq!(field[0].name, "foo");
    assert_eq!(field[1].name, "bar");
    assert_eq!(field.to_string(), "foo.bar");
    assert!(!field.has_array_access());
}

#[test]
fn test_field_from_str_with_arrays() {
    let field = field_unwrap!("foo[1].bar[2][3].baz");
    assert_eq!(field.len(), 3);
    assert_eq!(field[0].name, "foo");
    assert_eq!(field[0].arr_indices, vec![1]);
    assert_eq!(field[1].name, "bar");
    assert_eq!(field[1].arr_indices, vec![2, 3]);
    assert_eq!(field[2].name, "baz");
    assert!(field.has_array_access());
    assert_eq!(field.to_string(), "foo[1].bar[2][3].baz");
}

#[test]
fn test_serde_roundtrip() {
    let original = field_unwrap!("foo[1].bar.baz[0]");
    let json_str = serde_json::to_string(&original).unwrap();
    assert_eq!(json_str, "\"foo[1].bar.baz[0]\"");

    let deserialized: Field = serde_json::from_str(&json_str).unwrap();
    assert_eq!(original, deserialized);
}
