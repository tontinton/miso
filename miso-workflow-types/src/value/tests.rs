use crate::value::{Map, Value};

#[test]
fn test_null() {
    assert_eq!(json!(null), Value::Null);
}

#[test]
fn test_boolean() {
    assert_eq!(json!(true), Value::Bool(true));
    assert_eq!(json!(false), Value::Bool(false));
}

#[test]
fn test_integer() {
    assert_eq!(json!(42), Value::Int(42));
    assert_eq!(json!(-10), Value::Int(-10));
}

#[test]
fn test_float() {
    assert_eq!(json!(3.17), Value::Float(3.17));
    assert_eq!(json!(-2.5), Value::Float(-2.5));
}

#[test]
fn test_string() {
    assert_eq!(json!("hello"), Value::String("hello".to_string()));

    let s = "world".to_string();
    assert_eq!(json!(s.clone()), Value::String(s));
}

#[test]
fn test_array() {
    assert_eq!(
        json!([1, 2, 3]),
        Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
    );

    assert_eq!(
        json!([true, "hello", 42]),
        Value::Array(vec![
            Value::Bool(true),
            Value::String("hello".to_string()),
            Value::Int(42)
        ])
    );

    assert_eq!(
        json!([[1, 2], [3, 4]]),
        Value::Array(vec![
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
            Value::Array(vec![Value::Int(3), Value::Int(4)])
        ])
    );
}

#[test]
fn test_object() {
    let expected = {
        let mut map = Map::new();
        map.insert("name".to_string(), Value::String("John".to_string()));
        map.insert("age".to_string(), Value::Int(30));
        Value::Object(map)
    };

    assert_eq!(json!({ "name": "John", "age": 30 }), expected);
}

#[test]
fn test_nested_object() {
    let expected = {
        let mut inner_map = Map::new();
        inner_map.insert(
            "street".to_string(),
            Value::String("123 Main St".to_string()),
        );
        inner_map.insert("city".to_string(), Value::String("Anytown".to_string()));

        let mut outer_map = Map::new();
        outer_map.insert("name".to_string(), Value::String("John".to_string()));
        outer_map.insert("address".to_string(), Value::Object(inner_map));

        Value::Object(outer_map)
    };

    assert_eq!(
        json!({
            "name": "John",
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
            }
        }),
        expected
    );
}

#[test]
fn test_complex_structure() {
    let result = json!({
        "users": [
            {
                "id": 1,
                "name": "Alice",
                "active": true
            },
            {
                "id": 2,
                "name": "Bob",
                "active": false
            }
        ],
        "total": 2,
        "metadata": null
    });

    match result {
        Value::Object(map) => {
            assert_eq!(map.len(), 3);
            assert!(map.contains_key("users"));
            assert!(map.contains_key("total"));
            assert!(map.contains_key("metadata"));
            if let Some(Value::Array(users)) = map.get("users") {
                assert_eq!(users.len(), 2);
            }
            assert_eq!(map.get("total"), Some(&Value::Int(2)));
            assert_eq!(map.get("metadata"), Some(&Value::Null));
        }
        _ => panic!("Expected object"),
    }
}

#[test]
fn test_variables() {
    let name = "Alice";
    let age = 25;
    let active = true;

    let result = json!({
        "name": name,
        "age": age * 5,
        "active": active,
    });

    let expected = {
        let mut map = Map::new();
        map.insert("name".to_string(), Value::String("Alice".to_string()));
        map.insert("age".to_string(), Value::Int(125));
        map.insert("active".to_string(), Value::Bool(true));
        Value::Object(map)
    };

    assert_eq!(result, expected);
}
