use serde::{Deserialize, Serialize};

use crate::connector::Log;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum ProjectField {
    Name(String),
    Rename(/*from=*/ String, /*to=*/ String),
}

pub fn apply_project_fields(fields: &[ProjectField], mut input_log: Log) -> Log {
    let mut output_log = Log::new();
    for field in fields {
        match field {
            ProjectField::Name(name) => {
                if let Some((key, value)) = input_log.remove_entry(name) {
                    output_log.insert(key, value);
                }
            }
            ProjectField::Rename(from, to) => {
                if let Some(value) = input_log.remove(from) {
                    output_log.insert(to.to_string(), value);
                }
            }
        }
    }
    output_log
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn apply_project_fields_sanity() -> std::io::Result<()> {
        let project_fields_raw = r#"[
            "name",
            ["from", "to"]
        ]"#;
        let project_fields: Vec<ProjectField> = serde_json::from_str(project_fields_raw)?;

        let log_raw = r#"{
            "name": 123,
            "name2": 133,
            "from": 100
        }"#;
        let log: Log = serde_json::from_str(log_raw)?;

        let result = apply_project_fields(&project_fields, log);
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("name"), Some(&json!(123)));
        assert_eq!(result.get("to"), Some(&json!(100)));
        Ok(())
    }
}
