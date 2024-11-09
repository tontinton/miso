use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum TransformAst {
    Value(String),
    Field(String),

    #[serde(rename = "*")]
    Mul(Box<TransformAst>, Box<TransformAst>),
    #[serde(rename = "/")]
    Div(Box<TransformAst>, Box<TransformAst>),
    #[serde(rename = "+")]
    Plus(Box<TransformAst>, Box<TransformAst>),
    #[serde(rename = "-")]
    Minus(Box<TransformAst>, Box<TransformAst>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectField {
    from: TransformAst,
    to: String,
}

fn transform_binop_ast_to_vrl(left: &TransformAst, right: &TransformAst, op: &str) -> String {
    format!(
        "({} {} {})",
        transform_ast_to_vrl(left, true),
        op,
        transform_ast_to_vrl(right, true)
    )
}

fn transform_ast_to_vrl(ast: &TransformAst, binop: bool) -> String {
    match ast {
        TransformAst::Value(value) => value.to_string(),
        TransformAst::Field(name) if binop => format!("to_int!(.{name})"),
        TransformAst::Field(name) => format!(".{name}"),
        TransformAst::Mul(left, right) => transform_binop_ast_to_vrl(left, right, "*"),
        TransformAst::Div(left, right) => transform_binop_ast_to_vrl(left, right, "/"),
        TransformAst::Plus(left, right) => transform_binop_ast_to_vrl(left, right, "+"),
        TransformAst::Minus(left, right) => transform_binop_ast_to_vrl(left, right, "-"),
    }
}

pub fn project_fields_to_vrl(fields: &[ProjectField]) -> String {
    let mut items = Vec::with_capacity(fields.len());
    for field in fields {
        items.push(format!(
            "\"{}\": {}",
            field.to,
            transform_ast_to_vrl(&field.from, false)
        ));
    }
    format!(". = {{{}}}", items.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_fields_to_vrl_sanity() -> std::io::Result<()> {
        let project_fields_raw = r#"[
            {
                "from": {
                    "*": [{"field": "name"}, {"value": "100"}]
                },
                "to": "test1"
            },
            {
                "from": {
                    "+": [{"field": "left"}, {"field": "right"}]
                },
                "to": "test2"
            }
        ]"#;
        let project_fields: Vec<ProjectField> = serde_json::from_str(project_fields_raw)?;
        let result = project_fields_to_vrl(&project_fields);
        assert_eq!(
            result,
            r#". = {"test1": (to_int!(.name) * 100), "test2": (to_int!(.left) + to_int!(.right))}"#
        );
        Ok(())
    }
}
