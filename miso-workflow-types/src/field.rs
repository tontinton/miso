use std::{fmt, ops::Deref, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize};

#[macro_export]
macro_rules! field_unwrap {
    ($expr:expr) => {
        Field::from_str($expr)
            .unwrap_or_else(|e| panic!("failed to parse field {:?}: {}", $expr, e))
    };
}

#[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct FieldAccess {
    pub name: String,

    /// When set, it means access field as name[idx0][idx1].
    pub arr_indices: Vec<usize>,
}

impl FieldAccess {
    pub fn new(name: String, arr_indices: Vec<usize>) -> Self {
        Self { name, arr_indices }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FieldAccessParseError {
    #[error("missing closing bracket ']'")]
    MissingClosingBracket,
    #[error("invalid index value: {0}")]
    InvalidIndex(String),
    #[error("empty field name")]
    EmptyFieldName,
}

impl FromStr for FieldAccess {
    type Err = FieldAccessParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(FieldAccessParseError::EmptyFieldName);
        }

        let Some(bracket_start) = s.find('[') else {
            return Ok(Self {
                name: s.to_string(),
                arr_indices: vec![],
            });
        };

        let field_name = &s[..bracket_start];
        if field_name.is_empty() {
            return Err(FieldAccessParseError::EmptyFieldName);
        }

        let bracket_part = &s[bracket_start..];
        let mut arr_indices = Vec::new();
        let mut chars = bracket_part.chars().peekable();

        while chars.peek() == Some(&'[') {
            chars.next(); // skip '['
            let mut index_str = String::new();

            for ch in chars.by_ref() {
                if ch == ']' {
                    break;
                }
                index_str.push(ch);
            }

            // if we didn't break at ']', then it's missing
            if !index_str.is_empty() && chars.peek().is_none() && !bracket_part.ends_with(']') {
                return Err(FieldAccessParseError::MissingClosingBracket);
            }

            let index = index_str
                .parse::<usize>()
                .map_err(|_| FieldAccessParseError::InvalidIndex(index_str.clone()))?;

            arr_indices.push(index);
        }

        Ok(Self {
            name: field_name.to_string(),
            arr_indices,
        })
    }
}

impl fmt::Display for FieldAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        for &idx in &self.arr_indices {
            write!(f, "[{idx}]")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct Field(Vec<FieldAccess>);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FieldParseError {
    #[error("invalid field access at segment '{0}': {1}")]
    InvalidSegment(String, FieldAccessParseError),
}

impl Field {
    pub fn new(access: Vec<FieldAccess>) -> Self {
        Self(access)
    }

    /// Returns a new Field with the given suffix appended to the last segment's name.
    pub fn with_suffix(self, suffix: &str) -> Self {
        let mut new_field = self;
        if let Some(last) = new_field.0.last_mut() {
            last.name.push_str(suffix);
        }
        new_field
    }

    pub fn has_array_access(&self) -> bool {
        self.0.iter().any(|x| !x.arr_indices.is_empty())
    }
}

impl FromStr for Field {
    type Err = FieldParseError;

    fn from_str(field: &str) -> Result<Self, Self::Err> {
        let mut accesses = Vec::new();

        for part in field.split('.') {
            let fa = FieldAccess::from_str(part)
                .map_err(|e| FieldParseError::InvalidSegment(part.to_string(), e))?;
            accesses.push(fa);
        }

        Ok(Self(accesses))
    }
}

impl Deref for Field {
    type Target = Vec<FieldAccess>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut iter = self.0.iter();

        if let Some(first) = iter.next() {
            write!(f, "{first}")?;
            for access in iter {
                write!(f, ".{access}")?;
            }
        }

        Ok(())
    }
}

impl Serialize for Field {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse::<Field>().map_err(serde::de::Error::custom)
    }
}

impl From<&Field> for String {
    fn from(f: &Field) -> Self {
        f.to_string()
    }
}
