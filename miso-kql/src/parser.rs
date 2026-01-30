use std::{fmt, str::FromStr};

use chumsky::{
    input::{Stream, ValueInput},
    prelude::*,
};
use hashbrown::{HashMap, HashSet};
use logos::Logos;
use miso_workflow_types::{
    expand::{Expand, ExpandKind},
    expr::{CastType, Expr},
    field::{Field, FieldAccess},
    join::{Join, JoinType},
    json,
    project::ProjectField,
    query::{QueryStep, ScanKind},
    sort::{NullsOrder, Sort, SortOrder},
    summarize::{Aggregation, ByField, Summarize},
    value::Value,
};
use serde::Serialize;
use time::OffsetDateTime;

use crate::lexer::{StringValue, Token};

const ERROR_FIELD_NAME: &str = "_error";

const EXPR_TERMINATORS: &[Token] = &[
    Token::Pipe,
    Token::Comma,
    Token::RParen,
    Token::RBracket,
    Token::And,
    Token::Or,
];

macro_rules! binary_ops {
    ($prev:expr; $( $token:path => $expr:path ),* $(,)?) => {
        $prev
            .clone()
            .then(
                choice((
                    $( just($token).then(
                        $prev.clone().recover_with(via_parser(
                            one_of(EXPR_TERMINATORS)
                                .rewind()
                                .validate(|_, e, emitter| {
                                    emitter.emit(Rich::custom(
                                        e.span(),
                                        "expected expression after operator",
                                    ));
                                    Expr::Literal(Value::Null)
                                })
                        ))
                    ), )*
                ))
                .repeated()
                .collect::<Vec<_>>(),
            )
            .map(|(left, ops)| {
                ops.into_iter().fold(left, |acc, (op, right)| {
                    match op {
                        $( $token => $expr(Box::new(acc), Box::new(right)), )*
                        _ => unreachable!(),
                    }
                })
            })
            .boxed()
    };
}

macro_rules! agg_zero_param {
    ($token:ident => $variant:ident) => {
        just(Token::$token)
            .ignore_then(just(Token::LParen))
            .ignore_then(just(Token::RParen))
            .to(Aggregation::$variant)
    };
}

macro_rules! agg_single_param {
    ($field:ident; $token:ident => $variant:ident) => {
        just(Token::$token)
            .ignore_then(
                $field
                    .clone()
                    .delimited_by(just(Token::LParen), just(Token::RParen))
                    .recover_with(via_parser(nested_delimiters(
                        Token::LParen,
                        Token::RParen,
                        [(Token::LBracket, Token::RBracket)],
                        |_| Field::from_str(ERROR_FIELD_NAME).expect("error field"),
                    ))),
            )
            .map(Aggregation::$variant)
    };
}

#[derive(Debug, Serialize)]
pub struct ParseError {
    pub span: std::ops::Range<usize>,
    pub line: usize,
    pub column: usize,
    pub message: String,
    pub reason: String,
}

impl std::error::Error for ParseError {}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Parse error at {}:{}: {}",
            self.line, self.column, self.message
        )?;
        Ok(())
    }
}

/// Convert a byte offset in the source to line and column numbers (1-indexed).
fn offset_to_line_col(source: &str, offset: usize) -> (usize, usize) {
    let mut line = 1;
    let mut col = 1;

    for (i, ch) in source.chars().enumerate() {
        if i >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            col = 1;
        } else {
            col += 1;
        }
    }

    (line, col)
}

pub fn parse(query: &str) -> Result<Vec<QueryStep>, Vec<ParseError>> {
    let token_iter = Token::lexer(query).spanned().map(|(tok, span)| match tok {
        Ok(tok) => (tok, span.into()),
        Err(()) => (Token::Error, span.into()),
    });

    let token_stream =
        Stream::from_iter(token_iter).map((0..query.len()).into(), |(t, s): (_, _)| (t, s));

    match query_parser().parse(token_stream).into_result() {
        Ok(expr) => Ok(expr),
        Err(errs) => {
            let parse_errors: Vec<ParseError> = errs
                .into_iter()
                .map(|err| {
                    let span = err.span().into_range();
                    let (line, column) = offset_to_line_col(query, span.start);
                    ParseError {
                        span,
                        line,
                        column,
                        message: format!("{err:?}"),
                        reason: format!("{:?}", err.reason()),
                    }
                })
                .collect();
            Err(parse_errors)
        }
    }
}

fn ident_parser<'a, I>() -> impl Parser<'a, I, String, extra::Err<Rich<'a, Token>>> + Clone
where
    I: ValueInput<'a, Token = Token, Span = SimpleSpan>,
{
    select! {
        Token::Ident(name) => name,

        // Should figure out a better way than this...
        Token::In => "in".to_string(),
        Token::Contains => "contains".to_string(),
        Token::StartsWith => "startswith".to_string(),
        Token::EndsWith => "endswith".to_string(),
        Token::Has => "has".to_string(),
        Token::HasCs => "has_cs".to_string(),
        Token::Datetime => "datetime".to_string(),
        Token::Now => "now".to_string(),
        Token::ToString => "tostring".to_string(),
        Token::ToInt => "toint".to_string(),
        Token::ToLong => "tolong".to_string(),
        Token::ToReal => "toreal".to_string(),
        Token::ToDecimal => "todecimal".to_string(),
        Token::ToBool => "tobool".to_string(),
        Token::Null => "null".to_string(),
        Token::Hint => "hint".to_string(),
        Token::Partitions => "partitions".to_string(),
        Token::Exists => "exists".to_string(),
        Token::By => "by".to_string(),
        Token::Asc => "asc".to_string(),
        Token::Desc => "desc".to_string(),
        Token::Nulls => "nulls".to_string(),
        Token::First => "first".to_string(),
        Token::Last => "last".to_string(),
        Token::Where => "where".to_string(),
        Token::Filter => "filter".to_string(),
        Token::Project => "project".to_string(),
        // ProjectRename left out on purpose as it includes a '-' which is not a valid character
        // in ident.
        Token::Extend => "extend".to_string(),
        Token::Limit => "limit".to_string(),
        Token::Take => "take".to_string(),
        Token::Sort => "sort".to_string(),
        Token::Order => "order".to_string(),
        Token::Top => "top".to_string(),
        Token::Summarize => "summarize".to_string(),
        Token::Distinct => "distinct".to_string(),
        Token::Join => "join".to_string(),
        Token::Kind => "kind".to_string(),
        Token::Inner => "inner".to_string(),
        Token::Outer => "outer".to_string(),
        Token::Left => "left".to_string(),
        Token::Right => "right".to_string(),
        Token::On => "on".to_string(),
        Token::Bag => "bag".to_string(),
        Token::Array => "array".to_string(),
        Token::Union => "union".to_string(),
        Token::Count => "count".to_string(),
        Token::Tee => "tee".to_string(),
        Token::Countif => "countif".to_string(),
        Token::DCount => "dcount".to_string(),
        Token::Sum => "sum".to_string(),
        Token::Min => "min".to_string(),
        Token::Max => "max".to_string(),
        Token::Avg => "avg".to_string(),
        Token::Bin => "bin".to_string(),
        Token::Let => "let".to_string(),
        Token::Between => "between".to_string(),
        Token::Case => "case".to_string(),
    }
}

fn field_parser<'a, I>(
    allow_array: bool,
) -> impl Parser<'a, I, Field, extra::Err<Rich<'a, Token>>> + Clone
where
    I: ValueInput<'a, Token = Token, Span = SimpleSpan>,
{
    let top_level = just(Token::At)
        .or_not()
        .then(ident_parser())
        .map(|(at, name)| {
            if at.is_some() {
                format!("@{name}")
            } else {
                name
            }
        })
        .boxed();

    let single_field = if allow_array {
        top_level
            .then(
                just(Token::LBracket)
                    .ignore_then(
                        select! {
                            Token::Integer(i) => i
                        }
                        .validate(|i, e, emitter| {
                            if i < 0 {
                                emitter.emit(Rich::custom(
                                    e.span(),
                                    "array index must be non-negative. Use field[0], field[1], etc.",
                                ));
                                0
                            } else {
                                i as usize
                            }
                        }),
                    )
                    .then_ignore(just(Token::RBracket))
                    .repeated()
                    .collect::<Vec<_>>()
                    .labelled("array index"),
            )
            .map(|(name, arr_indices)| FieldAccess::new(name, arr_indices))
            .boxed()
    } else {
        top_level.map(|name| FieldAccess::new(name, vec![])).boxed()
    };

    let first = single_field.clone();
    let rest = just(Token::Dot)
        .ignore_then(single_field)
        .repeated()
        .collect::<Vec<_>>();

    first
        .then(rest)
        .map(|(first, mut rest)| {
            rest.insert(0, first);
            Field::new(rest)
        })
        .labelled("field")
        .boxed()
}

fn now_expr() -> Expr {
    Expr::Literal(json!(OffsetDateTime::now_utc()))
}

fn datetime_parser<'a, I>() -> impl Parser<'a, I, Expr, extra::Err<Rich<'a, Token>>> + Clone
where
    I: ValueInput<'a, Token = Token, Span = SimpleSpan>,
{
    let datetime = just(Token::Datetime)
        .ignore_then(
            // datetime() - current time.
            just(Token::LParen)
                .ignore_then(just(Token::RParen))
                .map(|_| now_expr())
                .or(
                    // datetime(null) - null value.
                    just(Token::LParen)
                        .ignore_then(just(Token::Null))
                        .then_ignore(just(Token::RParen))
                        .map(|_| Expr::Literal(Value::Null)),
                )
                .or(
                    // datetime(year.month.day hour:minute:second.milliseconds)
                    // or datetime(year.month.day).
                    just(Token::LParen)
                        .ignore_then(
                            select! { Token::DatetimeLiteral(x) => Expr::Literal(Value::from(x)) },
                        )
                        .then_ignore(just(Token::RParen))
                        .boxed(),
                )
                .recover_with(via_parser(nested_delimiters(
                    Token::LParen,
                    Token::RParen,
                    [(Token::LBracket, Token::RBracket)],
                    |_| Expr::Literal(Value::Null),
                ))),
        )
        .labelled("datetime")
        .boxed();

    let now = just(Token::Now)
        .ignore_then(just(Token::LParen))
        .ignore_then(just(Token::RParen))
        .map(|_| now_expr())
        .recover_with(via_parser(nested_delimiters(
            Token::LParen,
            Token::RParen,
            [(Token::LBracket, Token::RBracket)],
            |_| Expr::Literal(Value::Null),
        )))
        .labelled("now")
        .boxed();

    datetime.or(now).labelled("datetime literal").boxed()
}

fn expr_parser<'a, I>() -> impl Parser<'a, I, Expr, extra::Err<Rich<'a, Token>>> + Clone
where
    I: ValueInput<'a, Token = Token, Span = SimpleSpan>,
{
    recursive(|expr| {
        let string_literal = select! {
            Token::String(x) => x
        }
        .validate(|val, e, emitter| match val {
            StringValue::Text(text) => text,
            StringValue::Bytes(_) => {
                emitter.emit(Rich::custom(
                    e.span(),
                    "byte strings are currently not supported. Use regular strings with double quotes",
                ));
                String::new()
            }
        })
        .labelled("string literal")
        .boxed();

        let datetime = datetime_parser();

        let literal = select! {
            Token::Integer(x) => {
                Expr::Literal(json!(x))
            },
            Token::Float(x) => Expr::Literal(json!(x)),
            Token::Bool(x) => Expr::Literal(json!(x)),
            Token::Timespan(x) => Expr::Literal(json!(x)),
            Token::Null => Expr::Literal(Value::Null),
        }
        .or(string_literal.map(|x| Expr::Literal(json!(x))))
        .labelled("literal")
        .boxed();

        let field = field_parser(true);

        let exists = just(Token::Exists)
            .ignore_then(
                field
                    .clone()
                    .delimited_by(just(Token::LParen), just(Token::RParen))
                    .recover_with(via_parser(nested_delimiters(
                        Token::LParen,
                        Token::RParen,
                        [(Token::LBracket, Token::RBracket)],
                        |_| Field::from_str(ERROR_FIELD_NAME).expect("error field"),
                    ))),
            )
            .map(Expr::Exists)
            .labelled("exists")
            .boxed();

        let case = just(Token::Case)
            .ignore_then(
                expr.clone()
                    .separated_by(just(Token::Comma))
                    .collect::<Vec<_>>()
                    .delimited_by(just(Token::LParen), just(Token::RParen))
                    .recover_with(via_parser(nested_delimiters(
                        Token::LParen,
                        Token::RParen,
                        [(Token::LBracket, Token::RBracket)],
                        |_| vec![],
                    ))),
            )
            .validate(|mut items, e, emitter| {
                if items.len() < 3 {
                    emitter.emit(Rich::custom(
                        e.span(),
                        "case() requires at least 3 arguments: case(condition, result, defaultResult). Example: case(x > 10, \"high\", \"low\")",
                    ));
                    return Expr::Literal(Value::Null);
                }

                let else_expr = items.pop().unwrap();
                let mut pairs = Vec::new();
                let mut iter = items.into_iter();

                while let (Some(pred), Some(then)) = (iter.next(), iter.next()) {
                    pairs.push((pred, then));
                }

                if iter.next().is_some() {
                    emitter.emit(Rich::custom(
                        e.span(),
                        "case() needs pairs of (condition, result) before the default. Example: case(x > 10, \"high\", x > 5, \"medium\", \"low\")",
                    ));
                }

                Expr::Case(pairs, Box::new(else_expr))
            })
            .labelled("case")
            .boxed();

        let bin = just(Token::Bin)
            .ignore_then(
                expr.clone()
                    .then_ignore(just(Token::Comma))
                    .then(expr.clone())
                    .delimited_by(just(Token::LParen), just(Token::RParen))
                    .recover_with(via_parser(nested_delimiters(
                        Token::LParen,
                        Token::RParen,
                        [(Token::LBracket, Token::RBracket)],
                        |_| (Expr::Literal(Value::Null), Expr::Literal(Value::Null)),
                    ))),
            )
            .map(|(l, r)| Expr::Bin(Box::new(l), Box::new(r)))
            .labelled("bin")
            .boxed();

        let expr_delimited_by_parentheses = expr
            .clone()
            .delimited_by(just(Token::LParen), just(Token::RParen))
            .recover_with(via_parser(nested_delimiters(
                Token::LParen,
                Token::RParen,
                [(Token::LBracket, Token::RBracket)],
                |_| Expr::Literal(Value::Null),
            )))
            .labelled("parentheses over an expression");

        let not = just(Token::Not)
            .ignore_then(expr_delimited_by_parentheses.clone())
            .map(|expr| Expr::Not(Box::new(expr.clone())))
            .labelled("not")
            .boxed();

        let negate = just(Token::Minus)
            .ignore_then(literal.clone())
            .map(|expr| Expr::Minus(Box::new(Expr::Literal(json!(0))), Box::new(expr)))
            .labelled("negate")
            .boxed();

        let cast = select! {
            Token::ToString => CastType::String,
            Token::ToInt | Token::ToLong => CastType::Int,
            Token::ToReal | Token::ToDecimal => CastType::Float,
            Token::ToBool => CastType::Bool,
        }
        .then(expr_delimited_by_parentheses.clone())
        .map(|(cast, e)| Expr::Cast(cast, Box::new(e)))
        .labelled("cast")
        .boxed();

        let atom = literal
            .or(expr_delimited_by_parentheses)
            .or(bin)
            .or(not)
            .or(negate)
            .or(cast)
            .or(exists)
            .or(case)
            .or(datetime)
            // Must be last (fields can contain tokens that are keywords).
            .or(field.map(Expr::Field))
            .recover_with(skip_then_retry_until(
                any().ignored(),
                one_of(EXPR_TERMINATORS).ignored(),
            ))
            .boxed();

        let mul_div = binary_ops!(atom;
            Token::Mul => Expr::Mul,
            Token::Div => Expr::Div,
        )
        .labelled("multiplication or division");

        let add_sub = binary_ops!(mul_div;
            Token::Plus  => Expr::Plus,
            Token::Minus => Expr::Minus,
        )
        .labelled("addition or subtraction");

        let comparisons = binary_ops!(add_sub;
            Token::DoubleEq => Expr::Eq,
            Token::Ne       => Expr::Ne,
            Token::Gte      => Expr::Gte,
            Token::Gt       => Expr::Gt,
            Token::Lte      => Expr::Lte,
            Token::Lt       => Expr::Lt,
        )
        .labelled("comparison");

        let text_ops = binary_ops!(comparisons;
            Token::StartsWith => Expr::StartsWith,
            Token::EndsWith   => Expr::EndsWith,
            Token::Contains   => Expr::Contains,
            Token::Has        => Expr::Has,
            Token::HasCs      => Expr::HasCs,
        )
        .labelled("text operation");

        let bin_op_expr = text_ops
            .recover_with(skip_until(
                any().ignored(),
                one_of([Token::Comma, Token::Pipe]).ignored(),
                || Expr::Literal(Value::Null),
            ))
            .boxed();

        let range = bin_op_expr
            .clone()
            .then_ignore(just(Token::DotDot))
            .then(bin_op_expr.clone())
            .delimited_by(just(Token::LParen), just(Token::RParen))
            .recover_with(via_parser(nested_delimiters(
                Token::LParen,
                Token::RParen,
                [(Token::LBracket, Token::RBracket)],
                |_| (Expr::Literal(Value::Null), Expr::Literal(Value::Null)),
            )))
            .boxed();

        let between_expr = bin_op_expr
            .clone()
            .then(
                choice((
                    just(Token::Between).to(true),
                    just(Token::NotBetween).to(false),
                ))
                .then(range)
                .or_not(),
            )
            .map(|(left, maybe_range)| match maybe_range {
                Some((is_between, (start, end))) => {
                    if is_between {
                        Expr::And(
                            Box::new(Expr::Gte(Box::new(left.clone()), Box::new(start))),
                            Box::new(Expr::Lte(Box::new(left), Box::new(end))),
                        )
                    } else {
                        Expr::Or(
                            Box::new(Expr::Lt(Box::new(left.clone()), Box::new(start))),
                            Box::new(Expr::Gt(Box::new(left), Box::new(end))),
                        )
                    }
                }
                None => left,
            })
            .boxed();

        let expr_list = between_expr
            .clone()
            .recover_with(skip_until(
                any().ignored(),
                one_of([Token::Comma, Token::RParen]).ignored(),
                || Expr::Literal(Value::Null),
            ))
            .separated_by(just(Token::Comma))
            .collect::<Vec<_>>()
            .delimited_by(just(Token::LParen), just(Token::RParen))
            .recover_with(via_parser(nested_delimiters(
                Token::LParen,
                Token::RParen,
                [(Token::LBracket, Token::RBracket)],
                |_| vec![],
            )))
            .boxed();

        let in_expr = between_expr
            .clone()
            .then(just(Token::In).ignore_then(expr_list).or_not())
            .map(|(left, maybe_list)| match maybe_list {
                Some(list) => Expr::In(Box::new(left), list),
                None => left,
            })
            .boxed();

        let and_expr = in_expr
            .clone()
            .foldl(just(Token::And).ignore_then(in_expr).repeated(), |l, r| {
                Expr::And(Box::new(l), Box::new(r))
            })
            .labelled("and")
            .boxed();

        and_expr
            .clone()
            .foldl(just(Token::Or).ignore_then(and_expr).repeated(), |l, r| {
                Expr::Or(Box::new(l), Box::new(r))
            })
            .labelled("or")
            .boxed()
    })
}

fn agg_default_name(agg: &Aggregation) -> String {
    match agg {
        Aggregation::Count => "count_".to_string(),
        Aggregation::DCount(field) => format!("dcount_{}", field.display_with("_")),
        Aggregation::Sum(field) => format!("sum_{}", field.display_with("_")),
        Aggregation::Avg(field) => format!("avg_{}", field.display_with("_")),
        Aggregation::Min(field) => format!("min_{}", field.display_with("_")),
        Aggregation::Max(field) => format!("max_{}", field.display_with("_")),
        Aggregation::Countif(_) => "countif_".to_string(),
    }
}

fn expr_default_name(expr: &Expr) -> Option<String> {
    Some(match expr {
        Expr::Field(field) => field.to_string(),
        Expr::Cast(_, inner) => expr_default_name(inner)?,
        Expr::Bin(lhs, _) => expr_default_name(lhs)?,
        _ => return None,
    })
}

#[derive(Debug)]
struct UnnamedProjectField {
    from: Expr,
    to: Option<Field>,
}

fn generate_unique_name(base: &str, initial: &str, used_names: &mut HashSet<String>) -> Field {
    let mut candidate = initial.to_string();
    let mut counter = 1;
    while used_names.contains(&candidate) {
        candidate = format!("{}{}", base, counter);
        counter += 1;
    }
    let field = Field::from_str(&candidate);
    used_names.insert(candidate);
    field.unwrap()
}

fn name_project_fields(fields: Vec<UnnamedProjectField>) -> Vec<ProjectField> {
    let mut result = vec![];
    let mut used_names: HashSet<String> = HashSet::new();
    for pf in fields {
        let to = if let Some(to) = pf.to {
            let default = to.to_string();
            generate_unique_name(&default, &default, &mut used_names)
        } else {
            let default_opt = expr_default_name(&pf.from);
            if let Some(default) = default_opt {
                generate_unique_name(&default, &default, &mut used_names)
            } else {
                generate_unique_name("Column", "Column1", &mut used_names)
            }
        };
        result.push(ProjectField { from: pf.from, to });
    }
    result
}

fn name_summarize_by(exprs: Vec<Expr>) -> Vec<ByField> {
    let mut result = vec![];
    let mut used_names: HashSet<String> = HashSet::new();
    for expr in exprs {
        let name = if let Some(default) = expr_default_name(&expr) {
            generate_unique_name(&default, &default, &mut used_names)
        } else {
            generate_unique_name("Column", "Column1", &mut used_names)
        };
        result.push(ByField { expr, name });
    }
    result
}

fn query_step_parser<'a, I>(
    query_expr: impl Parser<'a, I, Vec<QueryStep>, extra::Err<Rich<'a, Token>>> + Clone + 'a,
) -> impl Parser<'a, I, QueryStep, extra::Err<Rich<'a, Token>>> + Clone
where
    I: ValueInput<'a, Token = Token, Span = SimpleSpan>,
{
    let expr = expr_parser().labelled("expression");
    let field_no_arr = field_parser(false);
    let field = field_parser(true);

    let filter_step = just(Token::Where)
        .or(just(Token::Filter))
        .ignore_then(
            just(Token::Pipe)
                .rewind()
                .validate(|_, e, emitter| {
                    emitter.emit(Rich::custom(e.span(), "expected expression after where"));
                    Expr::Literal(Value::Null)
                })
                .or(expr.clone()),
        )
        .map(QueryStep::Filter)
        .labelled("filter")
        .boxed();

    let project_expr = (field_no_arr.clone().then_ignore(just(Token::Eq)).or_not())
        .then(expr.clone())
        .map(|(maybe_to, from)| UnnamedProjectField { from, to: maybe_to })
        .labelled("project expression")
        .boxed();

    let project_exprs = project_expr
        .separated_by(just(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>();

    let project_step = just(Token::Project)
        .ignore_then(
            just(Token::Pipe)
                .rewind()
                .validate(|_, e, emitter| {
                    emitter.emit(Rich::custom(e.span(), "expected expression after project"));
                    vec![UnnamedProjectField {
                        from: Expr::Literal(Value::Null),
                        to: None,
                    }]
                })
                .or(project_exprs.clone()),
        )
        .map(|fields: Vec<UnnamedProjectField>| QueryStep::Project(name_project_fields(fields)))
        .labelled("project")
        .boxed();

    let extend_step = just(Token::Extend)
        .ignore_then(project_exprs)
        .map(|fields: Vec<UnnamedProjectField>| QueryStep::Extend(name_project_fields(fields)))
        .labelled("extend");

    let project_rename_expr = field_no_arr
        .clone()
        .then(just(Token::Eq).ignore_then(field_no_arr.clone()))
        .map(|(to, from)| (from, to))
        .labelled("project-rename expression")
        .boxed();

    let project_rename_exprs = project_rename_expr
        .separated_by(just(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>();

    let project_rename_step = just(Token::ProjectRename)
        .ignore_then(project_rename_exprs.clone())
        .map(QueryStep::Rename)
        .labelled("project-rename")
        .boxed();

    let mv_expand_exprs = field
        .clone()
        .separated_by(just(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>();

    let mv_expand_step = just(Token::MvExpand)
        .ignore_then(
            just(Token::Kind)
                .ignore_then(just(Token::Eq))
                .ignore_then(
                    select! {
                        Token::Bag => ExpandKind::Bag,
                        Token::Array => ExpandKind::Array,
                    }
                    .recover_with(skip_until(
                        any().ignored(),
                        one_of([Token::Pipe, Token::Comma]).ignored(),
                        ExpandKind::default,
                    )),
                )
                .or_not(),
        )
        .then(mv_expand_exprs)
        .map(|(kind, fields)| {
            QueryStep::Expand(Expand {
                fields,
                kind: kind.unwrap_or_default(),
            })
        })
        .labelled("mv-expand")
        .boxed();

    let limit_int = select! { Token::Integer(x) => x }
        .validate(|x, e, emitter| {
            if x < 0 {
                emitter.emit(Rich::custom(
                    e.span(),
                    "limit must be a positive number. Use 'limit 100' or 'take 50'",
                ));
                100
            } else {
                x as u64
            }
        })
        .labelled("limit value");

    let limit_step = just(Token::Limit)
        .or(just(Token::Take))
        .ignore_then(limit_int)
        .map(QueryStep::Limit)
        .labelled("limit")
        .boxed();

    let sort_expr = field
        .clone()
        .then(
            just(Token::Asc)
                .to(SortOrder::Asc)
                .or(just(Token::Desc).to(SortOrder::Desc))
                .or_not(),
        )
        .then(
            just(Token::Nulls)
                .ignore_then(
                    just(Token::First)
                        .to(NullsOrder::First)
                        .or(just(Token::Last).to(NullsOrder::Last))
                        .recover_with(skip_until(
                            any().ignored(),
                            one_of([Token::Comma, Token::Pipe]).ignored(),
                            NullsOrder::default,
                        )),
                )
                .or_not(),
        )
        .map(|((by, order), nulls)| Sort {
            by,
            order: order.unwrap_or_default(),
            nulls: nulls.unwrap_or_default(),
        })
        .labelled("sort expression")
        .boxed();

    let sort_exprs = sort_expr
        .separated_by(just(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>();

    let sort_step = just(Token::Sort)
        .or(just(Token::Order))
        .ignore_then(just(Token::By).recover_with(skip_until(
            any().ignored(),
            one_of([Token::Pipe, Token::Comma]).ignored(),
            || Token::By,
        )))
        .ignore_then(sort_exprs.clone())
        .map(QueryStep::Sort)
        .labelled("sort")
        .boxed();

    let top_step = just(Token::Top)
        .ignore_then(limit_int)
        .then_ignore(just(Token::By).recover_with(skip_until(
            any().ignored(),
            one_of([Token::Pipe]).ignored(),
            || Token::By,
        )))
        .then(sort_exprs)
        .map(|(limit, sorts)| QueryStep::Top(sorts, limit))
        .labelled("top")
        .boxed();

    let summarize_agg = agg_zero_param!(Count => Count)
        .or(agg_single_param!(field; DCount => DCount))
        .or(agg_single_param!(field; Sum => Sum))
        .or(agg_single_param!(field; Avg => Avg))
        .or(agg_single_param!(field; Min => Min))
        .or(agg_single_param!(field; Max => Max))
        .or(just(Token::Countif)
            .ignore_then(
                expr.clone()
                    .delimited_by(just(Token::LParen), just(Token::RParen))
                    .recover_with(via_parser(nested_delimiters(
                        Token::LParen,
                        Token::RParen,
                        [(Token::LBracket, Token::RBracket)],
                        |_| Expr::Literal(Value::Null),
                    ))),
            )
            .map(Aggregation::Countif))
        .labelled("summarize aggregation")
        .boxed();

    let summarize_agg_expr = (field_no_arr.then_ignore(just(Token::Eq)).or_not())
        .then(summarize_agg)
        .map(|(maybe_field, agg)| (maybe_field, agg))
        .labelled("summarize aggregation expression")
        .boxed();

    let summarize_agg_exprs = summarize_agg_expr
        .separated_by(just(Token::Comma))
        .collect::<Vec<_>>()
        .boxed();

    let summarize_step = just(Token::Summarize)
        .ignore_then(summarize_agg_exprs)
        .then(
            just(Token::By)
                .ignore_then(
                    expr.clone()
                        .separated_by(just(Token::Comma))
                        .collect::<Vec<_>>(),
                )
                .or_not(),
        )
        .map(|(aggs_tuple, by)| {
            let mut aggs = HashMap::new();
            let mut unnamed_aggs = vec![];
            for (maybe_field, agg) in aggs_tuple {
                if let Some(field) = maybe_field {
                    aggs.insert(field, agg);
                } else {
                    unnamed_aggs.push(agg);
                }
            }

            for agg in unnamed_aggs {
                let base = agg_default_name(&agg);
                let mut name = base.clone();
                let mut counter = 1;
                while aggs.contains_key(&Field::from_str(&name).unwrap()) {
                    name = format!("{}{}", base, counter);
                    counter += 1;
                }
                let field = Field::from_str(&name).unwrap();
                aggs.insert(field, agg);
            }
            QueryStep::Summarize(Summarize {
                aggs,
                by: name_summarize_by(by.unwrap_or_default()),
            })
        })
        .labelled("summarize")
        .boxed();

    let distinct_step = just(Token::Distinct)
        .ignore_then(
            field
                .clone()
                .recover_with(skip_until(
                    any().ignored(),
                    one_of([Token::Comma, Token::Pipe]).ignored(),
                    || Field::from_str(ERROR_FIELD_NAME).expect("error field"),
                ))
                .separated_by(just(Token::Comma))
                .collect::<Vec<_>>(),
        )
        .map(QueryStep::Distinct)
        .labelled("distinct")
        .boxed();

    let union_step = just(Token::Union)
        .ignore_then(
            query_expr
                .clone()
                .delimited_by(just(Token::LParen), just(Token::RParen))
                .recover_with(via_parser(nested_delimiters(
                    Token::LParen,
                    Token::RParen,
                    [
                        (Token::LParen, Token::RParen),
                        (Token::LBracket, Token::RBracket),
                    ],
                    |_| vec![QueryStep::Count],
                ))),
        )
        .map(QueryStep::Union)
        .labelled("union")
        .boxed();

    let join_prefixed_field = just(Token::Dollar)
        .ignore_then(select! {
            Token::Left => true,
            Token::Right => false,
        })
        .then_ignore(just(Token::Dot))
        .then(field.clone())
        .boxed();

    let join_condition = join_prefixed_field
        .clone()
        .then_ignore(just(Token::DoubleEq))
        .then(join_prefixed_field)
        .validate(
            |((left_side, left_expr), (right_side, right_expr)), e, emitter| match (
                left_side, right_side,
            ) {
                (true, false) => (left_expr, right_expr),
                (false, true) => (right_expr, left_expr),
                (true, true) => {
                    emitter.emit(Rich::custom(
                        e.span(),
                        "join condition has both sides marked as $left. Use '$left.field == $right.field'",
                    ));
                    (left_expr, right_expr)
                }
                (false, false) => {
                    emitter.emit(Rich::custom(
                        e.span(),
                        "join condition has both sides marked as $right. Use '$left.field == $right.field'",
                    ));
                    (left_expr, right_expr)
                }
            },
        )
        .or(field.clone().map(|f| (f.clone(), f)))
        .recover_with(skip_then_retry_until(
            any().ignored(),
            just(Token::Pipe).ignored(),
        ))
        .labelled("join on condition")
        .boxed();

    let join_step = just(Token::Join)
        .ignore_then(
            just(Token::Kind)
                .ignore_then(just(Token::Eq))
                .ignore_then(select! {
                    Token::Inner => JoinType::Inner,
                    Token::Outer => JoinType::Outer,
                    Token::Left => JoinType::Left,
                    Token::Right => JoinType::Right,
                })
                .or_not(),
        )
        .then(
            just(Token::Hint)
                .ignore_then(just(Token::Dot))
                .ignore_then(just(Token::Partitions))
                .ignore_then(just(Token::Eq))
                .ignore_then(
                    select! { Token::Integer(x) => x }
                        .validate(|x, e, emitter| {
                            if x <= 0 {
                                emitter.emit(Rich::custom(
                                    e.span(),
                                    "partition count must be positive. Use 'hint.partitions=2' or similar",
                                ));
                                1 // default to 1
                            } else {
                                x
                            }
                        })
                )
                .or_not(),
        )
        .then(
            query_expr
                .delimited_by(just(Token::LParen), just(Token::RParen))
                .recover_with(via_parser(nested_delimiters(
                    Token::LParen,
                    Token::RParen,
                    [(Token::LBracket, Token::RBracket)],
                    |_| vec![QueryStep::Count],
                ))),
        )
        .then(just(Token::On).ignore_then(join_condition))
        .map(|(((kind, partitions), steps), on)| {
            QueryStep::Join(
                Join {
                    on,
                    type_: kind.unwrap_or_default(),
                    partitions: partitions.unwrap_or(1i64) as usize,
                },
                steps,
            )
        })
        .labelled("join")
        .boxed();

    let count_step = just(Token::Count)
        .to(QueryStep::Count)
        .labelled("count")
        .boxed();

    let ident = ident_parser();
    let tee_step = just(Token::Tee)
        .ignore_then(ident.clone())
        .then_ignore(just(Token::Dot))
        .then(ident)
        .map(|(connector, collection)| QueryStep::Tee {
            connector,
            collection,
        })
        .labelled("tee")
        .boxed();

    filter_step
        .or(project_step)
        .or(extend_step)
        .or(project_rename_step)
        .or(mv_expand_step)
        .or(limit_step)
        .or(sort_step)
        .or(top_step)
        .or(summarize_step)
        .or(distinct_step)
        .or(union_step)
        .or(join_step)
        .or(count_step)
        .or(tee_step)
        .recover_with(skip_until(
            none_of([Token::Pipe]).ignored(),
            just(Token::Pipe).ignored(),
            || QueryStep::Count,
        ))
        .boxed()
}

fn query_parser<'a, I>() -> impl Parser<'a, I, Vec<QueryStep>, extra::Err<Rich<'a, Token>>> + Clone
where
    I: ValueInput<'a, Token = Token, Span = SimpleSpan>,
{
    recursive(|query_expr| {
        let ident = ident_parser();

        let let_step = just(Token::Let)
            .ignore_then(ident.clone())
            .then_ignore(just(Token::Eq))
            .then(query_expr.clone().recover_with(skip_until(
                any().ignored(),
                just(Token::Semicolon).ignored(),
                || vec![QueryStep::Count],
            )))
            .then_ignore(just(Token::Semicolon))
            .map(|(name, steps)| QueryStep::Let(name, steps))
            .labelled("let")
            .boxed();

        let scan_step = ident
            .clone()
            .then(
                just(Token::Dot)
                    .ignore_then(ident.recover_with(skip_until(
                        any().ignored(),
                        one_of([Token::Pipe]).ignored(),
                        || ERROR_FIELD_NAME.to_string(),
                    )))
                    .or_not(),
            )
            .map(|(connector, collection)| {
                QueryStep::Scan(match collection {
                    Some(collection) => ScanKind::Collection {
                        connector,
                        collection,
                    },
                    None => ScanKind::Var(connector),
                })
            })
            .labelled("scan")
            .boxed();

        let pipeline = query_step_parser(query_expr)
            .separated_by(just(Token::Pipe))
            .collect::<Vec<_>>()
            .boxed();

        let_step
            .repeated()
            .collect::<Vec<_>>()
            .then(scan_step)
            .then(just(Token::Pipe).ignore_then(pipeline).or_not())
            .map(|((lets, scan), steps)| {
                let mut all_steps = lets;
                all_steps.push(scan);
                if let Some(mut pipeline_steps) = steps {
                    all_steps.append(&mut pipeline_steps);
                }
                all_steps
            })
            .recover_with(skip_then_retry_until(any().ignored(), end()))
            .labelled("query")
            .boxed()
    })
}
