use std::{fmt, str::FromStr};

use chumsky::{
    input::{Stream, ValueInput},
    prelude::*,
};
use hashbrown::HashMap;
use logos::Logos;
use miso_workflow_types::{
    expand::{Expand, ExpandKind},
    expr::{CastType, Expr},
    field::{Field, FieldAccess},
    join::{Join, JoinType},
    json,
    project::ProjectField,
    query::QueryStep,
    sort::{NullsOrder, Sort, SortOrder},
    summarize::{Aggregation, Summarize},
    value::Value,
};
use serde::Serialize;
use time::OffsetDateTime;

use crate::lexer::{StringValue, Token};

const ERROR_PLACEHOLDER: &str = "ERROR";

macro_rules! binary_ops {
    ($prev:expr; $( $token:path => $expr:path ),* $(,)?) => {
        $prev
            .clone()
            .then(
                choice((
                    $( just($token).then($prev.clone()), )*
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
                        |_| Field::from_str(ERROR_PLACEHOLDER).expect("error field"),
                    ))),
            )
            .map(Aggregation::$variant)
    };
}

#[derive(Debug, Serialize)]
pub struct ParseError {
    span: std::ops::Range<usize>,
    message: String,
    reason: String,
}

impl std::error::Error for ParseError {}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Parse error at {:?}: {} ({})",
            self.span, self.message, self.reason
        )
    }
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
                .map(|err| ParseError {
                    span: err.span().into_range(),
                    message: format!("{err:?}"),
                    reason: format!("{:?}", err.reason()),
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
        Token::Countif => "countif".to_string(),
        Token::DCount => "dcount".to_string(),
        Token::Sum => "sum".to_string(),
        Token::Min => "min".to_string(),
        Token::Max => "max".to_string(),
        Token::Avg => "avg".to_string(),
        Token::Bin => "bin".to_string(),
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
                    .ignore_then(select! {
                        Token::Integer(i) if i >= 0 => i as usize
                    })
                    .then_ignore(just(Token::RBracket))
                    .repeated()
                    .collect::<Vec<_>>(),
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
                    "byte strings are currently not supported",
                ));
                ERROR_PLACEHOLDER.to_string()
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
                        |_| Field::from_str(ERROR_PLACEHOLDER).expect("error field"),
                    ))),
            )
            .map(Expr::Exists)
            .labelled("exists")
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
            .or(cast)
            .or(exists)
            .or(datetime)
            // Must be last (fields can contain tokens that are keywords).
            .or(field.map(Expr::Field))
            .recover_with(skip_then_retry_until(
                any().ignored(),
                one_of([
                    Token::Pipe,
                    Token::Comma,
                    Token::RParen,
                    Token::RBracket,
                    Token::And,
                    Token::Or,
                ])
                .ignored(),
            ))
            .boxed();

        let mul_div = binary_ops!(atom;
            Token::Mul => Expr::Mul,
            Token::Div => Expr::Div,
        );

        let add_sub = binary_ops!(mul_div;
            Token::Plus  => Expr::Plus,
            Token::Minus => Expr::Minus,
        );

        let comparisons = binary_ops!(add_sub;
            Token::DoubleEq => Expr::Eq,
            Token::Ne       => Expr::Ne,
            Token::Gte      => Expr::Gte,
            Token::Gt       => Expr::Gt,
            Token::Lte      => Expr::Lte,
            Token::Lt       => Expr::Lt,
        );

        let text_ops = binary_ops!(comparisons;
            Token::StartsWith => Expr::StartsWith,
            Token::EndsWith   => Expr::EndsWith,
            Token::Contains   => Expr::Contains,
            Token::Has        => Expr::Has,
            Token::HasCs      => Expr::HasCs,
        );

        let bin_op_expr = text_ops
            .recover_with(skip_until(
                any().ignored(),
                one_of([Token::Comma, Token::Pipe]).ignored(),
                || Expr::Literal(Value::Null),
            ))
            .boxed();

        let expr_list = bin_op_expr
            .clone()
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

        let in_expr = bin_op_expr
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
        .ignore_then(expr.clone())
        .map(QueryStep::Filter)
        .labelled("filter")
        .boxed();

    let project_expr = field_no_arr
        .clone()
        .then(just(Token::Eq).ignore_then(expr.clone()).or_not())
        .map(|(to, from)| ProjectField {
            from: from.unwrap_or_else(|| Expr::Field(to.clone())),
            to,
        })
        .labelled("project expression")
        .boxed();

    let project_exprs = project_expr
        .separated_by(just(Token::Comma))
        .at_least(1)
        .collect::<Vec<_>>();

    let project_step = just(Token::Project)
        .ignore_then(project_exprs.clone())
        .map(QueryStep::Project)
        .labelled("project")
        .boxed();

    let extend_step = just(Token::Extend)
        .ignore_then(project_exprs)
        .map(QueryStep::Extend)
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
                .ignore_then(select! {
                    Token::Bag => ExpandKind::Bag,
                    Token::Array => ExpandKind::Array,
                })
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
            if x >= 0 && x <= u32::MAX as i64 {
                x as u32
            } else {
                emitter.emit(Rich::custom(
                    e.span(),
                    "limit must be a non-negative integer less than 2^32",
                ));
                0
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
                        .or(just(Token::Last).to(NullsOrder::Last)),
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
        .ignore_then(just(Token::By))
        .ignore_then(sort_exprs.clone())
        .map(QueryStep::Sort)
        .labelled("sort")
        .boxed();

    let top_step = just(Token::Top)
        .ignore_then(limit_int)
        .then_ignore(just(Token::By))
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

    let summarize_agg_expr = field_no_arr
        .then_ignore(just(Token::Eq).recover_with(skip_then_retry_until(
            any().ignored(),
            one_of([Token::Comma, Token::Pipe]).ignored(),
        )))
        .then(summarize_agg)
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
            for (field, agg) in aggs_tuple {
                aggs.insert(field, agg);
            }
            QueryStep::Summarize(Summarize {
                aggs,
                by: by.unwrap_or_default(),
            })
        })
        .labelled("summarize")
        .boxed();

    let distinct_step = just(Token::Distinct)
        .ignore_then(
            field
                .clone()
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
                _ => {
                    emitter.emit(Rich::custom(
                        e.span(),
                        "join condition must have one $left and one $right field",
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
                .ignore_then(select! { Token::Integer(x) => x })
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

    let count_step = just(Token::Count).to(QueryStep::Count).labelled("count");

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
        .boxed()
}

fn query_parser<'a, I>() -> impl Parser<'a, I, Vec<QueryStep>, extra::Err<Rich<'a, Token>>> + Clone
where
    I: ValueInput<'a, Token = Token, Span = SimpleSpan>,
{
    recursive(|query_expr| {
        let ident = ident_parser();

        let scan_step = ident
            .clone()
            .then_ignore(just(Token::Dot))
            .then(ident)
            .map(|(connector, collection)| QueryStep::Scan(connector, collection))
            .labelled("scan")
            .boxed();

        let pipeline = query_step_parser(query_expr)
            .separated_by(just(Token::Pipe))
            .collect::<Vec<_>>()
            .boxed();

        scan_step
            .then(just(Token::Pipe).ignore_then(pipeline).or_not())
            .map(|(scan, steps)| {
                let mut steps = steps.unwrap_or_default();
                steps.insert(0, scan);
                steps
            })
            .recover_with(skip_then_retry_until(any().ignored(), end()))
            .labelled("query")
            .boxed()
    })
}
