use std::{cell::RefCell, slice};

use crate::workflow::WorkflowStepKind;

const DEBUG_PATTERN_PRINTS: bool = false;
const DEBUG_GROUPS_PRINTS: bool = false;

#[macro_export]
macro_rules! pattern {
    (@inner ^) => {
        $crate::optimizations::pattern::CompPattern::Start
    };
    (@inner $) => {
        $crate::optimizations::pattern::CompPattern::End
    };
    (@inner .) => {
        $crate::optimizations::pattern::CompPattern::Any
    };
    (@inner *) => {
        $crate::optimizations::pattern::CompPattern::ZeroOrMore
    };
    (@inner +) => {
        $crate::optimizations::pattern::CompPattern::OneOrMore
    };
    (@inner ?) => {
        $crate::optimizations::pattern::CompPattern::Optional
    };
    (@inner [$($variant:ident)+]) => {
        $crate::optimizations::pattern::CompPattern::Range(vec![$($crate::workflow::WorkflowStepKind::$variant),+])
    };
    (@inner [^$($variant:ident)+]) => {
        $crate::optimizations::pattern::CompPattern::NotRange(vec![$($crate::workflow::WorkflowStepKind::$variant),+])
    };
    (@inner $variant:ident) => {
        $crate::optimizations::pattern::CompPattern::Exact($crate::workflow::WorkflowStepKind::$variant)
    };
    (@inner ($($inner:tt)+)) => {
        $crate::optimizations::pattern::CompPattern::Group(vec![$(pattern!(@inner $inner)),+])
    };
    (@inner $($inner:tt)+) => {{
        $crate::optimizations::pattern::CompPattern::Sequence(vec![$(pattern!(@inner $inner)),+])
    }};

    ($($inner:tt)+) => {
        pattern!(@inner $($inner)+).compile()
    };
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum CompPattern {
    Exact(WorkflowStepKind),         // Matches a specific enum variant
    Any,                             // Matches any single enum variant (.)
    ZeroOrMore,                      // Matches zero or more occurrences (*)
    OneOrMore,                       // Matches one or more occurrences (+)
    Optional,                        // Matches zero or one occurrence (?)
    Range(Vec<WorkflowStepKind>),    // Matches any one of the specified variants ([])
    NotRange(Vec<WorkflowStepKind>), // Matches none of the specified variants ([^])
    Sequence(Vec<CompPattern>),      // Matches a sequence of patterns
    Group(Vec<CompPattern>),         // Matches a group of patterns and captures them
    Start,                           // Matches the start of the input (^)
    End,                             // Matches the end of the input ($)
}

impl CompPattern {
    fn compile_patterns(patterns: Vec<CompPattern>) -> Vec<Pattern> {
        let mut result = Vec::new();

        let mut current = None;
        for pattern in patterns {
            current = Some(match pattern {
                CompPattern::ZeroOrMore => {
                    Pattern::ZeroOrMore(Box::new(current.take().expect("* to be after pattern")))
                }
                CompPattern::OneOrMore => {
                    Pattern::OneOrMore(Box::new(current.take().expect("+ to be after pattern")))
                }
                CompPattern::Optional => {
                    Pattern::Optional(Box::new(current.take().expect("? to be after pattern")))
                }
                CompPattern::Group(inner) => {
                    if let Some(c) = current.take() {
                        result.push(c);
                    }
                    let compiled_inner = Self::compile_patterns(inner);
                    result.push(Pattern::GroupStart);
                    result.extend(compiled_inner);
                    result.push(Pattern::GroupEnd);
                    continue;
                }
                _ => {
                    if let Some(c) = current.take() {
                        result.push(c);
                    }
                    pattern.compile()
                }
            });
        }

        if let Some(c) = current.take() {
            result.push(c);
        }

        result
    }

    pub fn compile(self) -> Pattern {
        match self {
            CompPattern::Exact(x) => Pattern::Exact(x),
            CompPattern::Any => Pattern::Any,
            CompPattern::Range(x) => Pattern::Range(x),
            CompPattern::NotRange(x) => Pattern::NotRange(x),
            CompPattern::Sequence(patterns) => Pattern::Sequence(Self::compile_patterns(patterns)),
            CompPattern::Group(patterns) => {
                let compiled_inner = Self::compile_patterns(patterns);
                let mut result = Vec::with_capacity(compiled_inner.len() + 2);
                result.push(Pattern::GroupStart);
                result.extend(compiled_inner);
                result.push(Pattern::GroupEnd);
                Pattern::Sequence(result)
            }
            CompPattern::Start => Pattern::Start,
            CompPattern::End => Pattern::End,
            _ => {
                panic!("Did not expect: {self:?}");
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Pattern {
    Exact(WorkflowStepKind),         // Matches a specific enum variant
    Any,                             // Matches any single enum variant (.)
    ZeroOrMore(Box<Pattern>),        // Matches zero or more occurrences (*)
    OneOrMore(Box<Pattern>),         // Matches one or more occurrences (+)
    Optional(Box<Pattern>),          // Matches zero or one occurrence (?)
    Range(Vec<WorkflowStepKind>),    // Matches any one of the specified variants ([])
    NotRange(Vec<WorkflowStepKind>), // Matches none of the specified variants ([^])
    Sequence(Vec<Pattern>),          // Matches a sequence of patterns
    GroupStart,                      // Matches a group of patterns and captures them (
    GroupEnd,                        // Matches a group of patterns and captures them )
    Start,                           // Matches the start of the input (^)
    End,                             // Matches the end of the input ($)
}

pub type Group = (usize, usize);

trait GroupCollector {
    fn stage_start(&self, start: usize);
    fn stage_end(&self, end: usize);
    fn commit(&self);
    fn discard(&self);
}

struct VecCollector {
    groups: RefCell<Vec<Group>>,
    stack: RefCell<Vec<usize>>,
    staged: RefCell<Vec<(usize, /*is_start=*/ bool)>>,
}

impl Default for VecCollector {
    fn default() -> Self {
        Self {
            groups: RefCell::new(Vec::new()),
            stack: RefCell::new(Vec::new()),
            staged: RefCell::new(Vec::new()),
        }
    }
}

impl GroupCollector for VecCollector {
    fn stage_start(&self, start: usize) {
        if DEBUG_GROUPS_PRINTS {
            println!("Staging start: {start}");
        }
        self.staged.borrow_mut().push((start, true))
    }

    fn stage_end(&self, end: usize) {
        if DEBUG_GROUPS_PRINTS {
            println!("Staging end: {end}");
        }
        self.staged.borrow_mut().push((end, false))
    }

    fn commit(&self) {
        if DEBUG_GROUPS_PRINTS {
            println!(
                "Committing {:?} {:?} {:?}",
                self.staged.borrow(),
                self.stack.borrow(),
                self.groups.borrow()
            );
        }

        for (x, is_start) in self.staged.take() {
            if is_start {
                self.stack.borrow_mut().push(x);
            } else {
                let start = self.stack.borrow_mut().pop().unwrap();
                self.groups.borrow_mut().push((start, x));
            }
        }
    }

    fn discard(&self) {
        if DEBUG_GROUPS_PRINTS {
            println!("Discarding {:?}", self.staged.borrow());
        }
        self.staged.borrow_mut().clear();
    }
}

struct NoOpCollector;

impl GroupCollector for NoOpCollector {
    fn stage_start(&self, _: usize) {}
    fn stage_end(&self, _: usize) {}
    fn commit(&self) {}
    fn discard(&self) {}
}

impl Pattern {
    pub fn search_first(pattern: &Pattern, input: &[WorkflowStepKind]) -> Option<Group> {
        Self::search_first_on_collector(pattern, input, &NoOpCollector)
    }

    pub fn search_first_with_groups(
        pattern: &Pattern,
        input: &[WorkflowStepKind],
        groups: &mut Vec<Group>,
    ) -> Option<Group> {
        let collector = VecCollector::default();
        let result = Self::search_first_on_collector(pattern, input, &collector)?;
        *groups = collector.groups.take().into_iter().collect();
        Some(result)
    }

    fn search_first_on_collector(
        pattern: &Pattern,
        input: &[WorkflowStepKind],
        groups: &impl GroupCollector,
    ) -> Option<Group> {
        for start_idx in 0..input.len() {
            if DEBUG_PATTERN_PRINTS {
                println!("\nSearch: {:?}", &input[start_idx..]);
            }

            if let Some(end_idx) = Self::match_pattern(
                slice::from_ref(pattern),
                &input[start_idx..],
                false,
                0,
                0,
                groups,
            ) {
                groups.commit();
                return Some((start_idx, start_idx + end_idx));
            }
        }
        None
    }

    pub fn matches(pattern: &Pattern, input: &[WorkflowStepKind]) -> bool {
        Self::match_pattern(slice::from_ref(pattern), input, true, 0, 0, &NoOpCollector).is_some()
    }

    fn match_pattern(
        pattern: &[Pattern],
        input: &[WorkflowStepKind],
        exhaust_input: bool,
        p_idx: usize,
        i_idx: usize,
        groups: &impl GroupCollector,
    ) -> Option<usize> {
        assert!(!pattern.is_empty());

        if DEBUG_PATTERN_PRINTS {
            println!("1: {pattern:?}::{p_idx:?}, {input:?}::{i_idx:?}");
        }

        if p_idx == pattern.len() {
            if !exhaust_input || i_idx == input.len() {
                // We've reached the end of both pattern and input, it's a match.
                return Some(i_idx);
            }

            // We've run out of pattern but not input, it's not a match.
            return None;
        }

        let continue_match = |p_idx: usize, i_idx: usize| -> Option<usize> {
            Self::match_pattern(pattern, input, exhaust_input, p_idx, i_idx, groups)
        };

        let match_inner_pattern = |inner_pattern: &Pattern, i_idx: usize| -> Option<usize> {
            Self::match_pattern(
                slice::from_ref(inner_pattern),
                &input[i_idx..],
                false,
                0,
                0,
                &NoOpCollector,
            )
        };

        match pattern[p_idx] {
            Pattern::Start => {
                if p_idx != 0 {
                    return None;
                }
                return continue_match(p_idx + 1, i_idx);
            }
            Pattern::End => {
                if i_idx != input.len() {
                    return None;
                }
                return continue_match(p_idx + 1, i_idx);
            }
            Pattern::GroupStart => {
                groups.stage_start(i_idx);
                return continue_match(p_idx + 1, i_idx);
            }
            Pattern::GroupEnd => {
                groups.stage_end(i_idx);
                return continue_match(p_idx + 1, i_idx);
            }
            _ => {}
        }

        if i_idx == input.len() {
            // We've run out of input but not pattern.
            // Check if remaining patterns are all meaningless.
            return pattern[p_idx..]
                .iter()
                .all(|p| {
                    matches!(
                        p,
                        Pattern::Optional(..) | Pattern::ZeroOrMore(..) | Pattern::End
                    )
                })
                .then_some(i_idx);
        }

        if DEBUG_PATTERN_PRINTS {
            println!("2: {:?} on {:?}", pattern[p_idx], input[i_idx]);
        }

        match &pattern[p_idx] {
            Pattern::Exact(expected) => {
                if *expected != input[i_idx] {
                    return None;
                }
                continue_match(p_idx + 1, i_idx + 1)
            }
            Pattern::Any => continue_match(p_idx + 1, i_idx + 1),
            Pattern::Range(variants) => {
                if variants.contains(&input[i_idx]) {
                    return continue_match(p_idx + 1, i_idx + 1);
                }
                None
            }
            Pattern::NotRange(variants) => {
                if !variants.contains(&input[i_idx]) {
                    return continue_match(p_idx + 1, i_idx + 1);
                }
                None
            }
            Pattern::Optional(inner_pattern) => {
                match inner_pattern.as_ref() {
                    Pattern::ZeroOrMore(inner_pattern) => {
                        // Lazy (*?).

                        groups.commit();

                        if let Some(result) = continue_match(p_idx + 1, i_idx) {
                            return Some(result);
                        }
                        groups.discard();

                        if let Some(end) = match_inner_pattern(inner_pattern, i_idx) {
                            return continue_match(p_idx, i_idx + end);
                        }
                        groups.discard();

                        None
                    }
                    Pattern::OneOrMore(inner_pattern) => {
                        // Lazy (+?).

                        groups.commit();

                        if let Some(end) = match_inner_pattern(inner_pattern, i_idx) {
                            let mut current_idx = i_idx + end;

                            while let Some(additional_end) =
                                match_inner_pattern(inner_pattern, current_idx)
                            {
                                current_idx += additional_end;
                                if let Some(end) = continue_match(p_idx + 1, current_idx) {
                                    return Some(end);
                                }
                                groups.discard();
                            }

                            return continue_match(p_idx + 1, i_idx + end);
                        }
                        None
                    }
                    _ => {
                        if let Some(end) = match_inner_pattern(inner_pattern, i_idx) {
                            return continue_match(p_idx + 1, i_idx + end);
                        }
                        continue_match(p_idx + 1, i_idx)
                    }
                }
            }
            Pattern::ZeroOrMore(inner_pattern) => {
                groups.commit();

                let mut current_idx = i_idx;

                // Greedily match as much as possible.
                while let Some(end) = match_inner_pattern(inner_pattern, current_idx) {
                    current_idx += end;
                }

                // Backtrack - try shorter matches until one succeeds.
                while current_idx >= i_idx {
                    if let Some(end) = continue_match(p_idx + 1, current_idx) {
                        groups.commit();
                        return Some(end);
                    }
                    groups.discard();
                    current_idx -= 1;
                }

                None
            }
            Pattern::OneOrMore(inner_pattern) => {
                groups.commit();

                let mut current_idx = i_idx;
                if let Some(first_end) = match_inner_pattern(inner_pattern, current_idx) {
                    current_idx += first_end;

                    // Greedily match as much as possible.
                    while let Some(end) = match_inner_pattern(inner_pattern, current_idx) {
                        current_idx += end;
                    }

                    // Backtrack - try shorter matches until one succeeds.
                    while current_idx > i_idx {
                        if let Some(end) = continue_match(p_idx + 1, current_idx) {
                            groups.commit();
                            return Some(end);
                        }
                        groups.discard();
                        current_idx -= 1;
                    }
                }
                None
            }
            Pattern::Sequence(inner_patterns) => {
                if let Some(end) =
                    Self::match_pattern(inner_patterns, input, exhaust_input, 0, i_idx, groups)
                {
                    return continue_match(p_idx + 1, end);
                }
                None
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_first_sanity() {
        assert_eq!(
            Pattern::search_first(
                &pattern!(Limit Limit),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Count,
                ],
            ),
            Some((1, 3))
        );

        assert_eq!(
            Pattern::search_first(
                &pattern!(Limit Limit),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Count,
                    WorkflowStepKind::Limit,
                ],
            ),
            None
        );
    }

    #[test]
    fn search_first_lazy() {
        assert_eq!(
            Pattern::search_first(
                &pattern!(Limit .*? Scan),
                &[
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Scan,
                ],
            ),
            Some((0, 4))
        );

        assert_eq!(
            Pattern::search_first(
                &pattern!(Limit .+? Scan),
                &[
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Scan,
                ],
            ),
            Some((0, 4))
        );

        assert_eq!(
            Pattern::search_first(
                &pattern!(Limit .* Scan),
                &[
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Scan,
                ],
            ),
            Some((0, 5))
        );

        assert_eq!(
            Pattern::search_first(
                &pattern!(Limit + ?Scan),
                &[
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Scan,
                ],
            ),
            Some((2, 4))
        );
    }

    #[test]
    fn search_first_greedy() {
        assert_eq!(
            Pattern::search_first(
                &pattern!(Filter [Limit Count]* Count),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Count,
                    WorkflowStepKind::Count,
                ],
            ),
            Some((1, 6))
        );
    }

    #[test]
    fn search_first_exhaust_input() {
        assert_eq!(
            Pattern::search_first(
                &pattern!(Scan (Project Filter) Count),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Project,
                    WorkflowStepKind::Count,
                ],
            ),
            None
        );

        assert_eq!(
            Pattern::search_first(
                &pattern!(Scan (Project Filter) Count),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Project,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Count,
                    WorkflowStepKind::Count,
                ],
            ),
            Some((1, 5))
        );

        assert_eq!(
            Pattern::search_first(
                &pattern!(Scan (Project Filter) Project (Filter Count)),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Project,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Project,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Count,
                    WorkflowStepKind::Count,
                ],
            ),
            Some((1, 7))
        );
    }

    #[test]
    fn search_first_with_groups() {
        let mut groups = Vec::new();
        assert_eq!(
            Pattern::search_first_with_groups(
                &pattern!(Limit(Limit)Count),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Count,
                ],
                &mut groups,
            ),
            Some((1, 4))
        );
        assert_eq!(&groups, &[(1, 2)]);

        groups.clear();
        assert_eq!(
            Pattern::search_first_with_groups(
                &pattern!((Limit)(Limit Count)),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Count,
                ],
                &mut groups,
            ),
            Some((1, 4))
        );
        assert_eq!(&groups, &[(0, 1), (1, 3)]);

        groups.clear();
        assert_eq!(
            Pattern::search_first_with_groups(
                &pattern!(Filter(Limit + ?Count)),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Count,
                    WorkflowStepKind::Count,
                ],
                &mut groups,
            ),
            Some((1, 5))
        );
        assert_eq!(&groups, &[(1, 4)]);

        groups.clear();
        assert_eq!(
            Pattern::search_first_with_groups(
                &pattern!((Scan Filter) ([Limit Count]*) (Count)),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Limit,
                    WorkflowStepKind::Count,
                    WorkflowStepKind::Count,
                ],
                &mut groups,
            ),
            Some((0, 6))
        );
        assert_eq!(&groups, &[(0, 2), (2, 5), (5, 6)]);

        groups.clear();
        assert_eq!(
            Pattern::search_first_with_groups(
                &pattern!(Scan ([^TopN Filter]+) Count),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Project,
                    WorkflowStepKind::Project,
                    WorkflowStepKind::Sort,
                    WorkflowStepKind::Count,
                ],
                &mut groups,
            ),
            Some((0, 5))
        );
        assert_eq!(&groups, &[(1, 4)]);

        groups.clear();
        assert_eq!(
            Pattern::search_first_with_groups(
                &pattern!(Sort ([^Limit]*) Count),
                &[
                    WorkflowStepKind::Scan,
                    WorkflowStepKind::Filter,
                    WorkflowStepKind::Sort,
                    WorkflowStepKind::Sort,
                    WorkflowStepKind::Count,
                ],
                &mut groups,
            ),
            Some((2, 5))
        );
        assert_eq!(&groups, &[(1, 2)]);
    }

    #[test]
    fn matches_sanity() {
        assert!(Pattern::matches(
            &pattern!(^Scan Count $),
            &[WorkflowStepKind::Scan, WorkflowStepKind::Count],
        ));

        assert!(Pattern::matches(
            &pattern!(Scan Count $),
            &[WorkflowStepKind::Scan, WorkflowStepKind::Count],
        ));

        assert!(!Pattern::matches(
            &pattern!(^Scan Count$),
            &[
                WorkflowStepKind::Scan,
                WorkflowStepKind::Count,
                WorkflowStepKind::Count
            ],
        ));

        assert!(!Pattern::matches(
            &pattern!(^Count Scan$),
            &[WorkflowStepKind::Scan, WorkflowStepKind::Count],
        ));

        assert!(Pattern::matches(
            &pattern!(^Scan*$),
            &[WorkflowStepKind::Scan],
        ));

        assert!(Pattern::matches(
            &pattern!(^Scan+$),
            &[WorkflowStepKind::Scan, WorkflowStepKind::Scan],
        ));

        assert!(Pattern::matches(
            &pattern!(^Scan.*Project$),
            &[
                WorkflowStepKind::Scan,
                WorkflowStepKind::Filter,
                WorkflowStepKind::TopN,
                WorkflowStepKind::Project
            ],
        ));

        assert!(Pattern::matches(
            &pattern!(^Scan [TopN Filter]+ Count$),
            &[
                WorkflowStepKind::Scan,
                WorkflowStepKind::Filter,
                WorkflowStepKind::TopN,
                WorkflowStepKind::TopN,
                WorkflowStepKind::Count,
            ],
        ));

        assert!(!Pattern::matches(
            &pattern!(^Scan [TopN Filter]+ Count$),
            &[WorkflowStepKind::Scan, WorkflowStepKind::Count],
        ));

        assert!(Pattern::matches(
            &pattern!(^Scan [TopN Filter]* Count$),
            &[WorkflowStepKind::Scan, WorkflowStepKind::Count],
        ));

        assert!(Pattern::matches(
            &pattern!(^Scan [TopN Filter]? Count$),
            &[WorkflowStepKind::Scan, WorkflowStepKind::Count],
        ));

        assert!(!Pattern::matches(
            &pattern!(^Scan+$),
            &[
                WorkflowStepKind::Scan,
                WorkflowStepKind::Filter,
                WorkflowStepKind::Scan
            ],
        ));

        assert!(Pattern::matches(
            &pattern!(^Scan [^TopN Filter]+ Count$),
            &[
                WorkflowStepKind::Scan,
                WorkflowStepKind::Project,
                WorkflowStepKind::Project,
                WorkflowStepKind::Sort,
                WorkflowStepKind::Count,
            ],
        ));
    }
}
