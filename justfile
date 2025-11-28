build *ARGS:
    cargo build {{ARGS}}

run *ARGS:
    cargo run {{ARGS}}

test *ARGS:
    cargo nextest run --workspace {{ARGS}}

lint:
    cargo clippy --all --all-features --tests -- -D warnings

lint-fix:
    cargo clippy --all --all-features --tests --fix

fmt-check:
    cargo fmt --all -- --check

fmt:
    cargo fmt --all

# Full CI check
ci: fmt-check lint test
