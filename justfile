build *ARGS:
    cargo build {{ARGS}}

default:
    build

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

run-opensearch:
    scripts/run_opensearch.sh

run-quickwit:
    scripts/run_quickwit.sh

test-elasticsearch:
    cargo nextest run --workspace elasticsearch_predicate_pushdown

test-quickwit:
    cargo nextest run --workspace quickwit_predicate_pushdown

# Fast feedback loop: test an already running `just run-elasticsearch`
test-local-elasticsearch:
    EXT_ES=http://localhost:9200 cargo nextest run --workspace elasticsearch_predicate_pushdown

# Fast feedback loop: test an already running `just run-quickwit`
test-local-quickwit:
    EXT_QW=http://localhost:7280 cargo nextest run --workspace quickwit_predicate_pushdown

# Full CI check
ci: fmt-check lint test
