build *ARGS:
    cargo build {{ARGS}}

default:
    build

run *ARGS:
    cargo run {{ARGS}}

test *ARGS:
    cargo nextest run --workspace {{ARGS}}

test-workflow-stress:
    WORKFLOW_TEST_RUNS=1000 cargo nextest run -p miso-workflow

# Run all tests (`just test`) but without any predicate pushdown (connector) tests
test-without-connectors:
    cargo nextest run --workspace -E 'not test(predicate_pushdown)'

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

run-splunk:
    scripts/run_splunk.sh

test-elasticsearch:
    cargo nextest run --workspace elasticsearch_predicate_pushdown

test-quickwit:
    cargo nextest run --workspace quickwit_predicate_pushdown

test-splunk:
    cargo nextest run --workspace splunk_predicate_pushdown

# Fast feedback loop: test an already running `just run-elasticsearch`
test-local-elasticsearch:
    EXT_ES=http://localhost:9200 cargo nextest run --workspace elasticsearch_predicate_pushdown

# Fast feedback loop: test an already running `just run-quickwit`
test-local-quickwit:
    EXT_QW=http://localhost:7280 cargo nextest run --workspace quickwit_predicate_pushdown

# Fast feedback loop: test an already running `just run-splunk`
test-local-splunk:
    EXT_SPLUNK=https://localhost:8089 cargo nextest run --workspace splunk_predicate_pushdown

# Full CI check
ci: fmt-check lint test
