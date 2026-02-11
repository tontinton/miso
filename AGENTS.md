Miso is a query engine over semi-structured (JSON) logs that processes KQL (Kusto Query Language) queries and streams results via SSE (Server-Sent Events). It performs predicate pushdown optimizations to transpile query steps into connector-native queries, similar to Trino but without requiring predefined table schemas.

## Code guidelines

- NO TRIVIAL COMMENTS
- Optimize for simplicity, keep code short and lean as much as possible
- Follow Rust idioms and best practices
- Latest Rust features can be used
- Descriptive variable and function names
- No wildcard imports
- Import at top of file
- Explicit error handling with `Result<T, E>` over panics
- Use `color_eyre` when the specific error is not as important
- Use custom error types using `thiserror` for domain-specific errors
- Place unit tests in the same file using `#[cfg(test)]` modules
- Add dependencies to global `Cargo.toml`, and then set workspace=true in specific package
- Try solving with existing dependencies before adding new ones
- Prefer well-maintained crates from crates.io
- Be mindful of allocations in hot paths
- Prefer structured logging
- Provide helpful error messages
- Use #[test_case] when writing tests, and use snake_case for naming the tests

## Running / Testing

- Run tests: `cargo nextest run --workspace`
- Run lint: `cargo clippy --all --all-features --tests -- -D warnings`
- Run miso: `cargo run`
- Run miso without optimizations (sometimes useful for debugging): `cargo run -- --no-optimizations`

See justfile for more

## Architecture

The codebase is organized as a Rust workspace with the following key crates:

### Core Query Pipeline
1. **miso-kql**: Lexer and parser for KQL queries → produces AST
2. **miso-workflow-types**: Type definitions shared across the pipeline (QueryStep, Expr, Field, Value, Log types)
3. **miso-server**:
   - HTTP server with `/query` endpoint
   - Transforms parsed KQL (QueryStep) into executable WorkflowSteps
   - Manages connectors and views
4. **miso-workflow**: Execution engine that runs WorkflowSteps and streams Log results
5. **miso-optimizations**: Query optimizer that applies pattern-based transformations to WorkflowSteps

### Query Flow
```
KQL String
  → [miso-kql] parse
  → Vec<QueryStep> (query AST)
  → [miso-server] query_to_workflow::to_workflow_steps
  → Vec<WorkflowStep> (executable plan)
  → [miso-optimizations] Optimizer::optimize
  → Optimized Vec<WorkflowStep>
  → [miso-workflow] Workflow::run
  → LogTryStream (streaming results)
  → [miso-server] SSE response to client
```

### Supporting Crates
- **miso-connectors**: Connector trait and implementations (for example Quickwit)
  - Connectors implement predicate pushdown by accepting QueryHandles
  - QuickwitConnector translates WorkflowSteps into Quickwit ES queries
- **miso-common**: Shared utilities (metrics, shutdown handling, time utilities)
- **miso-tui**: Terminal UI (separate from server)

### Key Concepts

**WorkflowStep vs QueryStep**:
- QueryStep is the parsed KQL representation (miso-workflow-types/src/query.rs)
- WorkflowStep is the executable representation with connector references (miso-workflow/src/lib.rs)

**Predicate Pushdown**:
- The optimizer transforms WorkflowSteps to push filters, limits, projections, and aggregations into Scan steps
- Connectors receive a QueryHandle describing what operations to perform natively
- See miso-optimizations/src/push_into_scan/mod.rs and miso-connectors/src/quickwit.rs

**Optimization Passes**:
- Located in miso-optimizations/src/
- Pattern-based rewrites using the `pattern` module, similar to regex
- Multiple passes: pre-pushdown, pushdown, post-pushdown
- Key optimizations: dynamic filtering, const folding, project propagation, top-n conversion

**Partial Streaming**:
- Allows returning results from individual Union branches as they complete
- Controlled via PartialStream enum in query requests
- See miso-workflow/src/partial_stream.rs

**Views**:
- Named, reusable query fragments stored in the `views` connector
- Can be referenced like: `views.my_view | where ...`

## Development Notes

### Adding a New Connector
1. Implement the `Connector` trait in miso-connectors/src/
2. Define connector-specific QueryHandle and Split types with `#[typetag::serde]`
3. Implement predicate pushdown in `apply_*` methods
4. Test using integration tests, see tests/quickwit.rs

### Adding a New QueryStep/WorkflowStep
1. Add variant to QueryStep enum (miso-workflow-types/src/query.rs)
2. Update KQL parser to produce it (miso-kql/src/parser.rs)
3. Add corresponding WorkflowStep variant if needed (miso-workflow/src/lib.rs)
4. Implement execution logic in miso-workflow/src/
5. Add optimization patterns in miso-optimizations/src/

### Testing Integration Tests
- Integration tests in tests/ (e.g. tests/elasticsearch.rs and tests/quickwit.rs) use testcontainers
- Each test file runs as a single test function that creates a container, and spawns all test cases concurrently
- Filter individual test cases with the `TEST_FILTER` environment variable (for example `cargo test --test elasticsearch/quickwit`)
- For a fast feedback loop, always prefer running an external connector using `just run-quickwit` or `just run-opensearch`, and then use `EXT_QW=http://localhost:7280` or `EXT_ES=http://localhost:9200` when running the tests
