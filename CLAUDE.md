## Java Practices

- Never use the deprecated java.sql.Time, use java.sql.LocalTime instead
- When computing day offsets from epoch, never use any Java function that might misapply a local TZ offset, for example, toLocalDate().
- ALWAYS ask for more guidance if you are not confident in your reasoning.

## Testing Practices
- When testing time, data, and timestamps, always use numeric values as expectations; never use formatted values.
- When testing timestamp with no tz, values should be stored as UTC, but read and adjusted to local TZ.
- The test suite should be run with the default settings, unless the test is specifically designed to test a specific setting.
- Never relax a test to pass when it should fail without asking for approval from the user.
- The file adapter tests are extensive and require an extended timeout to complete
- Running all tests requires an extended timeout


## Code Maintenance

- Do not add backward compatibility to code revisions unless specifically requested.
- Breaking changes are preferred if they more faithfully represent the objective. Creating overload constructors and methods introducing unwanted defaults in order to maintain backwards compatibility should be avoided.
- Always fix forbidden API issues, or DO NOT CREATE THEM, in Java code.
- Always generate Java code with correct styles
- Never unilaterally remove features. You may only simplify or improve features.
- By default, lex is ORACLE and unquoted casing is TO_LOWER, and name generation is SMART_CASING (lower snake case).
- Always quote reserved words that have been used as identifiers in SQL statements.
- Always quote mixed or upper case identifiers.
- Never quote lower case identifiers.
- Always analyze and present a plane for code changes, then request approval to make changes.

## Splunk Adapter Notes

- The Splunk adapter can only push down simple field references in projections. Complex expressions (CAST, arithmetic, functions) must be handled by Calcite
- Always check RexNode types before casting - never assume all projections are RexInputRef
- Follow JDBC adapter patterns as the reference implementation

## File Adapter Notes

- The duckdb engine is a special case of the parquet engine. It runs an internal parquet engine to convert everything to parquet, and then creates a duckdb catalog over those parquet files.
- When duckdb is saying that is cannot find a "catalog", it is attempting to look up a name as a table, schema or catalog in that order. It means it exhausted all possible lookups. We always use 1 catalog: "memory"

## Documentation Practices

- Read the docs and stop making things up
- the preference is for all connections to use lex = ORACLE and unquoted casing = TO_LOWER, while sql statements use unquoted identifiers (unless not possible - like mixed casing or special characters is a requirement of the use case)

## Commands
- When I ask you to "cleanup debug code", do the following: identify any uses of `System.out` or `System.err` and determine if they should be removed or added as logger debug, 2) identify tests that are clearly temp or debug, and either remove them OR organize them and tag them as unit, integration, or performance, 3) identify dead code, report it and ask for approval to remove it, and 4) identify any temp markdown files and remove them.
- In order to determine how a value was computed, generate a stacktrace output
- When I ask you to "run full regression tests for [adapter-name]" or "run regression tests", execute the following based on the adapter:
  - **File adapter**: Run tests for all engine types (PARQUET, DUCKDB, LINQ4J, ARROW) using `CALCITE_FILE_ENGINE_TYPE=[engine] gtimeout 1800 ./gradlew :file:test --continue --console=plain`
  - **Splunk adapter**: Run with `CALCITE_TEST_SPLUNK=true gtimeout 1800 ./gradlew :splunk:test --continue --console=plain`
  - **SharePoint adapter**: Run with `SHAREPOINT_INTEGRATION_TESTS=true gtimeout 1800 ./gradlew :sharepoint-list:test --continue --console=plain`
  - **All adapters**: Run each adapter's full test suite sequentially with appropriate environment variables and extended timeouts
  - Always use `--continue` to run all tests even if some fail, `--console=plain` for clean output, and `gtimeout` with sufficient time (1800-3600 seconds)
