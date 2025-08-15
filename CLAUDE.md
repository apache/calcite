## Java Practices

- Never use the deprecated java.sql.Time, use java.sql.LocalTime instead
- When computing day offsets from epoch, never use any Java function that might misapply a local TZ offset, for example, toLocalDate().

## Testing Practices
- When testing time, data, and timestamps, always use numeric values as expectations; never use formatted values.
- When writing tests that involve temporal elements, do not use displayed values; use numeric values, OR make sure you understand how displayed values are created
- When testing timestamp with no tz, values should be in UTC.
- Tests should use lower case for all identifiers.
- The test suite should be run with the default settings, unless the test is specifically designed to test a specific setting.
- By default, lex is ORACLE and unquoted casing is TO_LOWER, and name generation is SMART_CASING (lower snake case).
- Never relax a test to pass when it should fail without asking for approval from the user.
- Always quote reserved words that have been used as identifiers in SQL statements.

## Code Maintenance

- Do not add backward compatibility to code revisions unless specifically requested.

## Splunk Adapter Notes

- The Splunk adapter can only push down simple field references in projections. Complex expressions (CAST, arithmetic, functions) must be handled by Calcite
- Always check RexNode types before casting - never assume all projections are RexInputRef
- Follow JDBC adapter patterns as the reference implementation

## Testing Practices

- The file adapter tests are extensive and require an extended timeout to complete
- Running all tests requires an extended timeout

## Documentation Practices

- Read the docs and stop making things up
- the preference is for all connections to use lex = ORACLE and unquoted casing = TO_LOWER, while sql statements use unquoted identifiers (unless not possible - like mixed casing or special characters is a requirement of the use case)

## Commands
- When I ask you to "cleanup debug code", do the following: identify any uses of `System.out` or `System.err` and determine if they should be removed or added as logger debug, 2) identify tests that are clearly temp or debug, and either remove them OR organize them and tag them as unit, integration, or performance, 3) identify dead code, report it and ask for approval to remove it, and 4) identify any temp markdown files and remove them.
