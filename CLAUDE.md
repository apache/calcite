## Java Practices

- Never use the deprecated java.sql.Time, use java.sql.LocalTime instead
- When testing time, data and timestamps always use numeric values as expectations, never use formatted values.
- When writing tests that involve temporal elements do not use displayed values use numeric values OR make sure you understand how displayed values are created

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

- Read the fucking docs and stop making up shit