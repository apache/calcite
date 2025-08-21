# Claude Code Custom Commands for Calcite

This directory contains custom commands for Claude Code to help with Calcite adapter development and testing.

## Available Commands

### Testing Commands

| Command | Description | Example Usage |
|---------|-------------|---------------|
| `/project:regression` | Run full regression tests for an adapter | `/project:regression file` |
| `/project:test:adapter` | Test specific adapter with options | `/project:test:adapter splunk IntegrationTest` |
| `/project:test:quick` | Quick test run for development | `/project:test:quick file` |
| `/project:test:all` | Run all adapter tests comprehensively | `/project:test:all` |

## Usage Examples

### Run regression tests for file adapter
```
/project:regression file
```

### Test Splunk adapter with specific pattern
```
/project:test:adapter splunk "*IntegrationTest"
```

### Quick test during development
```
/project:test:quick sharepoint-list
```

### Run complete test suite
```
/project:test:all
```

## Command Arguments

Most commands accept arguments in the format:
```
/project:command adapter [engine] [test-pattern]
```

- `adapter`: The adapter to test (file, splunk, sharepoint-list, etc.)
- `engine`: For file adapter only (PARQUET, DUCKDB, LINQ4J, ARROW)
- `test-pattern`: Optional test class or method pattern to run

## Adding New Commands

To add a new command:

1. Create a new `.md` file in `.claude/commands/` or a subdirectory
2. Name it according to the command you want (e.g., `deploy.md` for `/project:deploy`)
3. Use `$ARGUMENTS` placeholder for dynamic arguments
4. Include clear instructions for Claude to follow

## Best Practices

1. Keep commands focused on a single task
2. Include error handling instructions
3. Specify timeout values for long-running operations
4. Document expected outcomes
5. Use subdirectories to organize related commands

## Environment Setup

Some adapters require environment variables:

### Splunk Adapter
- `CALCITE_TEST_SPLUNK=true`
- Optional: `SPLUNK_URL`, `SPLUNK_USER`, `SPLUNK_PASSWORD`

### SharePoint Adapter
- `SHAREPOINT_INTEGRATION_TESTS=true`
- `SHAREPOINT_TENANT_ID`
- `SHAREPOINT_CLIENT_ID`
- `SHAREPOINT_CLIENT_SECRET`
- `SHAREPOINT_SITE_URL`

### File Adapter
- `CALCITE_FILE_ENGINE_TYPE` (PARQUET|DUCKDB|LINQ4J|ARROW)