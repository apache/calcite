# File Adapter Development Guidelines

## Java Code Standards

### Temporal Types
- **NEVER** use deprecated `java.sql.Time` - always use `java.time.LocalTime` instead
- **NEVER** use Java functions that might misapply local timezone offsets when computing day offsets from epoch
  - Use: UTC-based calculations explicitly

### Code Quality
- **NEVER** create Forbidden API violations - fix any existing ones immediately
- **ALWAYS** follow proper Java code style conventions
- **ALWAYS** apply DRY (Don't Repeat Yourself) principle when refactoring
- **NEVER** add backward compatibility unless explicitly requested
- **NEVER** remove features unilaterally - only simplify or improve them

### Debugging
- **NEVER** use `System.out`/`System.err` in production code
- **ALWAYS** use `logger.debug()` with lazy evaluation:
  ```java
  // GOOD: String only built if debug enabled
  logger.debug("Value: {}", () -> expensiveComputation());

  // BAD: String always built
  logger.debug("Value: " + expensiveComputation());
  ```

## Testing Standards

### Temporal Testing
- **ALWAYS** use numeric values (epoch millis, day counts, date parts) for temporal expectations
- **NEVER** use formatted/displayed date strings as test expectations
- When testing displayed values, understand exactly how they're generated
- Timestamp without timezone behavior:
  - Stored as UTC
  - Read and adjusted to local timezone with same wall clock time for display, meaning all date parts are the same only the TZ part changes.

### SQL Identifier Conventions
- **DEFAULT SETTINGS** (always use these):
  - Lex: `ORACLE`
  - Unquoted casing: `TO_LOWER`
  - Name generation: `SMART_CASING` (lower snake_case)

- **Identifier Rules**:
  - **ALWAYS** use lowercase for all test identifiers
  - **ALWAYS** quote reserved words used as identifiers
  - **ALWAYS** quote mixed-case or uppercase identifiers
  - **NEVER** quote lowercase identifiers
  - SQL statements use unquoted identifiers UNLESS:
    - The identifier has upper or mixed casing
    - The identifier contains special characters
    - The identifier is a reserved word

### Test Execution
- Tests run with default settings unless specifically testing a configuration
- File adapter tests require extended timeouts (use `gtimeout 1800` or more)
- **ALWAYS** use the build directory as the working directory for tests
- **NEVER** relax a failing test criteria without user approval

### Performance Testing
- **ALWAYS** use prepared statements to minimize planning overhead
- Debug/verification tests should be:
  - Located in a temp tests folder
  - Tagged with `@Tag("temp")` for easy identification/removal

## File Structure & Configuration

### FileSchema Configuration
- `FileSchema.baseDirectory` defaults to: `{working_directory}/.aperio/{schema_name}` (fully qualified path)
- Can be overridden via `model.json` schema operand `baseDirectory` attribute
- `FileSchema.sourceDirectory` defaults to the model.json parent path (fully qualified path)
- Can be overridden via `model.json` schema operand `sourceDirectory` attribute
- **ALWAYS** convert paths to fully qualified absolute paths

### Cache Configuration
- `ephemeralCache`: Boolean flag in schema operand (default: false)
  - When `true`: Uses temporary directory that's cleared on restart
  - When `false`: Uses persistent cache in working directory
  - Tests should set `ephemeralCache: true` for isolation via `BaseFileTest.addEphemeralCacheToModel()`
  - Production typically uses `false` for restart benefits
  - Multiple connection instances can share cache by setting same `baseDirectory` for horizontal scale-out

## Development Workflow

### Code Review & Cleanup Command
When asked to "cleanup debug code":
1. **Application Code**: Remove or convert `System.out`/`System.err` to logger.debug()
   - Note: Test code may retain System.out for test output
2. **Test Organization**:
   - Identify temp/debug tests
   - Either remove OR properly organize with tags: `@Tag("unit")`, `@Tag("integration")`, `@Tag("performance")`
3. **Dead Code**: Identify, report, and await approval before removal
4. **Temporary Files**: Remove any temporary markdown or documentation files

### Debugging Command
- To trace value computation: Generate and analyze stack traces
- **When debugging a module in test**: Add the appropriate log level configuration to `/src/test/resources/log4j2-test.xml`

## Documentation Standards

- **ALWAYS** verify against official documentation before making assumptions
- **NEVER** create documentation files unless explicitly requested
- Focus on code clarity over external documentation

## Important Principles

- Do exactly what is asked - nothing more, nothing less
- Prefer editing existing files over creating new ones
- Never create classes like "FixedXYZ" or "EnhancedXYZ" - improve the original
- When making changes:
  1. Analyze and present a plan
  2. Request approval
  3. Implement approved changes

## Plan Review Requirement

- **ALWAYS** compare your plan against these developer guidelines before presenting to user
- Ensure the plan:
  - Follows all coding standards (no System.out, proper temporal handling, etc.)
  - Adheres to testing standards (numeric temporal values, proper SQL casing)
  - Respects the "no backward compatibility" and "no FixedXYZ classes" rules
  - Doesn't create unnecessary files or documentation
  - Follows the DRY principle
  - Uses proper logging with lazy evaluation
