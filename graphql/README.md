# Apache Calcite Hasura GraphQL Adapter

This adapter enables Apache Calcite to query Hasura GraphQL endpoints using SQL. It provides a bridge between SQL and Hasura's GraphQL API by translating SQL queries into Hasura's GraphQL query format.

## Table of Contents

- [Features](#features)
- [Configuration](#configuration)
- [Query Examples](#query-examples)
- [Filter Processing](#filter-processing)
- [Sort Processing](#sort-processing)
- [Project Processing](#project-processing)
- [Type System](#type-system)
- [Implementation Details](#implementation-details)
- [Limitations](#limitations)
- [Requirements](#requirements)
- [Contributing](#contributing)
- [License](#license)

## Features

### Query Capabilities
- Execute SQL queries against Hasura GraphQL endpoints
- Automatic query optimization and planning
- Support for complex joins and subqueries
- Field projections and aliasing
- Computed field handling

### Filter Support
- Basic comparisons (`_eq`, `_gt`, `_lt`, `_gte`, `_lte`)
- NULL handling (`IS NULL`, `IS NOT NULL`)
- Range queries (`BETWEEN`)
- Logical operations:
    - AND combinations
    - OR combinations
    - NOT operations
- Support for both literals and parameters
- Complex condition decomposition

### Sort and Pagination
- Sorting with literals or parameters
- Flexible LIMIT/OFFSET with parameters
- Order by multiple fields
- Mixed literal/parameter pagination

### Project Processing
- Simple field references
- Computed field splitting
- Constant value handling
- Dynamic parameter support

## Configuration

Add the Hasura GraphQL adapter to your Calcite model file:

```json
{
  "version": "1.0",
  "defaultSchema": "hasura",
  "schemas": [
    {
      "name": "hasura",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.graphql.GraphQLSchemaFactory",
      "operand": {
        "endpoint": "https://your-hasura-instance.hasura.app/v1/graphql",
        "role": "admin",           // Optional: Hasura role
        "user": "user-id",         // Optional: Hasura user
        "auth": "Bearer token"     // Optional: Authorization header
      }
    }
  ]
}
```

## Query Examples

### Project Examples
```sql
-- Simple projections
SELECT name, email FROM users;

-- Computed fields
SELECT
  name,
  UPPER(email) as upper_email,    -- Computed locally
  age,                           -- Simple field
  ? as param_value               -- Parameter projection
FROM users;

-- Constants and expressions
SELECT
  name,
  'ACTIVE' as status,            -- Constant
  age * 2 as doubled_age        -- Computed locally
FROM users;
```

### Filter Examples

#### Basic Filters
```sql
-- Simple comparisons
SELECT * FROM users
WHERE age > 21;

-- Parameters
SELECT * FROM users
WHERE age > ?
  AND salary BETWEEN ? AND ?;

-- NULL checks
SELECT * FROM users
WHERE email IS NOT NULL
  AND phone IS NULL;
```

#### Logical Operations
```sql
-- AND combinations
SELECT * FROM users
WHERE age > 21
  AND status = 'ACTIVE'
  AND salary > 50000;

-- OR combinations
SELECT * FROM users
WHERE status = 'ACTIVE'
  OR status = 'PENDING';

-- Mixed conditions
SELECT * FROM users
WHERE (status = 'ACTIVE' AND age > 21)
   OR (status = 'PENDING' AND age > 18);

-- NOT conditions
SELECT * FROM users
WHERE NOT (status = 'INACTIVE')
  AND NOT (salary < 30000);
```

### Sort Examples
```sql
-- Basic sorting
SELECT * FROM users
ORDER BY name DESC;

-- Parameter-based pagination
SELECT * FROM users
ORDER BY name DESC
LIMIT ? OFFSET ?;

-- Multiple sort fields
SELECT * FROM users
ORDER BY age DESC, name ASC
LIMIT ? OFFSET ?;

-- Mixed literals and parameters
SELECT * FROM users
ORDER BY name DESC
LIMIT 10 OFFSET ?;
```

## Filter Processing

### Pushdown Eligibility

| Filter Type | Pushable | Conditions |
|-------------|----------|------------|
| Simple Comparisons | Yes | Column reference with literal/parameter |
| IS NULL/NOT NULL | Yes | Must reference column directly |
| BETWEEN | Yes | Column with literal/parameter bounds |
| AND | Yes | All simple conditions pushed together |
| OR | Yes | All simple conditions pushed together |
| NOT | Yes | On simple conditions |
| Complex Expressions | No | Processed by Calcite |

### Implementation Details

#### Filter Rule Logic
```java
// Handle both AND and OR conditions
if (condition.isA(SqlKind.AND) || condition.isA(SqlKind.OR)) {
    List<RexNode> operands = ((RexCall) condition).getOperands();
    return operands.stream().allMatch(this::isSimpleFilter);
}

// Process pushdown conditions
if (condition.isA(SqlKind.AND)) {
    pushDownCondition = RexUtil.composeConjunction(
        filter.getCluster().getRexBuilder(), pushDownConditions);
} else {
    pushDownCondition = RexUtil.composeDisjunction(
        filter.getCluster().getRexBuilder(), pushDownConditions);
}
```

#### Parameter Handling
```java
private static boolean isConstantOrDynamicParam(RexNode node) {
    return node instanceof RexLiteral || node instanceof RexDynamicParam;
}
```

## Project Processing

### Split Rule Logic
```java
// Handle computed fields
if (isSimpleFieldReference(expr) || isConstantOrDynamicParam(expr)) {
    // Push to base project
} else {
    // Add to computed project
}
```

## Sort Processing

### Sort Rule Validation
```java
// Accept both literals and parameters
if (sort.offset != null && !isConstantOrDynamicParam(sort.offset)) {
    return false;
}
if (sort.fetch != null && !isConstantOrDynamicParam(sort.fetch)) {
    return false;
}
```

## Type System

### Type Mapping

| Hasura Type    | SQL Type    |
|----------------|-------------|
| Int            | INTEGER     |
| Float          | FLOAT       |
| String         | VARCHAR     |
| uuid           | VARCHAR     |
| Boolean        | BOOLEAN     |
| jsonb          | MAP         |
| timestamp      | TIMESTAMP   |
| date           | DATE        |
| numeric        | DECIMAL     |
| bigint         | BIGINT      |

## Implementation Details

### Component Overview

1. **GraphQLRules**
    - Project splitting for computed fields
    - Logical operation handling
    - Parameter-aware processing
    - Sort validation

2. **Query Processing Pipeline**
   ```java
   PROJECT_SPLIT_RULE
   PROJECT_RULE
   SORT_RULE
   FILTER_RULE
   ```

3. **Rule Processing**
    - Handles simple projections
    - Processes logical operations
    - Manages parameter bindings
    - Validates sort operations

## Limitations

### Query Processing
- Complex expressions processed locally
- Function calls not pushed down
- Some mixed conditions may be split

### Feature Limitations
- No mutations support
- Limited aggregate support
- No subscription support
- No remote schemas

### Technical Constraints
- Single endpoint per schema
- Static authentication
- Limited function pushdown

## Requirements

### Core Dependencies
- Apache Calcite Core
- GraphQL Java client
- OkHttp3
- Jackson

### Optional Dependencies
- JWT libraries
- Logging frameworks

## Contributing

### Development Areas
1. Query Features
    - Mutation support
    - Aggregate functions
    - Subscription handling

2. Optimization
    - Enhanced function pushdown
    - Better join handling
    - Cost-based optimization

3. Security
    - Dynamic authentication
    - Role-based optimization
    - Custom security rules

### Development Guidelines
- Follow coding standards
- Add comprehensive tests
- Document new features
- Consider security implications
- Handle all edge cases

## License

Licensed under the Apache License 2.0 - see the LICENSE file for details.

---

For issues and questions:
- File issues on GitHub
- Check documentation
- Contact the maintainers
