<\!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Features and SQL Support

## Table of Contents
- [SQL:2003 Compliance](#sql2003-compliance)
- [Query Processing Features](#query-processing-features)
- [Security Features](#security-features)
- [Performance Features](#performance-features)
- [Integration Features](#integration-features)

## SQL:2003 Compliance
The adapter implements comprehensive SQL:2003 support, providing enterprise-grade SQL functionality against GraphQL endpoints.

### Core SQL Features
- Complete SQL:2003 standard compliance
- Extensive function library
- Complex expression support
- Data type compatibility
- Transaction support (where available)

### Query Types
- SELECT statements with full syntax support
- Subqueries in WHERE, FROM, and SELECT clauses
- Common Table Expressions (CTE)
- Recursive queries
- Set operations (UNION, INTERSECT, EXCEPT)

### Window Functions
```sql
-- Supported window functions include:
ROW_NUMBER()
RANK()
DENSE_RANK()
FIRST_VALUE
LAST_VALUE
LAG/LEAD
NTH_VALUE
PERCENT_RANK
CUME_DIST
```

#### Window Frame Specifications
- ROWS/RANGE
- CURRENT ROW
- UNBOUNDED PRECEDING/FOLLOWING
- n PRECEDING/FOLLOWING

### Joins
- INNER JOIN
- LEFT/RIGHT OUTER JOIN
- FULL OUTER JOIN
- CROSS JOIN
- Natural joins
- Complex join conditions
- Multiple join tables

### Aggregation
- Standard aggregates (COUNT, SUM, AVG, MIN, MAX)
- Statistical functions (STDDEV, VARIANCE)
- Conditional aggregates (filtered aggregates)
- GROUP BY with CUBE, ROLLUP, GROUPING SETS
- HAVING clause

### Functions
#### String Functions
```sql
SUBSTRING(string FROM start [FOR length])
TRIM([LEADING|TRAILING|BOTH] FROM string)
UPPER(string)
LOWER(string)
REPLACE(string, search, replace)
POSITION(substring IN string)
CHAR_LENGTH(string)
OVERLAY(string PLACING replacement FROM start [FOR length])
```

#### Numeric Functions
```sql
ABS(numeric)
CEIL(numeric)
FLOOR(numeric)
ROUND(numeric [, scale])
POWER(base, exponent)
MOD(dividend, divisor)
```

#### Date/Time Functions
```sql
CURRENT_DATE
CURRENT_TIME
CURRENT_TIMESTAMP
EXTRACT(field FROM source)
DATE_TRUNC(field, source)
```

#### Conditional Functions
```sql
CASE WHEN ... THEN ... END
COALESCE(value [, ...])
NULLIF(value1, value2)
GREATEST(value [, ...])
LEAST(value [, ...])
```

## Query Processing Features

### Optimization
- Cost-based optimization
- Rule-based optimization
- Join reordering
- Predicate pushdown
- Projection pruning
- Partition pruning
- Index selection
- Subquery optimization

### Execution
- Parallel query execution
- Streaming results
- Memory management
- Resource governance
- Query timeout handling
- Error recovery

### Type System
- Strong type checking
- Automatic type conversion
- Custom type support
- NULL handling
- Array types
- Composite types

## Security Features

### Authentication
- Token-based authentication
- Role-based access control
- User context propagation
- Session management
- SSL/TLS support

### Authorization
- Row-level security
- Column-level security
- Query restrictions
- Resource limits
- Audit logging

## Performance Features

### Caching
- Query result caching
- Schema caching
- Statistics caching
- Prepared statement caching
- Connection pooling

### Monitoring
- Query metrics
- Performance statistics
- Resource utilization
- Cache statistics
- Error tracking

## Integration Features

### Client Support
- JDBC driver compatibility
- Connection pooling
- Statement pooling
- Batch operations
- Transaction management

### Tool Integration
- SQL IDE support
- Query builders
- Schema browsers
- Monitoring tools
- Testing frameworks

### Extension Points
- Custom functions
- Custom types
- Custom optimizations
- Custom security rules
- Custom caching strategies

See also:
- [Configuration Guide](configuration.md)
- [Query Examples](query-examples.md)
- [SQL Processing](sql-processing.md)
