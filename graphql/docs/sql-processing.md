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

# SQL Processing

## Table of Contents
- [Overview](#overview)
- [Query Pipeline](#query-pipeline)
- [Optimization Rules](#optimization-rules)
- [Filter Processing](#filter-processing)
- [Sort Processing](#sort-processing)
- [Project Processing](#project-processing)
- [Performance Considerations](#performance-considerations)

## Overview
The SQL processing pipeline transforms SQL queries into optimized GraphQL requests while maintaining SQL:2003 compliance and maximizing query performance.

## Query Pipeline

1. **SQL Parsing**
   - Validates SQL syntax
   - Creates logical plan
   - Applies standard SQL rules

2. **Optimization**
   - Rule-based optimization
   - Cost-based decisions
   - Push-down analysis

3. **Execution**
   - GraphQL translation
   - Result materialization
   - Type conversion

## Optimization Rules

### Filter Push-down
```java
public class GraphQLFilterRule extends RelOptRule {
    public static final GraphQLFilterRule INSTANCE =
        new GraphQLFilterRule(
            operand(LogicalFilter.class,
                operand(LogicalTableScan.class, none())),
            "GraphQLFilterRule");

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final LogicalTableScan scan = call.rel(1);

        // Only push down if all conditions can be translated
        if (!canPushFilter(filter.getCondition())) {
            return;
        }

        // Create new plan with pushed filter
        call.transformTo(
            new GraphQLTableScan(
                filter.getCluster(),
                filter.getTraitSet(),
                scan.getTable(),
                filter.getCondition()));
    }
}
```

### Sort Push-down
```java
public class GraphQLSortRule extends RelOptRule {
    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final RelNode input = sort.getInput();

        // Handle offset and fetch
        RexNode offset = sort.offset;
        RexNode fetch = sort.fetch;

        // Create new plan with pushed sort
        call.transformTo(
            new GraphQLSort(
                sort.getCluster(),
                sort.getTraitSet(),
                input,
                sort.getCollation(),
                offset,
                fetch));
    }
}
```

### Project Push-down
```java
public class GraphQLProjectRule extends RelOptRule {
    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject project = call.rel(0);
        final RelNode input = project.getInput();

        // Only push down simple projections
        List<RexNode> projections = project.getProjects();
        if (!canPushProjections(projections)) {
            return;
        }

        call.transformTo(
            new GraphQLProject(
                project.getCluster(),
                project.getTraitSet(),
                input,
                projections,
                project.getRowType()));
    }
}
```

## Filter Processing

### Supported Filters
1. **Comparison Operators**
   - Equal (`=`)
   - Greater than (`>`)
   - Less than (`<`)
   - Greater than or equal (`>=`)
   - Less than or equal (`<=`)
   - Not equal (`<>`)

2. **Logical Operators**
   - AND
   - OR
   - NOT

3. **Special Conditions**
   - IS NULL
   - IS NOT NULL
   - BETWEEN
   - IN
   - EXISTS

### Filter Translation
```java
private String translateFilter(RexNode filter) {
    switch (filter.getKind()) {
        case EQUALS:
            return buildEqualsFilter((RexCall) filter);
        case LESS_THAN:
            return buildLessThanFilter((RexCall) filter);
        case GREATER_THAN:
            return buildGreaterThanFilter((RexCall) filter);
        case AND:
            return buildAndFilter((RexCall) filter);
        case OR:
            return buildOrFilter((RexCall) filter);
        // ... other cases
    }
}
```

## Sort Processing

### Sort Translation
```java
private String translateSort(Sort sort) {
    StringBuilder orderBy = new StringBuilder();
    for (RelFieldCollation collation : sort.getCollation().getFieldCollations()) {
        if (orderBy.length() > 0) {
            orderBy.append(", ");
        }
        orderBy.append(getFieldName(collation.getFieldIndex()));
        orderBy.append(collation.direction == RelFieldCollation.Direction.DESCENDING
            ? " DESC" : " ASC");
    }
    return orderBy.toString();
}
```

## Project Processing

### Project Types
1. **Simple Projections**
   - Direct field references
   - Renamed fields

2. **Computed Projections**
   - Expressions
   - Function calls
   - Case statements

### Project Translation
```java
private String translateProject(LogicalProject project) {
    StringBuilder projection = new StringBuilder();
    for (RexNode expr : project.getProjects()) {
        if (projection.length() > 0) {
            projection.append(", ");
        }
        projection.append(translateProjection(expr));
    }
    return projection.toString();
}
```

## Performance Considerations

### Optimization Guidelines
1. **Push Down Operations**
   - Filters should be pushed to data source
   - Sorts should be pushed when possible
   - Projections should minimize data transfer

2. **Avoid**
   - Complex computations in WHERE clause
   - Unnecessary type conversions
   - Large IN clauses
   - Complex expressions in ORDER BY

3. **Prefer**
   - Simple comparison operators
   - Index-friendly conditions
   - Batch operations
   - Parameter binding

### Query Planning
1. **Statistics Collection**
   - Table sizes
   - Column distributions
   - Index information

2. **Cost Calculation**
   - IO cost
   - CPU cost
   - Network cost
   - Memory usage

3. **Plan Selection**
   - Rule-based optimization
   - Cost-based optimization
   - Heuristic choices
