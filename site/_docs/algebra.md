---
layout: docs
title: Algebra
permalink: /docs/algebra.html
---
<!--
{% comment %}
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
{% endcomment %}
-->

Relational algebra is at the heart of Calcite. Every query is
represented as a tree of relational operators. You can translate from
SQL to relational algebra, or you can build the tree directly.

Planner rules transform expression trees using mathematical identities
that preserve semantics. For example, it is valid to push a filter
into an input of an inner join if the filter does not reference
columns from the other input.

Calcite optimizes queries by repeatedly applying planner rules to a
relational expression. A cost model guides the process, and the
planner engine generates an alternative expression that has the same
semantics as the original but a lower cost.

The planning process is extensible. You can add your own relational
operators, planner rules, cost models, and statistics.

## Algebra builder

The simplest way to build a relational expression is to use the algebra builder,
[RelBuilder]({{ site.apiRoot }}/org/apache/calcite/tools/RelBuilder.html).
Here is an example:

### TableScan

{% highlight java %}
final FrameworkConfig config;
final RelBuilder builder = RelBuilder.create(config);
final RelNode node = builder
  .scan("EMP")
  .build();
System.out.println(RelOptUtil.toString(node));
{% endhighlight %}

(You can find the full code for this and other examples in
[RelBuilderExample.java]({{ site.sourceRoot }}/core/src/test/java/org/apache/calcite/examples/RelBuilderExample.java).)

The code prints

{% highlight text %}
LogicalTableScan(table=[[scott, EMP]])
{% endhighlight %}

It has created a scan of the `EMP` table; equivalent to the SQL

{% highlight sql %}
SELECT *
FROM scott.EMP;
{% endhighlight %}

### Adding a Project

Now, let's add a Project, the equivalent of

{% highlight sql %}
SELECT ename, deptno
FROM scott.EMP;
{% endhighlight %}

We just add a call to the `project` method before calling
`build`:

{% highlight java %}
final RelNode node = builder
  .scan("EMP")
  .project(builder.field("DEPTNO"), builder.field("ENAME"))
  .build();
System.out.println(RelOptUtil.toString(node));
{% endhighlight %}

and the output is

{% highlight text %}
LogicalProject(DEPTNO=[$7], ENAME=[$1])
  LogicalTableScan(table=[[scott, EMP]])
{% endhighlight %}

The two calls to `builder.field` create simple expressions
that return the fields from the input relational expression,
namely the TableScan created by the `scan` call.

Calcite has converted them to field references by ordinal,
`$7` and `$1`.

### Adding a Filter and Aggregate

A query with an Aggregate, and a Filter:

{% highlight java %}
final RelNode node = builder
  .scan("EMP")
  .aggregate(builder.groupKey("DEPTNO"),
      builder.count(false, "C"),
      builder.sum(false, "S", builder.field("SAL")))
  .filter(
      builder.call(SqlStdOperatorTable.GREATER_THAN,
          builder.field("C"),
          builder.literal(10)))
  .build();
System.out.println(RelOptUtil.toString(node));
{% endhighlight %}

is equivalent to SQL

{% highlight sql %}
SELECT deptno, count(*) AS c, sum(sal) AS s
FROM emp
GROUP BY deptno
HAVING count(*) > 10
{% endhighlight %}

and produces

{% highlight text %}
LogicalFilter(condition=[>($1, 10)])
  LogicalAggregate(group=[{7}], C=[COUNT()], S=[SUM($5)])
    LogicalTableScan(table=[[scott, EMP]])
{% endhighlight %}

### Push and pop

The builder uses a stack to store the relational expression produced by
one step and pass it as an input to the next step. This allows the
methods that produce relational expressions to produce a builder.

Most of the time, the only stack method you will use is `build()`, to get the
last relational expression, namely the root of the tree.

Sometimes the stack becomes so deeply nested it gets confusing. To keep things
straight, you can remove expressions from the stack. For example, here we are
building a bushy join:

{% highlight text %}
.
               join
             /      \
        join          join
      /      \      /      \
CUSTOMERS ORDERS LINE_ITEMS PRODUCTS
{% endhighlight %}

We build it in three stages. Store the intermediate results in variables
`left` and `right`, and use `push()` to put them back on the stack when it is
time to create the final `Join`:

{% highlight java %}
final RelNode left = builder
  .scan("CUSTOMERS")
  .scan("ORDERS")
  .join(JoinRelType.INNER, "ORDER_ID")
  .build();

final RelNode right = builder
  .scan("LINE_ITEMS")
  .scan("PRODUCTS")
  .join(JoinRelType.INNER, "PRODUCT_ID")
  .build();

final RelNode result = builder
  .push(left)
  .push(right)
  .join(JoinRelType.INNER, "ORDER_ID")
  .build();
{% endhighlight %}

### Switch Convention

The default RelBuilder creates logical RelNode without coventions. But you could
switch to use a different convention through `adoptConvention()`:

{% highlight java %}
final RelNode result = builder
  .push(input)
  .adoptConvention(EnumerableConvention.INSTANCE)
  .sort(toCollation)
  .build();
{% endhighlight %}

In this case, we create an EnumerableSort on top of the input RelNode.

### Field names and ordinals

You can reference a field by name or ordinal.

Ordinals are zero-based. Each operator guarantees the order in which its output
fields occur. For example, `Project` returns the fields in the generated by
each of the scalar expressions.

The field names of an operator are guaranteed to be unique, but sometimes that
means that the names are not exactly what you expect. For example, when you
join EMP to DEPT, one of the output fields will be called DEPTNO and another
will be called something like DEPTNO_1.

Some relational expression methods give you more control over field names:

* `project` lets you wrap expressions using `alias(expr, fieldName)`. It
  removes the wrapper but keeps the suggested name (as long as it is unique).
* `values(String[] fieldNames, Object... values)` accepts an array of field
  names. If any element of the array is null, the builder will generate a unique
  name.

If an expression projects an input field, or a cast of an input field, it will
use the name of that input field.

Once the unique field names have been assigned, the names are immutable.
If you have a particular `RelNode` instance, you can rely on the field names not
changing. In fact, the whole relational expression is immutable.

But if a relational expression has passed through several rewrite rules (see
[RelOptRule]({{ site.apiRoot }}/org/apache/calcite/plan/RelOptRule.html)), the field
names of the resulting expression might not look much like the originals.
At that point it is better to reference fields by ordinal.

When you are building a relational expression that accepts multiple inputs,
you need to build field references that take that into account. This occurs
most often when building join conditions.

Suppose you are building a join on EMP,
which has 8 fields [EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO]
and DEPT,
which has 3 fields [DEPTNO, DNAME, LOC].
Internally, Calcite represents those fields as offsets into
a combined input row with 11 fields: the first field of the left input is
field #0 (0-based, remember), and the first field of the right input is
field #8.

But through the builder API, you specify which field of which input.
To reference "SAL", internal field #5,
write `builder.field(2, 0, "SAL")`, `builder.field(2, "EMP", "SAL")`,
or `builder.field(2, 0, 5)`.
This means "the field #5 of input #0 of two inputs".
(Why does it need to know that there are two inputs? Because they are stored on
the stack; input #1 is at the top of the stack, and input #0 is below it.
If we did not tell the builder that were two inputs, it would not know how deep
to go for input #0.)

Similarly, to reference "DNAME", internal field #9 (8 + 1),
write `builder.field(2, 1, "DNAME")`, `builder.field(2, "DEPT", "DNAME")`,
or `builder.field(2, 1, 1)`.

### Recursive Queries

Warning: The current API is experimental and subject to change without notice.
A SQL recursive query, e.g. this one that generates the sequence 1, 2, 3, ...10:

{% highlight sql %}
WITH RECURSIVE aux(i) AS (
  VALUES (1)
  UNION ALL
  SELECT i+1 FROM aux WHERE i < 10
)
SELECT * FROM aux
{% endhighlight %}

can be generated using a scan on a TransientTable and a RepeatUnion:

{% highlight java %}
final RelNode node = builder
  .values(new String[] { "i" }, 1)
  .transientScan("aux")
  .filter(
      builder.call(
          SqlStdOperatorTable.LESS_THAN,
          builder.field(0),
          builder.literal(10)))
  .project(
      builder.call(
          SqlStdOperatorTable.PLUS,
          builder.field(0),
          builder.literal(1)))
  .repeatUnion("aux", true)
  .build();
System.out.println(RelOptUtil.toString(node));
{% endhighlight %}

which produces:

{% highlight text %}
LogicalRepeatUnion(all=[true])
  LogicalTableSpool(readType=[LAZY], writeType=[LAZY], tableName=[aux])
    LogicalValues(tuples=[[{ 1 }]])
  LogicalTableSpool(readType=[LAZY], writeType=[LAZY], tableName=[aux])
    LogicalProject($f0=[+($0, 1)])
      LogicalFilter(condition=[<($0, 10)])
        LogicalTableScan(table=[[aux]])
{% endhighlight %}

### API summary

#### Relational operators

The following methods create a relational expression
([RelNode]({{ site.apiRoot }}/org/apache/calcite/rel/RelNode.html)),
push it onto the stack, and
return the `RelBuilder`.

| Method              | Description
|:------------------- |:-----------
| `scan(tableName)` | Creates a [TableScan]({{ site.apiRoot }}/org/apache/calcite/rel/core/TableScan.html).
| `functionScan(operator, n, expr...)`<br/>`functionScan(operator, n, exprList)` | Creates a [TableFunctionScan]({{ site.apiRoot }}/org/apache/calcite/rel/core/TableFunctionScan.html) of the `n` most recent relational expressions.
| `transientScan(tableName [, rowType])` | Creates a [TableScan]({{ site.apiRoot }}/org/apache/calcite/rel/core/TableScan.html) on a [TransientTable]({{ site.apiRoot }}/org/apache/calcite/schema/TransientTable.html) with the given type (if not specified, the most recent relational expression's type will be used).
| `values(fieldNames, value...)`<br/>`values(rowType, tupleList)` | Creates a [Values]({{ site.apiRoot }}/org/apache/calcite/rel/core/Values.html).
| `filter([variablesSet, ] exprList)`<br/>`filter([variablesSet, ] expr...)` | Creates a [Filter]({{ site.apiRoot }}/org/apache/calcite/rel/core/Filter.html) over the AND of the given predicates; if `variablesSet` is specified, the predicates may reference those variables.
| `project(expr...)`<br/>`project(exprList [, fieldNames])` | Creates a [Project]({{ site.apiRoot }}/org/apache/calcite/rel/core/Project.html). To override the default name, wrap expressions using `alias`, or specify the `fieldNames` argument.
| `projectPlus(expr...)`<br/>`projectPlus(exprList)` | Variant of `project` that keeps original fields and appends the given expressions.
| `projectExcept(expr...)`<br/>`projectExcept(exprList)` | Variant of `project` that keeps original fields and removes the given expressions.
| `permute(mapping)` | Creates a [Project]({{ site.apiRoot }}/org/apache/calcite/rel/core/Project.html) that permutes the fields using `mapping`.
| `convert(rowType [, rename])` | Creates a [Project]({{ site.apiRoot }}/org/apache/calcite/rel/core/Project.html) that converts the fields to the given types, optionally also renaming them.
| `aggregate(groupKey, aggCall...)`<br/>`aggregate(groupKey, aggCallList)` | Creates an [Aggregate]({{ site.apiRoot }}/org/apache/calcite/rel/core/Aggregate.html).
| `distinct()` | Creates an [Aggregate]({{ site.apiRoot }}/org/apache/calcite/rel/core/Aggregate.html) that eliminates duplicate records.
| `sort(fieldOrdinal...)`<br/>`sort(expr...)`<br/>`sort(exprList)` | Creates a [Sort]({{ site.apiRoot }}/org/apache/calcite/rel/core/Sort.html).<br/><br/>In the first form, field ordinals are 0-based, and a negative ordinal indicates descending; for example, -2 means field 1 descending.<br/><br/>In the other forms, you can wrap expressions in `as`, `nullsFirst` or `nullsLast`.
| `sortLimit(offset, fetch, expr...)`<br/>`sortLimit(offset, fetch, exprList)` | Creates a [Sort]({{ site.apiRoot }}/org/apache/calcite/rel/core/Sort.html) with offset and limit.
| `limit(offset, fetch)` | Creates a [Sort]({{ site.apiRoot }}/org/apache/calcite/rel/core/Sort.html) that does not sort, only applies with offset and limit.
| `exchange(distribution)` | Creates an [Exchange]({{ site.apiRoot }}/org/apache/calcite/rel/core/Exchange.html).
| `sortExchange(distribution, collation)` | Creates a [SortExchange]({{ site.apiRoot }}/org/apache/calcite/rel/core/SortExchange.html).
| `correlate(joinType, correlationId, requiredField...)`<br/>`correlate(joinType, correlationId, requiredFieldList)` | Creates a [Correlate]({{ site.apiRoot }}/org/apache/calcite/rel/core/Correlate.html) of the two most recent relational expressions, with a variable name and required field expressions for the left relation.
| `join(joinType, expr...)`<br/>`join(joinType, exprList)`<br/>`join(joinType, fieldName...)` | Creates a [Join]({{ site.apiRoot }}/org/apache/calcite/rel/core/Join.html) of the two most recent relational expressions.<br/><br/>The first form joins on a boolean expression (multiple conditions are combined using AND).<br/><br/>The last form joins on named fields; each side must have a field of each name.
| `semiJoin(expr)` | Creates a [Join]({{ site.apiRoot }}/org/apache/calcite/rel/core/Join.html) with SEMI join type of the two most recent relational expressions.
| `antiJoin(expr)` | Creates a [Join]({{ site.apiRoot }}/org/apache/calcite/rel/core/Join.html) with ANTI join type of the two most recent relational expressions.
| `union(all [, n])` | Creates a [Union]({{ site.apiRoot }}/org/apache/calcite/rel/core/Union.html) of the `n` (default two) most recent relational expressions.
| `intersect(all [, n])` | Creates an [Intersect]({{ site.apiRoot }}/org/apache/calcite/rel/core/Intersect.html) of the `n` (default two) most recent relational expressions.
| `minus(all)` | Creates a [Minus]({{ site.apiRoot }}/org/apache/calcite/rel/core/Minus.html) of the two most recent relational expressions.
| `repeatUnion(tableName, all [, n])` | Creates a [RepeatUnion]({{ site.apiRoot }}/org/apache/calcite/rel/core/RepeatUnion.html) associated to a [TransientTable]({{ site.apiRoot }}/org/apache/calcite/schema/TransientTable.html) of the two most recent relational expressions, with `n` maximum number of iterations (default -1, i.e. no limit).
| `snapshot(period)` | Creates a [Snapshot]({{ site.apiRoot }}/org/apache/calcite/rel/core/Snapshot.html) of the given snapshot period.
| `match(pattern, strictStart,` `strictEnd, patterns, measures,` `after, subsets, allRows,` `partitionKeys, orderKeys,` `interval)` | Creates a [Match]({{ site.apiRoot }}/org/apache/calcite/rel/core/Match.html).

Argument types:

* `expr`, `interval` [RexNode]({{ site.apiRoot }}/org/apache/calcite/rex/RexNode.html)
* `expr...`, `requiredField...` Array of
  [RexNode]({{ site.apiRoot }}/org/apache/calcite/rex/RexNode.html)
* `exprList`, `measureList`, `partitionKeys`, `orderKeys`,
  `requiredFieldList` Iterable of
  [RexNode]({{ site.apiRoot }}/org/apache/calcite/rex/RexNode.html)
* `fieldOrdinal` Ordinal of a field within its row (starting from 0)
* `fieldName` Name of a field, unique within its row
* `fieldName...` Array of String
* `fieldNames` Iterable of String
* `rowType` [RelDataType]({{ site.apiRoot }}/org/apache/calcite/rel/type/RelDataType.html)
* `groupKey` [RelBuilder.GroupKey]({{ site.apiRoot }}/org/apache/calcite/tools/RelBuilder.GroupKey.html)
* `aggCall...` Array of [RelBuilder.AggCall]({{ site.apiRoot }}/org/apache/calcite/tools/RelBuilder.AggCall.html)
* `aggCallList` Iterable of [RelBuilder.AggCall]({{ site.apiRoot }}/org/apache/calcite/tools/RelBuilder.AggCall.html)
* `value...` Array of Object
* `value` Object
* `tupleList` Iterable of List of [RexLiteral]({{ site.apiRoot }}/org/apache/calcite/rex/RexLiteral.html)
* `all`, `distinct`, `strictStart`, `strictEnd`, `allRows` boolean
* `alias` String
* `correlationId` [CorrelationId]({{ site.apiRoot }}/org/apache/calcite/rel/core/CorrelationId.html)
* `variablesSet` Iterable of
  [CorrelationId]({{ site.apiRoot }}/org/apache/calcite/rel/core/CorrelationId.html)
* `varHolder` [Holder]({{ site.apiRoot }}/org/apache/calcite/util/Holder.html) of [RexCorrelVariable]({{ site.apiRoot }}/org/apache/calcite/rex/RexCorrelVariable.html)
* `patterns` Map whose key is String, value is [RexNode]({{ site.apiRoot }}/org/apache/calcite/rex/RexNode.html)
* `subsets` Map whose key is String, value is a sorted set of String
* `distribution` [RelDistribution]({{ site.apiRoot }}/org/apache/calcite/rel/RelDistribution.html)
* `collation` [RelCollation]({{ site.apiRoot }}/org/apache/calcite/rel/RelCollation.html)
* `operator` [SqlOperator]({{ site.apiRoot }}/org/apache/calcite/sql/SqlOperator.html)
* `joinType` [JoinRelType]({{ site.apiRoot }}/org/apache/calcite/rel/core/JoinRelType.html)

The builder methods perform various optimizations, including:

* `project` returns its input if asked to project all columns in order
* `filter` flattens the condition (so an `AND` and `OR` may have more than 2 children),
  simplifies (converting say `x = 1 AND TRUE` to `x = 1`)
* If you apply `sort` then `limit`, the effect is as if you had called `sortLimit`

There are annotation methods that add information to the top relational
expression on the stack:

| Method              | Description
|:------------------- |:-----------
| `as(alias)`         | Assigns a table alias to the top relational expression on the stack
| `variable(varHolder)` | Creates a correlation variable referencing the top relational expression

#### Stack methods

| Method              | Description
|:------------------- |:-----------
| `build()`           | Pops the most recently created relational expression off the stack
| `push(rel)`         | Pushes a relational expression onto the stack. Relational methods such as `scan`, above, call this method, but user code generally does not
| `pushAll(collection)` | Pushes a collection of relational expressions onto the stack
| `peek()`            | Returns the relational expression most recently put onto the stack, but does not remove it

#### Scalar expression methods

The following methods return a scalar expression
([RexNode]({{ site.apiRoot }}/org/apache/calcite/rex/RexNode.html)).

Many of them use the contents of the stack. For example, `field("DEPTNO")`
returns a reference to the "DEPTNO" field of the relational expression just
added to the stack.

| Method              | Description
|:------------------- |:-----------
| `literal(value)` | Constant
| `field(fieldName)` | Reference, by name, to a field of the top-most relational expression
| `field(fieldOrdinal)` | Reference, by ordinal, to a field of the top-most relational expression
| `field(inputCount, inputOrdinal, fieldName)` | Reference, by name, to a field of the (`inputCount` - `inputOrdinal`)th relational expression
| `field(inputCount, inputOrdinal, fieldOrdinal)` | Reference, by ordinal, to a field of the (`inputCount` - `inputOrdinal`)th relational expression
| `field(inputCount, alias, fieldName)` | Reference, by table alias and field name, to a field at most `inputCount - 1` elements from the top of the stack
| `field(alias, fieldName)` | Reference, by table alias and field name, to a field of the top-most relational expressions
| `field(expr, fieldName)` | Reference, by name, to a field of a record-valued expression
| `field(expr, fieldOrdinal)` | Reference, by ordinal, to a field of a record-valued expression
| `fields(fieldOrdinalList)` | List of expressions referencing input fields by ordinal
| `fields(mapping)` | List of expressions referencing input fields by a given mapping
| `fields(collation)` | List of expressions, `exprList`, such that `sort(exprList)` would replicate collation
| `call(op, expr...)`<br/>`call(op, exprList)` | Call to a function or operator
| `and(expr...)`<br/>`and(exprList)` | Logical AND. Flattens nested ANDs, and optimizes cases involving TRUE and FALSE.
| `or(expr...)`<br/>`or(exprList)` | Logical OR. Flattens nested ORs, and optimizes cases involving TRUE and FALSE.
| `not(expr)` | Logical NOT
| `equals(expr, expr)` | Equals
| `isNull(expr)` | Checks whether an expression is null
| `isNotNull(expr)` | Checks whether an expression is not null
| `alias(expr, fieldName)` | Renames an expression (only valid as an argument to `project`)
| `cast(expr, typeName)`<br/>`cast(expr, typeName, precision)`<br/>`cast(expr, typeName, precision, scale)`<br/> | Converts an expression to a given type
| `desc(expr)` | Changes sort direction to descending (only valid as an argument to `sort` or `sortLimit`)
| `nullsFirst(expr)` | Changes sort order to nulls first (only valid as an argument to `sort` or `sortLimit`)
| `nullsLast(expr)` | Changes sort order to nulls last (only valid as an argument to `sort` or `sortLimit`)
| `cursor(n, input)` | Reference to `input`th (0-based) relational input of a `TableFunctionScan` with `n` inputs (see `functionScan`)

#### Pattern methods

The following methods return patterns for use in `match`.

| Method              | Description
|:------------------- |:-----------
| `patternConcat(pattern...)` | Concatenates patterns
| `patternAlter(pattern...)` | Alternates patterns
| `patternQuantify(pattern, min, max)` | Quantifies a pattern
| `patternPermute(pattern...)` | Permutes a pattern
| `patternExclude(pattern)` | Excludes a pattern

#### Group key methods

The following methods return a
[RelBuilder.GroupKey]({{ site.apiRoot }}/org/apache/calcite/tools/RelBuilder.GroupKey.html).

| Method              | Description
|:------------------- |:-----------
| `groupKey(fieldName...)`<br/>`groupKey(fieldOrdinal...)`<br/>`groupKey(expr...)`<br/>`groupKey(exprList)` | Creates a group key of the given expressions
| `groupKey(exprList, exprListList)` | Creates a group key of the given expressions with grouping sets
| `groupKey(bitSet [, bitSets])` | Creates a group key of the given input columns, with multiple grouping sets if `bitSets` is specified

#### Aggregate call methods

The following methods return an
[RelBuilder.AggCall]({{ site.apiRoot }}/org/apache/calcite/tools/RelBuilder.AggCall.html).

| Method              | Description
|:------------------- |:-----------
| `aggregateCall(op, expr...)`<br/>`aggregateCall(op, exprList)` | Creates a call to a given aggregate function
| `count([ distinct, alias, ] expr...)`<br/>`count([ distinct, alias, ] exprList)` | Creates a call to the `COUNT` aggregate function
| `countStar(alias)` | Creates a call to the `COUNT(*)` aggregate function
| `sum([ distinct, alias, ] expr)` | Creates a call to the `SUM` aggregate function
| `min([ alias, ] expr)` | Creates a call to the `MIN` aggregate function
| `max([ alias, ] expr)` | Creates a call to the `MAX` aggregate function

To further modify the `AggCall`, call its methods:

| Method               | Description
|:-------------------- |:-----------
| `approximate(approximate)` | Allows approximate value for the aggregate of `approximate`
| `as(alias)`          | Assigns a column alias to this expression (see SQL `AS`)
| `distinct()`         | Eliminates duplicate values before aggregating (see SQL `DISTINCT`)
| `distinct(distinct)` | Eliminates duplicate values before aggregating if `distinct`
| `filter(expr)`       | Filters rows before aggregating (see SQL `FILTER (WHERE ...)`)
| `sort(expr...)`<br/>`sort(exprList)` | Sorts rows before aggregating (see SQL `WITHIN GROUP`)
