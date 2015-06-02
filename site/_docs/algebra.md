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

{% assign sourceRoot = "http://github.com/apache/incubator-calcite/blob/master" %}
{% assign apiRoot = "http://calcite.hydromatic.net/apidocs" %}

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
operators, planner rules, cost model, and statistics.

## Algebra builder

The simplest way to build a relational expression is to use the algebra builder,
[RelBuilder]({{ apiRoot }}/org/apache/calcite/tools/RelBuilder.html).
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
[RelBuilderExample.java]({{ sourceRoot }}/core/src/test/java/org/apache/calcite/examples/RelBuilderExample.java).)

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
([RelOptRule]({{ apiRoot }}/org/apache/calcite/plan/RelOptRule.html)), the field
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
write `builder.field(2, 0, "SAL")`
or `builder.field(2, 0, 5)`.
This means "the field #5 of input #0 of two inputs".
(Why does it need to know that there are two inputs? Because they are stored on
the stack; input #1 is at the top of the stack, and input #0 is below it.
If we did not tell the builder that were two inputs, it would not know how deep
to go for input #0.)

Similarly, to reference "DNAME", internal field #9 (8 + 1),
write `builder.field(2, 1, "DNAME")`
or `builder.field(2, 1, 1)`.

### API summary

#### Relational operators

The following methods create a relational expression
([RelNode]({{ apiRoot }}/org/apache/calcite/rel/RelNode.html)),
push it onto the stack, and
return the `RelBuilder`.

| Method              | Description
|:------------------- |:-----------
| `scan(tableName)` | Creates a [TableScan]({{ apiRoot }}/org/apache/calcite/rel/core/TableScan.html).
| `values(fieldNames, value...)`<br/>`values(rowType, tupleList)` | Creates a [Values]({{ apiRoot }}/org/apache/calcite/rel/core/Values.html).
| `filter(expr...)`<br/>`filter(exprList)` | Creates a [Filter]({{ apiRoot }}/org/apache/calcite/rel/core/Filter.html) over the AND of the given predicates.
| `project(expr...)`<br/>`project(exprList)` | Creates a [Project]({{ apiRoot }}/org/apache/calcite/rel/core/Project.html). To override the default name, wrap expressions using `alias`.
| `aggregate(groupKey, aggCall...)`<br/>`aggregate(groupKey, aggCallList)` | Creates an [Aggregate]({{ apiRoot }}/org/apache/calcite/rel/core/Aggregate.html).
| `distinct()` | Creates an [Aggregate]({{ apiRoot }}/org/apache/calcite/rel/core/Aggregate.html) that eliminates duplicate records.
| `sort(fieldOrdinal...)`<br/>`sort(expr...)`<br/>`sort(exprList)` | Creates a [Sort]({{ apiRoot }}/org/apache/calcite/rel/core/Sort.html).<br/><br/>In the first form, field ordinals are 0-based, and a negative ordinal indicates descending; for example, -2 means field 1 descending.<br/><br/>In the other forms, you can wrap expressions in `as`, `nullsFirst` or `nullsLast`.
| `sortLimit(offset, fetch, expr...)`<br/>`sortLimit(offset, fetch, exprList)` | Creates a [Sort]({{ apiRoot }}/org/apache/calcite/rel/core/Sort.html) with offset and limit.
| `limit(offset, fetch)` | Creates a [Sort]({{ apiRoot }}/org/apache/calcite/rel/core/Sort.html) that does not sort, only applies with offset and limit.
| `join(joinType, expr)`<br/>`join(joinType, fieldName...)` | Creates a [Join]({{ apiRoot }}/org/apache/calcite/rel/core/Join.html) of the two most recent relational expressions.<br/><br/>The first form joins on an boolean expression.<br/><br/>The second form joins on named fields; each side must have a field of each name.
| `union(all)` | Creates a [Union]({{ apiRoot }}/org/apache/calcite/rel/core/Union.html) of the two most recent relational expressions.
| `intersect(all)` | Creates an [Intersect]({{ apiRoot }}/org/apache/calcite/rel/core/Intersect.html) of the two most recent relational expressions.
| `minus(all)` | Creates a [Minus]({{ apiRoot }}/org/apache/calcite/rel/core/Minus.html) of the two most recent relational expressions.

Argument types:

* `expr`  [RexNode]({{ apiRoot }}/org/apache/calcite/rex/RexNode.html)
* `expr...` Array of [RexNode]({{ apiRoot }}/org/apache/calcite/rex/RexNode.html)
* `exprList` Iterable of [RexNode]({{ apiRoot }}/org/apache/calcite/rex/RexNode.html)
* `fieldOrdinal` Ordinal of a field within its row (starting from 0)
* `fieldName` Name of a field, unique within its row
* `fieldName...` Array of String
* `fieldNames` Iterable of String
* `rowType` [RelDataType]({{ apiRoot }}/org/apache/calcite/rel/type/RelDataType.html)
* `groupKey` [RelBuilder.GroupKey]({{ apiRoot }}/org/apache/calcite/tools/RelBuilder/GroupKey.html)
* `aggCall...` Array of [RelBuilder.AggCall]({{ apiRoot }}/org/apache/calcite/tools/RelBuilder/AggCall.html)
* `aggCallList` Iterable of [RelBuilder.AggCall]({{ apiRoot }}/org/apache/calcite/tools/RelBuilder/AggCall.html)
* `value...` Array of Object
* `value` Object
* `tupleList` Iterable of List of [RexLiteral]({{ apiRoot }}/org/apache/calcite/rex/RexLiteral.html)
* `all` boolean
* `distinct` boolean
* `alias` String

### Stack methods


| Method              | Description
|:------------------- |:-----------
| `build()`           | Pops the most recently created relational expression off the stack
| `push(rel)`         | Pushes a relational expression onto the stack. Relational methods such as `scan`, above, call this method, but user code generally does not
| `peek()`            | Returns the relational expression most recently put onto the stack, but does not remove it

#### Scalar expression methods

The following methods return a scalar expression
([RexNode]({{ apiRoot }}/org/apache/calcite/rex/RexNode.html)).

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

### Group key methods

The following methods return a
[RelBuilder.GroupKey]({{ apiRoot }}/org/apache/calcite/tools/RelBuilder/GroupKey.html).

| Method              | Description
|:------------------- |:-----------
| `groupKey(fieldName...)`<br/>`groupKey(fieldOrdinal...)`<br/>`groupKey(expr...)`<br/>`groupKey(exprList)` | Creates a group key of the given expressions

### Aggregate call methods

The following methods return an
[RelBuilder.AggCall]({{ apiRoot }}/org/apache/calcite/tools/RelBuilder/AggCall.html).

| Method              | Description
|:------------------- |:-----------
| `aggregateCall(op, distinct, alias, expr...)`<br/>`aggregateCall(op, distinct, alias, exprList)` | Creates a call to a given aggregate function
| `count(distinct, alias, expr...)` | Creates a call to the COUNT aggregate function
| `countStar(alias)` | Creates a call to the COUNT(*) aggregate function
| `sum(distinct, alias, expr)` | Creates a call to the SUM aggregate function
| `min(alias, expr)` | Creates a call to the MIN aggregate function
| `max(alias, expr)` | Creates a call to the MAX aggregate function
