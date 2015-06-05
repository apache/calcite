---
layout: docs
title: Streaming
permalink: /docs/stream.html
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

Calcite has extended SQL and relational algebra in order to support
streaming queries.

Streams are collections to records that flow continuously, and forever.
Unlike tables, they are not typically stored on disk, but flow over the
network and are held for short periods of time in memory.

Streams complement tables because they represent what is happening in the
present and future of the enterprise whereas tables represent the past.
It is very common for a stream to be archived into a table.

Like tables, you often want to query streams in a high-level language
based on relational algebra, validated according to a schema, and optimized
to take advantage of available resources and algorithms.

Calcite's SQL is an extension to standard SQL, not another 'SQL-like' language.
The distinction is important, for several reasons:

* Streaming SQL is easy to learn for anyone who knows regular SQL.
* The semantics are clear, because we aim to produce the same results on a
  stream as if the same data were in a table.
* You can write queries that combine streams and tables (or the history of
  a stream, which is basically an in-memory table).
* Lots of existing tools can generate standard SQL.

If you don't use the `STREAM` keyword, you are back in regular
standard SQL.

## An example schema

Our streaming SQL examples use the following schema:

* `Orders (rowtime, productId, orderId, units)` - a stream and a table
* `Products (rowtime, productId, name)` - a table
* `Shipments (rowtime, orderId)` - a stream

## A simple query

Let's start with the simplest streaming query:

{% highlight sql %}
SELECT STREAM *
FROM Orders;

  rowtime | productId | orderId | units
----------+-----------+---------+-------
 10:17:00 |        30 |       5 |     4
 10:17:05 |        10 |       6 |     1
 10:18:05 |        20 |       7 |     2
 10:18:07 |        30 |       8 |    20
 11:02:00 |        10 |       9 |     6
 11:04:00 |        10 |      10 |     1
 11:09:30 |        40 |      11 |    12
 11:24:11 |        10 |      12 |     4
{% endhighlight %}

This query reads all columns and rows from the `Orders` stream.
Like any streaming query, it never terminates. It outputs a record whenever
a record arrives in `Orders`.

Type `Control-C` to terminate the query.

The `STREAM` keyword is the main extension in streaming SQL. It tells the
system that you are interested in incoming orders, not existing ones. The query

{% highlight sql %}
SELECT *
FROM Orders;

  rowtime | productId | orderId | units
----------+-----------+---------+-------
 08:30:00 |        10 |       1 |     3
 08:45:10 |        20 |       2 |     1
 09:12:21 |        10 |       3 |    10
 09:27:44 |        30 |       4 |     2

4 records returned.
{% endhighlight %}

is also valid, but will print out all existing orders and then terminate. We
call it a *relational* query, as opposed to *streaming*. It has traditional
SQL semantics.

`Orders` is special, in that it has both a stream and a table. If you try to run
a streaming query on a table, or a relational query on a stream, Calcite gives
an error:

{% highlight sql %}
> SELECT * FROM Shipments;
ERROR: Cannot convert stream 'SHIPMENTS' to a table

> SELECT STREAM * FROM Products;
ERROR: Cannot convert table 'PRODUCTS' to a stream
{% endhighlight %}

# Filtering rows

Just as in regular SQL, you use a `WHERE` clause to filter rows:

{% highlight sql %}
SELECT STREAM *
FROM Orders
WHERE units > 3;

  rowtime | productId | orderId | units
----------+-----------+---------+-------
 10:17:00 |        30 |       5 |     4
 10:18:07 |        30 |       8 |    20
 11:02:00 |        10 |       9 |     6
 11:09:30 |        40 |      11 |    12
 11:24:11 |        10 |      12 |     4
{% endhighlight %}

# Projecting expressions

Use expressions in the `SELECT` clause to choose which columns to return or
compute expressions:

{% highlight sql %}
SELECT STREAM rowtime,
  'An order for ' || units || ' '
    || CASE units WHEN 1 THEN 'unit' ELSE 'units' END
    || ' of product #' || productId AS description
FROM Orders;

  rowtime | description
----------+---------------------------------------
 10:17:00 | An order for 4 units of product #30
 10:17:05 | An order for 1 unit of product #10
 10:18:05 | An order for 2 units of product #20
 10:18:07 | An order for 20 units of product #30
 11:02:00 | An order by 6 units of product #10
 11:04:00 | An order by 1 unit of product #10
 11:09:30 | An order for 12 units of product #40
 11:24:11 | An order by 4 units of product #10
{% endhighlight %}

We recommend that you always include the `rowtime` column in the `SELECT`
clause. Having a sorted timestamp in each stream and streaming query makes it
possible to do advanced calculations later, such as `GROUP BY` and `JOIN`.

# Tumbling windows

There are several ways to compute aggregate functions on streams. The
differences are:

* How many rows come out for each row in?
* Does each incoming value appear in one total, or more?
* What defines the "window", the set of rows that contribute to a given output row?
* Is the result a stream or a relation?

First we'll look a *tumbling window*, which is defined by a streaming
`GROUP BY`. Here is an example:

{% highlight sql %}
SELECT STREAM FLOOR(rowtime TO HOUR) AS rowtime,
  productId,
  COUNT(*) AS c,
  SUM(units) AS units
FROM Orders
GROUP BY FLOOR(rowtime TO HOUR), productId;

  rowtime | productId |       c | units
----------+-----------+---------+-------
 10:00:00 |        30 |       2 |    24
 10:00:00 |        10 |       1 |     1
 10:00:00 |        20 |       1 |     7
 11:00:00 |        10 |       3 |    11
 11:00:00 |        40 |       1 |    12
{% endhighlight %}

The result is a stream. At 11 o'clock, Calcite emits a sub-total for every
`productId` that had an order since 10 o'clock. At 12 o'clock, it will emit
the orders that occurred between 11:00 and 12:00. Each input row contributes to
only one output row.

How did Calcite know that the 10:00:00 sub-totals were complete at 11:00:00,
so that it could emit them? It knows that `rowtime` is increasing, and it knows
that `FLOOR(rowtime TO HOUR)` is also increasing. So, once it has seen a row
at or after 11:00:00, it will never see a row that will contribute to a 10:00:00
total.

A column or expression that is increasing or decreasing is said to be
*monotonic*. Without a monotonic expression in the `GROUP BY` clause, Calcite is
not able to make progress, and it will not allow the query:

{% highlight sql %}
> SELECT STREAM productId,
>   COUNT(*) AS c,
>   SUM(units) AS units
> FROM Orders
> GROUP BY productId;
ERROR: Streaming aggregation requires at least one monotonic expression in GROUP BY clause
{% endhighlight %}

Monotonic columns need to be declared in the schema. The monotonicity is
enforced when records enter the stream and assumed by queries that read from
that stream. We recommend that you give each stream a timestamp column called
`rowtime`, but you can declare others, `orderId`, for example.

# Filtering after aggregation

As in standard SQL, you can apply a `HAVING` clause to filter rows emitted by
a streaming `GROUP BY`:

{% highlight sql %}
SELECT STREAM FLOOR(rowtime TO HOUR) AS rowtime,
  productId
FROM Orders
GROUP BY FLOOR(rowtime TO HOUR), productId
HAVING COUNT(*) > 2 OR SUM(units) > 10;

  rowtime | productId
----------+-----------
 10:00:00 |        30
 11:00:00 |        10
 11:00:00 |        40
{% endhighlight %}

# Sub-queries, views and SQL's closure property

The previous `HAVING` query can be expressed using a `WHERE` clause on a
sub-query:

{% highlight sql %}
SELECT STREAM rowtime, productId
FROM (
  SELECT FLOOR(rowtime TO HOUR) AS rowtime,
    productId,
    COUNT(*) AS c,
    SUM(units) AS su
  FROM Orders
  GROUP BY FLOOR(rowtime TO HOUR), productId)
WHERE c > 2 OR su > 10;

  rowtime | productId
----------+-----------
 10:00:00 |        30
 11:00:00 |        10
 11:00:00 |        40
{% endhighlight %}

`HAVING` was introduced in the early days of SQL, when a way was needed to
perform a filter *after* aggregation. (Recall that `WHERE` filters rows before
they enter the `GROUP BY` clause.)

Since then, SQL has become a mathematically closed language, which means that
any operation you can perform on a table can also perform on a query.

The *closure property* of SQL is extremely powerful. Not only does it render
`HAVING` obsolete (or, at least, reduce it to syntactic sugar), it makes views
possible:

{% highlight sql %}
CREATE VIEW HourlyOrderTotals (rowtime, productId, c, su) AS
  SELECT FLOOR(rowtime TO HOUR),
    productId,
    COUNT(*),
    SUM(units)
  FROM Orders
  GROUP BY FLOOR(rowtime TO HOUR), productId;

SELECT STREAM rowtime, productId
FROM HourlyOrderTotals
WHERE c > 2 OR su > 10;

  rowtime | productId
----------+-----------
 10:00:00 |        30
 11:00:00 |        10
 11:00:00 |        40
{% endhighlight %}

Sub-queries in the `FROM` clause are sometimes referred to as "inline views",
but really, they are more fundamental than views. Views are just a convenient
way to carve your SQL into manageable chunks by giving the pieces names and
storing them in the metadata repository.

Many people find that nested queries and views are even more useful on streams
than they are on relations. Streaming queries are pipelines of
operators all running continuously, and often those pipelines get quite long.
Nested queries and views help to express and manage those pipelines.

And, by the way, a `WITH` clause can accomplish the same as a sub-query or
a view:

{% highlight sql %}
WITH HourlyOrderTotals (rowtime, productId, c, su) AS (
  SELECT FLOOR(rowtime TO HOUR),
    productId,
    COUNT(*),
    SUM(units)
  FROM Orders
  GROUP BY FLOOR(rowtime TO HOUR), productId)
SELECT STREAM rowtime, productId
FROM HourlyOrderTotals
WHERE c > 2 OR su > 10;

  rowtime | productId
----------+-----------
 10:00:00 |        30
 11:00:00 |        10
 11:00:00 |        40
{% endhighlight %}

## Converting between streams and relations

Look back at the definition of the `HourlyOrderTotals` view.
Is the view a stream or a relation?

It does not contain the `STREAM` keyword, so it is a relation.
However, it is a relation that can be converted into a stream.

You can use it in both relational and streaming queries:

{% highlight sql %}
# A relation; will query the historic Orders table.
# Returns the largest number of product #10 ever sold in one hour.
SELECT max(su)
FROM HourlyOrderTotals
WHERE productId = 10;

# A stream; will query the Orders stream.
# Returns every hour in which at least one product #10 was sold.
SELECT STREAM rowtime
FROM HourlyOrderTotals
WHERE productId = 10;
{% endhighlight %}

This approach is not limited to views and sub-queries.
Following the approach set out in CQL [<a href="#ref1">1</a>], every query
in streaming SQL is defined as a relational query and converted to a stream
using the `STREAM` keyword in the top-most `SELECT`.

If the `STREAM` keyword is present in sub-queries or view definitions, it has no
effect.

At query preparation time, Calcite figures out whether the relations referenced
in the query can be converted to streams or historical relations.

Sometimes a stream makes available some of its history (say the last 24 hours of
data in an Apache Kafka [<a href="#ref2">2</a>] topic)
but not all. At run time, Calcite figures out whether there is sufficient
history to run the query, and if not, gives an error.

## Hopping windows

Previously we saw how to define a tumbling window using a `GROUP BY` clause.
Each record contributed to a single sub-total record, the one containing its
hour and product id.

But suppose we want to emit, every hour, the number of each product ordered over
the past three hours. To do this, we use `SELECT ... OVER` and a sliding window
to combine multiple tumbling windows.

{% highlight sql %}
SELECT STREAM rowtime,
  productId,
  SUM(su) OVER w AS su,
  SUM(c) OVER w AS c
FROM HourlyTotals
WINDOW w AS (
  ORDER BY rowtime
  PARTITION BY productId
  RANGE INTERVAL '2' HOUR PRECEDING)
{% endhighlight %}

This query uses the `HourlyOrderTotals` view defined previously.
The 2 hour interval combines the totals timestamped 09:00:00, 10:00:00 and
11:00:00 for a particular product into a single total timestamped 11:00:00 and
summarizing orders for that product between 09:00:00 and 12:00:00.

## Limitations of tumbling and hopping windows

In the present syntax, we acknowledge that it is not easy to create certain
kinds of windows.

First, let's consider tumbling windows over complex periods.

The `FLOOR` and `CEIL` functions make is easy to create a tumbling window that
emits on a whole time unit (say every hour, or every minute) but less easy to
emit, say, every 15 minutes. One could imagine an extension to the `FLOOR`
function that emits unique values on just about any periodic basis (say in 11
minute intervals starting from midnight of the current day).

Next, let's consider hopping windows whose retention period is not a multiple
of its emission period. Say we want to output, at the top of each hour, the
orders for the previous 7,007 seconds. If we were to simulate this hopping
window using a sliding window over a tumbling window, as before, we would have
to sum lots of 1-second windows (because 3,600 and 7,007 are co-prime).
This is a lot of effort for both the system and the person writing the query.

Calcite could perhaps solve this generalizing `GROUP BY` syntax, but we would
be destroying the principle that an input row into a `GROUP BY` appears in
precisely one output row.

Calcite's SQL extensions for streaming queries are evolving. As we learn more
about how people wish to query streams, we plan to make the language more
expressive while remaining compatible with standard SQL and consistent with
its principles, look and feel.

## Sorting

The story for `ORDER BY` is similar to `GROUP BY`.
The syntax looks like regular SQL, but Calcite must be sure that it can deliver
timely results. It therefore requires a monotonic expression on the leading edge
of your `ORDER BY` key.

{% highlight sql %}
SELECT STREAM FLOOR(rowtime TO hour) AS rowtime, productId, orderId, units
FROM Orders
ORDER BY FLOOR(rowtime TO hour) ASC, units DESC;

  rowtime | productId | orderId | units
----------+-----------+---------+-------
 10:00:00 |        30 |       8 |    20
 10:00:00 |        30 |       5 |     4
 10:00:00 |        20 |       7 |     2
 10:00:00 |        10 |       6 |     1
 11:00:00 |        40 |      11 |    12
 11:00:00 |        10 |       9 |     6
 11:00:00 |        10 |      12 |     4
 11:00:00 |        10 |      10 |     1
{% endhighlight %}

Most queries will return results in the order that they were inserted,
because the engine is using streaming algorithms, but you should not rely on it.
For example, consider this:

{% highlight sql %}
SELECT STREAM *
FROM Orders
WHERE productId = 10
UNION ALL
SELECT STREAM *
FROM Orders
WHERE productId = 30;

  rowtime | productId | orderId | units
----------+-----------+---------+-------
 10:17:05 |        10 |       6 |     1
 10:17:00 |        30 |       5 |     4
 10:18:07 |        30 |       8 |    20
 11:02:00 |        10 |       9 |     6
 11:04:00 |        10 |      10 |     1
 11:24:11 |        10 |      12 |     4
{% endhighlight %}

The rows with `productId` = 30 are apparently out of order, probably because
the `Orders` stream was partitioned on `productId` and the partitioned streams
sent their data at different times.

If you require a particular ordering, add an explicit `ORDER BY`:

{% highlight sql %}
SELECT STREAM *
FROM Orders
WHERE productId = 10
UNION ALL
SELECT STREAM *
FROM Orders
WHERE productId = 30
ORDER BY rowtime;

  rowtime | productId | orderId | units
----------+-----------+---------+-------
 10:17:00 |        30 |       5 |     4
 10:17:05 |        10 |       6 |     1
 10:18:07 |        30 |       8 |    20
 11:02:00 |        10 |       9 |     6
 11:04:00 |        10 |      10 |     1
 11:24:11 |        10 |      12 |     4
{% endhighlight %}

Calcite will probably implement the `UNION ALL` by merging using `rowtime`,
which is only slightly less efficient.

You only need to add an `ORDER BY` to the outermost query. If you need to,
say, perform `GROUP BY` after a `UNION ALL`, Calcite will add an `ORDER BY`
implicitly, in order to make the GROUP BY algorithm possible.

## Table constructor

The `VALUES` clause creates an inline table with a given set of rows.

Streaming is disallowed. The set of rows never changes, and therefore a stream
would never return any rows.

{% highlight sql %}
> SELECT STREAM * FROM (VALUES (1, 'abc'));

ERROR: Cannot stream VALUES
{% endhighlight %}

## Sliding windows

Standard SQL features so-called "analytic functions" that can be used in the
`SELECT` clause. Unlike `GROUP BY`, these do not collapse records. For each
record that goes in, one record comes out. But the aggregate function is based
on a window of many rows.

Let's look at an example.

{% highlight sql %}
SELECT STREAM rowtime,
  productId,
  units,
  SUM(units) OVER (ORDER BY rowtime RANGE INTERVAL '1' HOUR PRECEDING) unitsLastHour
FROM Orders;
{% endhighlight %}

The feature packs a lot of power with little effort. You can have multiple
functions in the `SELECT` clause, based on multiple window specifications.

The following example returns orders whose average order size over the last
10 minutes is greater than the average order size for the last week.

{% highlight sql %}
SELECT STREAM *
FROM (
  SELECT STREAM rowtime,
    productId,
    units,
    AVG(units) OVER product (RANGE INTERVAL '10' MINUTE PRECEDING) AS m10,
    AVG(units) OVER product (RANGE INTERVAL '7' DAY PRECEDING) AS d7
  FROM Orders
  WINDOW product AS (
    ORDER BY rowtime
    PARTITION BY productId))
WHERE m10 > d7;
{% endhighlight %}

For conciseness, here we use a syntax where you partially define a window
using a `WINDOW` clause and then refine the window in each `OVER` clause.
You could also define all windows in the `WINDOW` clause, or all windows inline,
if you wish.

But the real power goes beyond syntax. Behind the scenes, this query is
maintaining two tables, and adding and removing values from sub-totals using
with FIFO queues. But you can access those tables without introducing a join
into the query.

Some other features of the windowed aggregation syntax:

* You can define windows based on row count.
* The window can reference rows that have not yet arrived.
  (The stream will wait until they have arrived).
* You can compute order-dependent functions such as `RANK` and median.

## Cascading windows

What if we want a query that returns a result for every record, like a
sliding window, but resets totals on a fixed time period, like a
tumbling window? Such a pattern is called a *cascading window*. Here
is an example:

{% highlight sql %}
SELECT STREAM rowtime,
  productId,
  units,
  SUM(units) OVER (PARTITION BY FLOOR(rowtime TO HOUR)) AS unitsSinceTopOfHour
FROM Orders;
{% endhighlight %}

It looks similar to a sliding window query, but the monotonic
expression occurs within the `PARTITION BY` clause of the window. As
the rowtime moves from from 10:59:59 to 11:00:00, `FLOOR(rowtime TO
HOUR)` changes from 10:00:00 to 11:00:00, and therefore a new
partition starts. The first row to arrive in the new hour will start a
new total; the second row will have a total that consists of two rows,
and so on.

Calcite knows that the old partition will never be used again, so
removes all sub-totals for that partition from its internal storage.

Analytic functions that using cascading and sliding windows can be
combined in the same query.

## State of the stream

Not all concepts in this article have been implemented in Calcite.
And others may be implemented in Calcite but not in a particular adapter
such as Samza SQL [<a href="#ref3">3</a>].

### Implemented
* Streaming SELECT, WHERE, GROUP BY, HAVING, UNION ALL, ORDER BY
* FLOOR and CEILING functions
* Monotonicity
* Streaming VALUES is disallowed

### Not implemented
* Stream-to-stream JOIN
* Stream-to-table JOIN
* Stream on view
* Streaming UNION ALL with ORDER BY (merge)
* Relational query on stream
* Streaming windowed aggregation (sliding and cascading windows)
* Check that STREAM in sub-queries and views is ignored
* Check that streaming ORDER BY cannot have OFFSET or LIMIT
* Limited history; at run time, check that there is sufficient history
  to run the query.

### To do in this document
* Re-visit whether you can stream VALUES
* OVER clause to define window on stream
* Windowed aggregation
* Punctuation
* Stream-to-table join
  * Stream-to-table join where table is changing
* Stream-to-stream join
* Relational queries on streams (e.g. "pie chart" query)
* Diagrams for various window types

## References

* [<a name="ref1">1</a>]
  <a href="http://ilpubs.stanford.edu:8090/758/">Arasu, Arvind and Babu,
  Shivnath and Widom, Jennifer (2003) The CQL Continuous Query
  Language: Semantic Foundations and Query Execution</a>.
* [<a name="ref2">2</a>]
  <a href="http://kafka.apache.org/documentation.html">Apache Kafka</a>.
* [<a name="ref3">3</a>] <a href="http://samza.apache.org">Apache Samza</a>.
