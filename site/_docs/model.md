---
layout: docs
title: JSON models
permalink: /docs/model.html
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

Calcite models can be represented as JSON files.
This page describes the structure of those files.

Models can also be built programmatically using the `Schema` SPI.

## Elements

### Root

{% highlight json %}
{
  version: '1.0',
  defaultSchema: 'mongo',
  schemas: [ Schema... ]
}
{% endhighlight %}

`version` (required string) must have value `1.0`.

`defaultSchema` (optional string). If specified, it is
the name (case-sensitive) of a schema defined in this model, and will
become the default schema for connections to Calcite that use this model.

`schemas` (optional list of <a href="#schema">Schema</a> elements).

### Schema

Occurs within `root.schemas`.

{% highlight json %}
{
  name: 'foodmart',
  path: ['lib'],
  cache: true,
  materializations: [ Materialization... ]
}
{% endhighlight %}

`name` (required string) is the name of the schema.

`type` (optional string, default `map`) indicates sub-type. Values are:

* `map` for <a href="#map-schema">Map Schema</a>
* `custom` for <a href="#custom-schema">Custom Schema</a>
* `jdbc` for <a href="#jdbc-schema">JDBC Schema</a>

`path` (optional list) is the SQL path that is used to
resolve functions used in this schema. If specified it must be a list,
and each element of the list must be either a string or a list of
strings. For example,

{% highlight json %}
  path: [ ['usr', 'lib'], 'lib' ]
{% endhighlight %}

declares a path with two elements: the schema '/usr/lib' and the
schema '/lib'. Most schemas are at the top level, so you can use a
string.

`materializations` (optional list of
<a href="#materialization">Materialization</a>) defines the tables
in this schema that are materializations of queries.

`cache` (optional boolean, default true) tells Calcite whether to
cache metadata (tables, functions and sub-schemas) generated
by this schema.

* If `false`, Calcite will go back to the schema each time it needs
  metadata, for example, each time it needs a list of tables in order to
  validate a query against the schema.

* If `true`, Calcite will cache the metadata the first time it reads
  it. This can lead to better performance, especially if name-matching is
  case-insensitive.

However, it also leads to the problem of cache staleness.
A particular schema implementation can override the
`Schema.contentsHaveChangedSince` method to tell Calcite
when it should consider its cache to be out of date.

Tables, functions and sub-schemas explicitly created in a schema are
not affected by this caching mechanism. They always appear in the schema
immediately, and are never flushed.

### Map Schema

Like base class <a href="#schema">Schema</a>, occurs within `root.schemas`.

{% highlight json %}
{
  name: 'foodmart',
  type: 'map',
  tables: [ Table... ],
  functions: [ Function... ]
}
{% endhighlight %}

`name`, `type`, `path`, `cache`, `materializations` inherited from
<a href="#schema">Schema</a>.

`tables` (optional list of <a href="#table">Table</a> elements)
defines the tables in this schema.

`functions` (optional list of <a href="#function">Function</a> elements)
defines the functions in this schema.

### Custom Schema

Like base class <a href="#schema">Schema</a>, occurs within `root.schemas`.

{% highlight json %}
{
  name: 'mongo',
  type: 'custom',
  factory: 'org.apache.calcite.adapter.mongodb.MongoSchemaFactory',
  operand: {
    host: 'localhost',
    database: 'test'
  }
}
{% endhighlight %}

`name`, `type`, `path`, `cache`, `materializations` inherited from
<a href="#schema">Schema</a>.

`factory` (required string) is the name of the factory class for this
schema. Must implement interface `org.apache.calcite.schema.SchemaFactory`
and have a public default constructor.

`operand` (optional map) contains attributes to be passed to the
factory.

### JDBC Schema

Like base class <a href="#schema">Schema</a>, occurs within `root.schemas`.

{% highlight json %}
{
  name: 'foodmart',
  type: 'jdbc',
  jdbcDriver: TODO,
  jdbcUrl: TODO,
  jdbcUser: TODO,
  jdbcPassword: TODO,
  jdbcCatalog: TODO,
  jdbcSchema: TODO
}
{% endhighlight %}

`name`, `type`, `path`, `cache`, `materializations` inherited from
<a href="#schema">Schema</a>.

`jdbcDriver` (optional string) is the name of the JDBC driver class. It not
specified, uses whichever class the JDBC DriverManager chooses.

`jdbcUrl` (optional string) is the JDBC connect string, for example
"jdbc:mysql://localhost/foodmart".

`jdbcUser` (optional string) is the JDBC user name.

`jdbcPassword` (optional string) is the JDBC password.

`jdbcCatalog` (optional string) is the name of the initial catalog in the JDBC
data source.

`jdbcSchema` (optional string) is the name of the initial schema in the JDBC
data source.

### Materialization

Occurs within `root.schemas.materializations`.

{% highlight json %}
{
  view: 'V',
  table: 'T',
  sql: 'select deptno, count(*) as c, sum(sal) as s from emp group by deptno'
}
{% endhighlight %}

`view` (optional string) TODO

`table` (optional string) TODO

`sql` (optional string, or list of strings that will be concatenated as a
 multi-line string) is the SQL definition of the materialization.

### Table

Occurs within `root.schemas.tables`.

{% highlight json %}
{
  name: 'sales_fact',
  columns: [ Column... ]
}
{% endhighlight %}

`name` (required string) is the name of this table. Must be unique within the schema.

`type` (optional string, default `custom`) indicates sub-type. Values are:

* `custom` for <a href="#custom-table">Custom Table</a>
* `view` for <a href="#view">View</a>

`columns` (optional list of <a href="#column">Column</a> elements)

### View

Like base class <a href="#table">Table</a>, occurs within `root.schemas.tables`.

{% highlight json %}
{
  name: 'female_emps',
  type: 'view',
  sql: "select * from emps where gender = 'F'",
  modifiable: true
}
{% endhighlight %}

`name`, `type`, `columns` inherited from <a href="#table">Table</a>.

`sql` (required string, or list of strings that will be concatenated as a
 multi-line string) is the SQL definition of the view.

`path` (optional list) is the SQL path to resolve the query. If not
specified, defaults to the current schema.

`modifiable` (optional boolean) is whether the view is modifiable.
If null or not specified, Calcite deduces whether the view is modifiable.

A view is modifiable if contains only SELECT, FROM, WHERE (no JOIN, aggregation
or sub-queries) and every column:

* is specified once in the SELECT clause; or
* occurs in the WHERE clause with a `column = literal` predicate; or
* is nullable.

The second clause allows Calcite to automatically provide the correct value for
hidden columns. It is useful in multi-tenant environments, where the `tenantId`
column is hidden, mandatory (NOT NULL), and has a constant value for a
particular view.

Errors regarding modifiable views:

* If a view is marked `modifiable: true` and is not modifiable, Calcite throws
  an error while reading the schema.
* If you submit an INSERT, UPDATE or UPSERT command to a non-modifiable view,
  Calcite throws an error when validating the statement.
* If a DML statement creates a row that would not appear in the view
  (for example, a row in `female_emps`, above, with `gender = 'M'`),
  Calcite throws an error when executing the statement.

### Custom Table

Like base class <a href="#table">Table</a>, occurs within `root.schemas.tables`.

{% highlight json %}
{
  name: 'female_emps',
  type: 'custom',
  factory: 'TODO',
  operand: {
    todo: 'TODO'
  }
}
{% endhighlight %}

`name`, `type`, `columns` inherited from <a href="#table">Table</a>.

`factory` (required string) is the name of the factory class for this
table. Must implement interface `org.apache.calcite.schema.TableFactory`
and have a public default constructor.

`operand` (optional map) contains attributes to be passed to the
factory.

### Column

Occurs within `root.schemas.tables.columns`.

{% highlight json %}
{
  name: 'empno'
}
{% endhighlight %}

`name` (required string) is the name of this column.

### Function

Occurs within `root.schemas.functions`.

{% highlight json %}
{
  name: 'MY_PLUS',
  className: 'com.example.functions.MyPlusFunction',
  methodName: 'apply',
  path: []
}
{% endhighlight %}

`name` (required string) is the name of this function.

`className` (required string) is the name of the class that implements this
function.

`methodName` (optional string) is the name of the method that implements this
function.

`path` (optional list of string) is the path for resolving this function.

### Lattice

Occurs within `root.schemas.lattices`.

{% highlight json %}
{
  name: 'star',
  sql: [
    'select 1 from "foodmart"."sales_fact_1997" as "s"',
    'join "foodmart"."product" as "p" using ("product_id")',
    'join "foodmart"."time_by_day" as "t" using ("time_id")',
    'join "foodmart"."product_class" as "pc" on "p"."product_class_id" = "pc"."product_class_id"'
  ],
  auto: false,
  algorithm: true,
  algorithmMaxMillis: 10000,
  rowCountEstimate: 86837,
  defaultMeasures: [ {
    agg: 'count'
  } ],
  tiles: [ {
    dimensions: [ 'the_year', ['t', 'quarter'] ],
    measures: [ {
      agg: 'sum',
      args: 'unit_sales'
    }, {
      agg: 'sum',
      args: 'store_sales'
    }, {
      agg: 'count'
    } ]
  } ]
}
{% endhighlight %}

`name` (required string) is the name of this lattice.

`sql` (required string, or list of strings that will be concatenated as a
multi-line string) is the SQL statement that defines the fact table, dimension
tables, and join paths for this lattice.

`auto` (optional boolean, default true) is whether to materialize tiles on need
as queries are executed.

`algorithm` (optional boolean, default false) is whether to use an optimization
algorithm to suggest and populate an initial set of tiles.

`algorithmMaxMillis` (optional long, default -1, meaning no limit) is the
maximum number of milliseconds for which to run the algorithm. After this point,
takes the best result the algorithm has come up with so far.

`rowCountEstimate` (optional double, default 1000.0) estimated number of rows in
the star

`tiles` (optional list of <a href="#tile">Tile</a> elements) is a list of
materialized aggregates to create up front.

`defaultMeasures`  (optional list of <a href="#measure">Measure</a> elements)
is a list of measures that a tile should have by default.
Any tile defined in `tiles` can still define its own measures, including
measures not on this list. If not specified, the default list of measures is
just 'count(*)':

{% highlight json %}
[ { name: 'count' } ]
{% endhighlight %}

See also: <a href="lattice.md">Lattices</a>.

### Tile

Occurs within `root.schemas.lattices.tiles`.

{% highlight json %}
{
  dimensions: [ 'the_year', ['t', 'quarter'] ],
  measures: [ {
    agg: 'sum',
    args: 'unit_sales'
  }, {
    agg: 'sum',
    args: 'store_sales'
  }, {
    agg: 'count'
  } ]
}
{% endhighlight %}

`dimensions` is a list of dimensions (columns from the star), like a `GROUP BY`
clause. Each element is either a string (the unique label of the column within
the star) or a string list (a column name qualified by a table name).

`measures` (optional list of <a href="#measure">Measure</a> elements) is a list
of aggregate functions applied to arguments. If not specified, uses the
lattice's default measure list.

### Measure

Occurs within `root.schemas.lattices.defaultMeasures`
and `root.schemas.lattices.tiles.measures`.

{% highlight json %}
{
  agg: 'sum',
  args: [ 'unit_sales' ]
}
{% endhighlight %}

`agg` is the name of an aggregate function (usually 'count', 'sum', 'min',
'max').

`args` (optional) is a column label (string), or list of zero or more columns.
If a list, each element is either a string (the unique label of the column
within the star) or a string list (a column name qualified by a table name).
