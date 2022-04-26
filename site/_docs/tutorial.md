---
layout: docs
title: Tutorial
permalink: /docs/tutorial.html
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

This is a step-by-step tutorial that shows how to build and connect to
Calcite. It uses a simple adapter that makes a directory of CSV files
appear to be a schema containing tables. Calcite does the rest, and
provides a full SQL interface.

Calcite-example-CSV is a fully functional adapter for
Calcite that reads
text files in
<a href="https://en.wikipedia.org/wiki/Comma-separated_values">CSV
(comma-separated values)</a> format. It is remarkable that a couple of
hundred lines of Java code are sufficient to provide full SQL query
capability.

CSV also serves as a template for building adapters to other
data formats. Even though there are not many lines of code, it covers
several important concepts:

* user-defined schema using SchemaFactory and Schema interfaces;
* declaring schemas in a model JSON file;
* declaring views in a model JSON file;
* user-defined table using the Table interface;
* determining the record type of a table;
* a simple implementation of Table, using the ScannableTable interface, that
  enumerates all rows directly;
* a more advanced implementation that implements FilterableTable, and can
  filter out rows according to simple predicates;
* advanced implementation of Table, using TranslatableTable, that translates
  to relational operators using planner rules.

## Download and build

You need Java (version 8, 9 or 10) and Git.

{% highlight bash %}
$ git clone https://github.com/apache/calcite.git
$ cd calcite/example/csv
$ ./sqlline
{% endhighlight %}

## First queries

Now let's connect to Calcite using
<a href="https://github.com/julianhyde/sqlline">sqlline</a>, a SQL shell
that is included in this project.

{% highlight bash %}
$ ./sqlline
sqlline> !connect jdbc:calcite:model=src/test/resources/model.json admin admin
{% endhighlight %}

(If you are running Windows, the command is `sqlline.bat`.)

Execute a metadata query:

{% highlight bash %}
sqlline> !tables
+-----------+-------------+------------+--------------+---------+----------+------------+-----------+---------------------------+----------------+
| TABLE_CAT | TABLE_SCHEM | TABLE_NAME |  TABLE_TYPE  | REMARKS | TYPE_CAT | TYPE_SCHEM | TYPE_NAME | SELF_REFERENCING_COL_NAME | REF_GENERATION |
+-----------+-------------+------------+--------------+---------+----------+------------+-----------+---------------------------+----------------+
|           | SALES       | DEPTS      | TABLE        |         |          |            |           |                           |                |
|           | SALES       | EMPS       | TABLE        |         |          |            |           |                           |                |
|           | SALES       | SDEPTS     | TABLE        |         |          |            |           |                           |                |
|           | metadata    | COLUMNS    | SYSTEM TABLE |         |          |            |           |                           |                |
|           | metadata    | TABLES     | SYSTEM TABLE |         |          |            |           |                           |                |
+-----------+-------------+------------+--------------+---------+----------+------------+-----------+---------------------------+----------------+
{% endhighlight %}

(JDBC experts, note: sqlline's <code>!tables</code> command is just executing
<a href="https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/sql/DatabaseMetaData.html#getTables(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])"><code>DatabaseMetaData.getTables()</code></a>
behind the scenes.
It has other commands to query JDBC metadata, such as <code>!columns</code> and <code>!describe</code>.)

As you can see there are 5 tables in the system: tables
<code>EMPS</code>, <code>DEPTS</code> and <code>SDEPTS</code> in the current
<code>SALES</code> schema, and <code>COLUMNS</code> and
<code>TABLES</code> in the system <code>metadata</code> schema. The
system tables are always present in Calcite, but the other tables are
provided by the specific implementation of the schema; in this case,
the <code>EMPS</code>, <code>DEPTS</code> and <code>SDEPTS</code> tables are based on the
<code>EMPS.csv.gz</code>, <code>DEPTS.csv</code> and <code>SDEPTS.csv</code> files in the
<code>resources/sales</code> directory.

Let's execute some queries on those tables, to show that Calcite is providing
a full implementation of SQL. First, a table scan:

{% highlight bash %}
sqlline> SELECT * FROM emps;
+-------+-------+--------+--------+---------------+-------+------+---------+---------+------------+
| EMPNO | NAME  | DEPTNO | GENDER |     CITY      | EMPID | AGE  | SLACKER | MANAGER |  JOINEDAT  |
+-------+-------+--------+--------+---------------+-------+------+---------+---------+------------+
| 100   | Fred  | 10     |        |               | 30    | 25   | true    | false   | 1996-08-03 |
| 110   | Eric  | 20     | M      | San Francisco | 3     | 80   |         | false   | 2001-01-01 |
| 110   | John  | 40     | M      | Vancouver     | 2     | null | false   | true    | 2002-05-03 |
| 120   | Wilma | 20     | F      |               | 1     | 5    |         | true    | 2005-09-07 |
| 130   | Alice | 40     | F      | Vancouver     | 2     | null | false   | true    | 2007-01-01 |
+-------+-------+--------+--------+---------------+-------+------+---------+---------+------------+
{% endhighlight %}

Now JOIN and GROUP BY:

{% highlight bash %}
sqlline> SELECT d.name, COUNT(*)
. . . .> FROM emps AS e JOIN depts AS d ON e.deptno = d.deptno
. . . .> GROUP BY d.name;
+------------+---------+
|    NAME    | EXPR$1  |
+------------+---------+
| Sales      | 1       |
| Marketing  | 2       |
+------------+---------+
{% endhighlight %}

Last, the VALUES operator generates a single row, and is a convenient
way to test expressions and SQL built-in functions:

{% highlight bash %}
sqlline> VALUES CHAR_LENGTH('Hello, ' || 'world!');
+---------+
| EXPR$0  |
+---------+
| 13      |
+---------+
{% endhighlight %}

Calcite has many other SQL features. We don't have time to cover them
here. Write some more queries to experiment.

## Schema discovery

Now, how did Calcite find these tables? Remember, core Calcite does not
know anything about CSV files. (As a "database without a storage
layer", Calcite doesn't know about any file formats.) Calcite knows about
those tables because we told it to run code in the calcite-example-csv
project.

There are a couple of steps in that chain. First, we define a schema
based on a schema factory class in a model file. Then the schema
factory creates a schema, and the schema creates several tables, each
of which knows how to get data by scanning a CSV file. Last, after
Calcite has parsed the query and planned it to use those tables, Calcite
invokes the tables to read the data as the query is being
executed. Now let's look at those steps in more detail.

On the JDBC connect string we gave the path of a model in JSON
format. Here is the model:

{% highlight json %}
{
  version: '1.0',
  defaultSchema: 'SALES',
  schemas: [
    {
      name: 'SALES',
      type: 'custom',
      factory: 'org.apache.calcite.adapter.csv.CsvSchemaFactory',
      operand: {
        directory: 'sales'
      }
    }
  ]
}
{% endhighlight %}

The model defines a single schema called 'SALES'. The schema is
powered by a plugin class,
<a href="{{ site.sourceRoot }}/example/csv/src/main/java/org/apache/calcite/adapter/csv/CsvSchemaFactory.java">org.apache.calcite.adapter.csv.CsvSchemaFactory</a>,
which is part of the
calcite-example-csv project and implements the Calcite interface
<a href="{{ site.apiRoot }}/org/apache/calcite/schema/SchemaFactory.html">SchemaFactory</a>.
Its <code>create</code> method instantiates a
schema, passing in the <code>directory</code> argument from the model file:

{% highlight java %}
public Schema create(SchemaPlus parentSchema, String name,
    Map<String, Object> operand) {
  final String directory = (String) operand.get("directory");
  final File base =
      (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
  File directoryFile = new File(directory);
  if (base != null && !directoryFile.isAbsolute()) {
    directoryFile = new File(base, directory);
  }
  String flavorName = (String) operand.get("flavor");
  CsvTable.Flavor flavor;
  if (flavorName == null) {
    flavor = CsvTable.Flavor.SCANNABLE;
  } else {
    flavor = CsvTable.Flavor.valueOf(flavorName.toUpperCase(Locale.ROOT));
  }
  return new CsvSchema(directoryFile, flavor);
}
{% endhighlight %}

Driven by the model, the schema factory instantiates a single schema
called 'SALES'.  The schema is an instance of
<a href="{{ site.sourceRoot }}/example/csv/src/main/java/org/apache/calcite/adapter/csv/CsvSchema.java">org.apache.calcite.adapter.csv.CsvSchema</a>
and implements the Calcite interface
<a href="{{ site.apiRoot }}/org/apache/calcite/schema/Schema.html">Schema</a>.

A schema's job is to produce a list of tables. (It can also list sub-schemas and
table-functions, but these are advanced features and calcite-example-csv does
not support them.) The tables implement Calcite's
<a href="{{ site.apiRoot }}/org/apache/calcite/schema/Table.html">Table</a>
interface. <code>CsvSchema</code> produces tables that are instances of
<a href="{{ site.sourceRoot }}/example/csv/src/main/java/org/apache/calcite/adapter/csv/CsvTable.java">CsvTable</a>
and its sub-classes.

Here is the relevant code from <code>CsvSchema</code>, overriding the
<code><a href="{{ site.apiRoot }}/org/apache/calcite/schema/impl/AbstractSchema.html#getTableMap()">getTableMap()</a></code>
method in the <code>AbstractSchema</code> base class.

{% highlight java %}
private Map<String, Table> createTableMap() {
  // Look for files in the directory ending in ".csv", ".csv.gz", ".json",
  // ".json.gz".
  final Source baseSource = Sources.of(directoryFile);
  File[] files = directoryFile.listFiles((dir, name) -> {
    final String nameSansGz = trim(name, ".gz");
    return nameSansGz.endsWith(".csv")
        || nameSansGz.endsWith(".json");
  });
  if (files == null) {
    System.out.println("directory " + directoryFile + " not found");
    files = new File[0];
  }
  // Build a map from table name to table; each file becomes a table.
  final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
  for (File file : files) {
    Source source = Sources.of(file);
    Source sourceSansGz = source.trim(".gz");
    final Source sourceSansJson = sourceSansGz.trimOrNull(".json");
    if (sourceSansJson != null) {
      final Table table = new JsonScannableTable(source);
      builder.put(sourceSansJson.relative(baseSource).path(), table);
    }
    final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
    if (sourceSansCsv != null) {
      final Table table = createTable(source);
      builder.put(sourceSansCsv.relative(baseSource).path(), table);
    }
  }
  return builder.build();
}

/** Creates different sub-type of table based on the "flavor" attribute. */
private Table createTable(Source source) {
  switch (flavor) {
  case TRANSLATABLE:
    return new CsvTranslatableTable(source, null);
  case SCANNABLE:
    return new CsvScannableTable(source, null);
  case FILTERABLE:
    return new CsvFilterableTable(source, null);
  default:
    throw new AssertionError("Unknown flavor " + this.flavor);
  }
}
{% endhighlight %}

The schema scans the directory, finds all files with the appropriate extension,
and creates tables for them. In this case, the directory
is <code>sales</code> and contains files
<code>EMPS.csv.gz</code>, <code>DEPTS.csv</code> and <code>SDEPTS.csv</code>, which these become
the tables <code>EMPS</code>, <code>DEPTS</code> and <code>SDEPTS</code>.

## Tables and views in schemas

Note how we did not need to define any tables in the model; the schema
generated the tables automatically.

You can define extra tables,
beyond those that are created automatically,
using the <code>tables</code> property of a schema.

Let's see how to create
an important and useful type of table, namely a view.

A view looks like a table when you are writing a query, but it doesn't store data.
It derives its result by executing a query.
The view is expanded while the query is being planned, so the query planner
can often perform optimizations like removing expressions from the SELECT
clause that are not used in the final result.

Here is a schema that defines a view:

{% highlight json %}
{
  version: '1.0',
  defaultSchema: 'SALES',
  schemas: [
    {
      name: 'SALES',
      type: 'custom',
      factory: 'org.apache.calcite.adapter.csv.CsvSchemaFactory',
      operand: {
        directory: 'sales'
      },
      tables: [
        {
          name: 'FEMALE_EMPS',
          type: 'view',
          sql: 'SELECT * FROM emps WHERE gender = \'F\''
        }
      ]
    }
  ]
}
{% endhighlight %}

The line <code>type: 'view'</code> tags <code>FEMALE_EMPS</code> as a view,
as opposed to a regular table or a custom table.
Note that single-quotes within the view definition are escaped using a
back-slash, in the normal way for JSON.

JSON doesn't make it easy to author long strings, so Calcite supports an
alternative syntax. If your view has a long SQL statement, you can instead
supply a list of lines rather than a single string:

{% highlight json %}
{
  name: 'FEMALE_EMPS',
  type: 'view',
  sql: [
    'SELECT * FROM emps',
    'WHERE gender = \'F\''
  ]
}
{% endhighlight %}

Now we have defined a view, we can use it in queries just as if it were a table:

{% highlight sql %}
sqlline> SELECT e.name, d.name FROM female_emps AS e JOIN depts AS d on e.deptno = d.deptno;
+--------+------------+
|  NAME  |    NAME    |
+--------+------------+
| Wilma  | Marketing  |
+--------+------------+
{% endhighlight %}

## Custom tables

Custom tables are tables whose implementation is driven by user-defined code.
They don't need to live in a custom schema.

There is an example in <code>model-with-custom-table.json</code>:

{% highlight json %}
{
  version: '1.0',
  defaultSchema: 'CUSTOM_TABLE',
  schemas: [
    {
      name: 'CUSTOM_TABLE',
      tables: [
        {
          name: 'EMPS',
          type: 'custom',
          factory: 'org.apache.calcite.adapter.csv.CsvTableFactory',
          operand: {
            file: 'sales/EMPS.csv.gz',
            flavor: "scannable"
          }
        }
      ]
    }
  ]
}
{% endhighlight %}

We can query the table in the usual way:

{% highlight sql %}
sqlline> !connect jdbc:calcite:model=src/test/resources/model-with-custom-table.json admin admin
sqlline> SELECT empno, name FROM custom_table.emps;
+--------+--------+
| EMPNO  |  NAME  |
+--------+--------+
| 100    | Fred   |
| 110    | Eric   |
| 110    | John   |
| 120    | Wilma  |
| 130    | Alice  |
+--------+--------+
{% endhighlight %}

The schema is a regular one, and contains a custom table powered by
<a href="{{ site.sourceRoot }}/example/csv/src/main/java/org/apache/calcite/adapter/csv/CsvTableFactory.java">org.apache.calcite.adapter.csv.CsvTableFactory</a>,
which implements the Calcite interface
<a href="{{ site.apiRoot }}/org/apache/calcite/schema/TableFactory.html">TableFactory</a>.
Its <code>create</code> method instantiates a <code>CsvScannableTable</code>,
passing in the <code>file</code> argument from the model file:

{% highlight java %}
public CsvTable create(SchemaPlus schema, String name,
    Map<String, Object> operand, @Nullable RelDataType rowType) {
  String fileName = (String) operand.get("file");
  final File base =
      (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
  final Source source = Sources.file(base, fileName);
  final RelProtoDataType protoRowType =
      rowType != null ? RelDataTypeImpl.proto(rowType) : null;
  return new CsvScannableTable(source, protoRowType);
}
{% endhighlight %}

Implementing a custom table is often a simpler alternative to implementing
a custom schema. Both approaches might end up creating a similar implementation
of the <code>Table</code> interface, but for the custom table you don't
need to implement metadata discovery. (<code>CsvTableFactory</code>
creates a <code>CsvScannableTable</code>, just as <code>CsvSchema</code> does,
but the table implementation does not scan the filesystem for .csv files.)

Custom tables require more work for the author of the model (the author
needs to specify each table and its file explicitly) but also give the author
more control (say, providing different parameters for each table).

## Comments in models

Models can include comments using `/* ... */` and `//` syntax:

{% highlight json %}
{
  version: '1.0',
  /* Multi-line
     comment. */
  defaultSchema: 'CUSTOM_TABLE',
  // Single-line comment.
  schemas: [
    ..
  ]
}
{% endhighlight %}

(Comments are not standard JSON, but are a harmless extension.)

## Optimizing queries using planner rules

The table implementations we have seen so far are fine as long as the tables
don't contain a great deal of data. But if your customer table has, say, a
hundred columns and a million rows, you would rather that the system did not
retrieve all of the data for every query. You would like Calcite to negotiate
with the adapter and find a more efficient way of accessing the data.

This negotiation is a simple form of query optimization. Calcite supports query
optimization by adding <i>planner rules</i>. Planner rules operate by
looking for patterns in the query parse tree (for instance a project on top
of a certain kind of table), and replacing the matched nodes in the tree by
a new set of nodes which implement the optimization.

Planner rules are also extensible, like schemas and tables. So, if you have a
data store that you want to access via SQL, you first define a custom table or
schema, and then you define some rules to make the access efficient.

To see this in action, let's use a planner rule to access
a subset of columns from a CSV file. Let's run the same query against two very
similar schemas:

{% highlight sql %}
sqlline> !connect jdbc:calcite:model=src/test/resources/model.json admin admin
sqlline> explain plan for select name from emps;
+-----------------------------------------------------+
| PLAN                                                |
+-----------------------------------------------------+
| EnumerableCalc(expr#0..9=[{inputs}], NAME=[$t1])    |
|   EnumerableTableScan(table=[[SALES, EMPS]])        |
+-----------------------------------------------------+
sqlline> !connect jdbc:calcite:model=src/test/resources/smart.json admin admin
sqlline> explain plan for select name from emps;
+-----------------------------------------------------+
| PLAN                                                |
+-----------------------------------------------------+
| CsvTableScan(table=[[SALES, EMPS]], fields=[[1]])   |
+-----------------------------------------------------+
{% endhighlight %}

What causes the difference in plan? Let's follow the trail of evidence. In the
<code>smart.json</code> model file, there is just one extra line:

{% highlight json %}
flavor: "translatable"
{% endhighlight %}

This causes a <code>CsvSchema</code> to be created with
<code>flavor = TRANSLATABLE</code>,
and its <code>createTable</code> method creates instances of
<a href="{{ site.sourceRoot }}/example/csv/src/main/java/org/apache/calcite/adapter/csv/CsvTranslatableTable.java">CsvTranslatableTable</a>
rather than a <code>CsvScannableTable</code>.

<code>CsvTranslatableTable</code> implements the
<code><a href="{{ site.apiRoot }}/org/apache/calcite/schema/TranslatableTable.html#toRel()">TranslatableTable.toRel()</a></code>
method to create
<a href="{{ site.sourceRoot }}/example/csv/src/main/java/org/apache/calcite/adapter/csv/CsvTableScan.java">CsvTableScan</a>.
Table scans are the leaves of a query operator tree.
The usual implementation is
<code><a href="{{ site.apiRoot }}/org/apache/calcite/adapter/enumerable/EnumerableTableScan.html">EnumerableTableScan</a></code>,
but we have created a distinctive sub-type that will cause rules to fire.

Here is the rule in its entirety:

{% highlight java %}
public class CsvProjectTableScanRule
    extends RelRule<CsvProjectTableScanRule.Config> {

  /** Creates a CsvProjectTableScanRule. */
  protected CsvProjectTableScanRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final CsvTableScan scan = call.rel(1);
    int[] fields = getProjectFields(project.getProjects());
    if (fields == null) {
      // Project contains expressions more complex than just field references.
      return;
    }
    call.transformTo(
        new CsvTableScan(
            scan.getCluster(),
            scan.getTable(),
            scan.csvTable,
            fields));
  }

  private static int[] getProjectFields(List<RexNode> exps) {
    final int[] fields = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        fields[i] = ((RexInputRef) exp).getIndex();
      } else {
        return null; // not a simple projection
      }
    }
    return fields;
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCsvProjectTableScanRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(CsvTableScan.class).noInputs()))
        .build();

    @Override default CsvProjectTableScanRule toRule() {
      return new CsvProjectTableScanRule(this);
    }
  }
} 
{% endhighlight %}

The default instance of the rule resides in the `CsvRules` holder class:

{% highlight java %}
public abstract class CsvRules {
  public static final CsvProjectTableScanRule PROJECT_SCAN =
      CsvProjectTableScanRule.Config.DEFAULT.toRule();
}
{% endhighlight %}

The call to the `withOperandSupplier` method in the default configuration
(the `DEFAULT` field in `interface Config`) declares the pattern of relational
expressions that will cause the rule to fire. The planner will invoke the rule
if it sees a `LogicalProject` whose sole input is a `CsvTableScan` with no
inputs.

Variants of the rule are possible. For example, a different rule instance
might instead match a `EnumerableProject` on a `CsvTableScan`.

The <code>onMatch</code> method generates a new relational expression and calls
<code><a href="{{ site.apiRoot }}/org/apache/calcite/plan/RelOptRuleCall.html#transformTo(org.apache.calcite.rel.RelNode)">RelOptRuleCall.transformTo()</a></code>
to indicate that the rule has fired successfully.

## The query optimization process

There's a lot to say about how clever Calcite's query planner is, but we won't
say it here. The cleverness is designed to take the burden off you, the writer
of planner rules.

First, Calcite doesn't fire rules in a prescribed order. The query optimization
process follows many branches of a branching tree, just like a chess playing
program examines many possible sequences of moves. If rules A and B both match a
given section of the query operator tree, then Calcite can fire both.

Second, Calcite uses cost in choosing between plans, but the cost model doesn't
prevent rules from firing which may seem to be more expensive in the short term.

Many optimizers have a linear optimization scheme. Faced with a choice between
rule A and rule B, as above, such an optimizer needs to choose immediately. It
might have a policy such as "apply rule A to the whole tree, then apply rule B
to the whole tree", or apply a cost-based policy, applying the rule that
produces the cheaper result.

Calcite doesn't require such compromises.
This makes it simple to combine various sets of rules.
If, say you want to combine rules to recognize materialized views with rules to
read from CSV and JDBC source systems, you just give Calcite the set of all
rules and tell it to go at it.

Calcite does use a cost model. The cost model decides which plan to ultimately
use, and sometimes to prune the search tree to prevent the search space from
exploding, but it never forces you to choose between rule A and rule B. This is
important, because it avoids falling into local minima in the search space that
are not actually optimal.

Also (you guessed it) the cost model is pluggable, as are the table and query
operator statistics it is based upon. But that can be a subject for later.

## JDBC adapter

The JDBC adapter maps a schema in a JDBC data source as a Calcite schema.

For example, this schema reads from a MySQL "foodmart" database:

{% highlight json %}
{
  version: '1.0',
  defaultSchema: 'FOODMART',
  schemas: [
    {
      name: 'FOODMART',
      type: 'custom',
      factory: 'org.apache.calcite.adapter.jdbc.JdbcSchema$Factory',
      operand: {
        jdbcDriver: 'com.mysql.jdbc.Driver',
        jdbcUrl: 'jdbc:mysql://localhost/foodmart',
        jdbcUser: 'foodmart',
        jdbcPassword: 'foodmart'
      }
    }
  ]
}
{% endhighlight %}

(The FoodMart database will be familiar to those of you who have used
the Mondrian OLAP engine, because it is Mondrian's main test data
set. To load the data set, follow <a
href="https://mondrian.pentaho.com/documentation/installation.php#2_Set_up_test_data">Mondrian's
installation instructions</a>.)

<b>Current limitations</b>: The JDBC adapter currently only pushes
down table scan operations; all other processing (filtering, joins,
aggregations and so forth) occurs within Calcite. Our goal is to push
down as much processing as possible to the source system, translating
syntax, data types and built-in functions as we go. If a Calcite query
is based on tables from a single JDBC database, in principle the whole
query should go to that database. If tables are from multiple JDBC
sources, or a mixture of JDBC and non-JDBC, Calcite will use the most
efficient distributed query approach that it can.

## The cloning JDBC adapter

The cloning JDBC adapter creates a hybrid database. The data is
sourced from a JDBC database but is read into in-memory tables the
first time each table is accessed. Calcite evaluates queries based on
those in-memory tables, effectively a cache of the database.

For example, the following model reads tables from a MySQL
"foodmart" database:

{% highlight json %}
{
  version: '1.0',
  defaultSchema: 'FOODMART_CLONE',
  schemas: [
    {
      name: 'FOODMART_CLONE',
      type: 'custom',
      factory: 'org.apache.calcite.adapter.clone.CloneSchema$Factory',
      operand: {
        jdbcDriver: 'com.mysql.jdbc.Driver',
        jdbcUrl: 'jdbc:mysql://localhost/foodmart',
        jdbcUser: 'foodmart',
        jdbcPassword: 'foodmart'
      }
    }
  ]
}
{% endhighlight %}

Another technique is to build a clone schema on top of an existing
schema. You use the <code>source</code> property to reference a schema
defined earlier in the model, like this:

{% highlight json %}
{
  version: '1.0',
  defaultSchema: 'FOODMART_CLONE',
  schemas: [
    {
      name: 'FOODMART',
      type: 'custom',
      factory: 'org.apache.calcite.adapter.jdbc.JdbcSchema$Factory',
      operand: {
        jdbcDriver: 'com.mysql.jdbc.Driver',
        jdbcUrl: 'jdbc:mysql://localhost/foodmart',
        jdbcUser: 'foodmart',
        jdbcPassword: 'foodmart'
      }
    },
    {
      name: 'FOODMART_CLONE',
      type: 'custom',
      factory: 'org.apache.calcite.adapter.clone.CloneSchema$Factory',
      operand: {
        source: 'FOODMART'
      }
    }
  ]
}
{% endhighlight %}

You can use this approach to create a clone schema on any type of
schema, not just JDBC.

The cloning adapter isn't the be-all and end-all. We plan to develop
more sophisticated caching strategies, and a more complete and
efficient implementation of in-memory tables, but for now the cloning
JDBC adapter shows what is possible and allows us to try out our
initial implementations.

## Further topics

There are many other ways to extend Calcite not yet described in this tutorial.
The [adapter specification](adapter.html) describes the APIs involved.
