Tutorial
========

Optiq-csv is a fully functional adapter for <a
href="https://github.com/julianhyde/optiq">Optiq</a> that reads text
files in <a
href="http://en.wikipedia.org/wiki/Comma-separated_values">CSV
(comma-separated values)</a> format. It is remarkable that a couple of
hundred lines of Java code are sufficient to provide full SQL query
capability.

Optiq-csv also serves as a template for building adapters to other
data formats. Even though there are not many lines of code, it covers
several important concepts:
* user-defined schema using SchemaFactory and Schema interfaces;
* declaring schemas in a model JSON file;
* declaring views in a model JSON file;
* user-defined table using the Table interface;
* determining the record type of a table;
* a simple implementation of Table that enumerates all rows directly;
* advanced implementation of Table that translates to relational operators using planner rules.

## First queries

Without further ado, let's get started.

If you haven't already installed, follow the instructions in the <a href="README.md">README</a>.
It's just two commands: <code>git clone</code> followed <code>mvn install</code>.

Now let's connect to Optiq using 
<a href="https://github.com/julianhyde/sqlline">sqlline</a>, a SQL shell
that is included in the optiq-csv github project.

```bash
$ ./sqlline
sqlline> !connect jdbc:optiq:model=target/test-classes/model.json admin admin
```

Execute a metadata query:

```bash
sqlline> !tables
+------------+--------------+-------------+---------------+----------+------+
| TABLE_CAT  | TABLE_SCHEM  | TABLE_NAME  |  TABLE_TYPE   | REMARKS  | TYPE |
+------------+--------------+-------------+---------------+----------+------+
| null       | SALES        | DEPTS       | TABLE         | null     | null |
| null       | SALES        | EMPS        | TABLE         | null     | null |
| null       | metadata     | COLUMNS     | SYSTEM_TABLE  | null     | null |
| null       | metadata     | TABLES      | SYSTEM_TABLE  | null     | null |
+------------+--------------+-------------+---------------+----------+------+
```

(JDBC experts, note: sqlline's <code>!tables</code> command is just executing
<a href="http://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html#getTables(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])"><code>DatabaseMetaData.getTables()</code></a>
behind the scenes.
It has other commands to query JDBC metadata, such as <code>!columns</code> and <code>!describe</code>.)

As you can see there are 4 tables in the system: tables
<code>EMPS</code> and <code>DEPTS</code> in the current
<code>SALES</code> schema, and <code>COLUMNS</code> and
<code>TABLES</code> in the system <code>metadata</code> schema. The
system tables are always present in Optiq, but the other tables are
provided by the specific implementation of the schema; in this case,
the <code>EMPS</code> and <code>DEPTS</code> tables are based on the
<code>EMPS.csv</code> and <code>DEPTS.csv</code> files in the
<code>target/test-classes</code> directory.

Let's execute some queries on those tables, to show that Optiq is providing
a full implementation of SQL. First, a table scan:

```bash
sqlline> SELECT * FROM emps;
+--------+--------+---------+---------+----------------+--------+-------+---+
| EMPNO  |  NAME  | DEPTNO  | GENDER  |      CITY      | EMPID  |  AGE  | S |
+--------+--------+---------+---------+----------------+--------+-------+---+
| 100    | Fred   | 10      |         |                | 30     | 25    | t |
| 110    | Eric   | 20      | M       | San Francisco  | 3      | 80    | n |
| 110    | John   | 40      | M       | Vancouver      | 2      | null  | f |
| 120    | Wilma  | 20      | F       |                | 1      | 5     | n |
| 130    | Alice  | 40      | F       | Vancouver      | 2      | null  | f |
+--------+--------+---------+---------+----------------+--------+-------+---+
```

Now JOIN and GROUP BY:

```bash
sqlline> SELECT d.name, COUNT(*)
. . . .> FROM emps AS e JOIN depts AS d ON e.deptno = d.deptno
. . . .> GROUP BY d.name;
+------------+---------+
|    NAME    | EXPR$1  |
+------------+---------+
| Sales      | 1       |
| Marketing  | 2       |
+------------+---------+
```

Last, the VALUES operator generates a single row, and is a convenient
way to test expressions and SQL built-in functions:

```bash
sqlline> VALUES CHAR_LENGTH('Hello, ' || 'world!');
+---------+
| EXPR$0  |
+---------+
| 13      |
+---------+
```

Optiq has many other SQL features. We don't have time to cover them
here. Write some more queries to experiment.

## Schema discovery

Now, how did Optiq find these tables? Remember, core Optiq does not
know anything about CSV files. (As a "database without a storage
layer", Optiq doesn't know about any file formats.) Optiq knows about
those tables because we told it to run code in the optiq-csv
project.

There are a couple of steps in that chain. First, we define a schema
based on a schema factory class in a model file. Then the schema
factory creates a schema, and the schema creates several tables, each
of which knows how to get data by scanning a CSV file. Last, after
Optiq has parsed the query and planned it to use those tables, Optiq
invokes the tables to read the data as the query is being
executed. Now let's look at those steps in more detail.

On the JDBC connect string we gave the path of a model in JSON
format. Here is the model:

```json
{
  version: '1.0',
  defaultSchema: 'SALES',
  schemas: [
    {
      name: 'SALES',
      type: 'custom',
      factory: 'net.hydromatic.optiq.impl.csv.CsvSchemaFactory',
      operand: {
        directory: 'target/test-classes/sales'
      }
    }
  ]
}
```

The model defines a single schema called 'SALES'. The schema is
powered by a plugin class,
<a href="https://github.com/julianhyde/optiq-csv/blob/master/src/main/java/net/hydromatic/optiq/impl/csv/CsvSchemaFactory.java">net.hydromatic.optiq.impl.csv.CsvSchemaFactory</a>, which is part of the
optiq-csv project and implements the Optiq interface
<a href="http://www.hydromatic.net/optiq/apidocs/net/hydromatic/optiq/SchemaFactory.html">SchemaFactory</a>. Its <code>create</code> method instantiates a
schema, passing in the <code>directory</code> argument from the model file:

```java
public Schema create(MutableSchema parentSchema, String name,
    Map<String, Object> operand) {
  Map map = (Map) operand;
  String directory = (String) map.get("directory");
  Boolean smart = (Boolean) map.get("smart");
  final CsvSchema schema =
      new CsvSchema(
          parentSchema,
          new File(directory),
          parentSchema.getSubSchemaExpression(name, CsvSchema.class),
          smart != null && smart);
  parentSchema.addSchema(name, schema);
  return schema;
}
```

Driven by the model, the schema factory instantiates a single schema
called 'SALES'.  The schema is an instance of
<a href="https://github.com/julianhyde/optiq-csv/blob/master/src/main/java/net/hydromatic/optiq/impl/csv/CsvSchema.java">net.hydromatic.optiq.impl.csv.CsvSchema</a>
and implements the Optiq interface <a
href="http://www.hydromatic.net/optiq/apidocs/net/hydromatic/optiq/Schema.html">Schema</a>.

A schema's job is to produce a list of tables. (It can also list sub-schemas and
table-functions, but these are advanced features and optiq-csv does
not support them.) The tables implement Optiq's <a
href="http://www.hydromatic.net/optiq/apidocs/net/hydromatic/optiq/Table.html">Table</a> interface. <code>CsvSchema</code> produces tables that are instances of
<a href="https://github.com/julianhyde/optiq-csv/blob/master/src/main/java/net/hydromatic/optiq/impl/csv/CsvTable.java">CsvTable</a>.

Here is the relevant code from <code>CsvSchema</code>, overriding the
<code><a href="http://www.hydromatic.net/optiq/apidocs/net/hydromatic/optiq/impl/java/MapSchema.html#initialTables()">initialTables()</a></code>
method in the <code>MapSchema</code> base class.

```java
protected Collection<TableInSchema> initialTables() {
  final List<TableInSchema> list = new ArrayList<TableInSchema>();
  File[] files = directoryFile.listFiles(
      new FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.endsWith(".csv");
        }
      });
  for (File file : files) {
    String tableName = file.getName();
    if (tableName.endsWith(".csv")) {
      tableName = tableName.substring(
          0, tableName.length() - ".csv".length());
    }
    final List<CsvFieldType> fieldTypes = new ArrayList<CsvFieldType>();
    final RelDataType rowType =
        CsvTable.deduceRowType(typeFactory, file, fieldTypes);
    final CsvTable table;
    if (smart) {
      table = new CsvSmartTable(this, tableName, file, rowType, fieldTypes);
    } else {
      table = new CsvTable(this, tableName, file, rowType, fieldTypes);
    }
    list.add(
        new TableInSchemaImpl(this, tableName, TableType.TABLE, table));
  }
  return list;
}
```

The schema scans the directory and finds all files whose name ends
with ".csv" and creates tables for them. In this case, the directory
is <code>target/test-classes/sales</code> and contains files
<code>EMPS.csv</code> and <code>DEPTS.csv</code>, which these become
the tables <code>EMPS</code> and <code>DEPTS</code>.

## Tables and views in schemas

Note how we did not need to define any tables in the model; the schema
generated the tables automatically. 

Some schema types allow you to define extra tables,
beyond those that are created automatically,
using the <code>tables</code> property of a schema.

(Specifically, Optiq checks whether the schema
returned from <code>SchemaFactory.create</code> implements the
<code><a href="http://www.hydromatic.net/optiq/apidocs/net/hydromatic/optiq/MutableSchema.html">MutableSchema</a></code>
interface. If it does, you can define tables in the model.
Further, if the schema extends the
<code><a href="http://www.hydromatic.net/optiq/apidocs/net/hydromatic/optiq/impl/java/MapSchema.html">MapSchema</a></code>
class, Optiq will call
<code><a href="http://www.hydromatic.net/optiq/apidocs/net/hydromatic/optiq/impl/java/MapSchema.html#initialize()">MapSchema.initialize()</a></code>
to create the automatic tables,
then go ahead and create the explicit tables.)

<code>CsvSchema</code> allows explicit tables, so let's see how to create
an important and useful type of table, namely a view.

A view looks like a table when you are writing a query, but it doesn't store data.
It derives its result by executing a query.
The view is expanded while the query is being planned, so the query planner
can often perform optimizations like removing expressions from the SELECT
clause that are not used in the final result.

Here is a schema that defines a view:

```json
{
  version: '1.0',
  defaultSchema: 'SALES',
  schemas: [
    {
      name: 'SALES',
      type: 'custom',
      factory: 'net.hydromatic.optiq.impl.csv.CsvSchemaFactory',
      operand: {
        directory: 'target/test-classes/sales'
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
```

The line <code>type: 'view'</code> tags <code>FEMALE_EMPS</code> as a view, as opposed to a regular table
or a custom table. Note that single-quotes within the view definition are escaped using a
back-slash, in the normal way for JSON.

We can now execute queries using that view just as if it were a table:

```sql
sqlline> SELECT e.name, d.name FROM female_emps AS e JOIN depts AS d on e.deptno = d.deptno;
+--------+------------+
|  NAME  |    NAME    |
+--------+------------+
| Wilma  | Marketing  |
+--------+------------+
```

## Custom tables

Custom tables are tables whose implementation is driven by user-defined code.
They don't need to live in a custom schema.

There is an example in <code>model-with-custom-table.json</code>:

```json
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
          factory: 'net.hydromatic.optiq.impl.csv.CsvTableFactory',
          operand: {
            file: 'target/test-classes/sales/EMPS.csv',
            smart: false
          }
        }
      ]
    }
  ]
}
```

We can query the table it in the usual way:

```sql
sqlline> !connect jdbc:optiq:model=target/test-classes/model-with-custom-table.json admin admin
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
```

The schema is a regular one, and contains a custom table powered by
<a href="https://github.com/julianhyde/optiq-csv/blob/master/src/main/java/net/hydromatic/optiq/impl/csv/CsvTableFactory.java">net.hydromatic.optiq.impl.csv.CsvTableFactory</a>,
which implements the Optiq interface
<a href="http://www.hydromatic.net/optiq/apidocs/net/hydromatic/optiq/TableFactory.html">TableFactory</a>.
Its <code>create</code> method instantiates a
table, passing in the <code>file</code> argument from the model file:

```java
public CsvTable create(Schema schema, String name,
    Map<String, Object> map, RelDataType rowType) {
  String fileName = (String) map.get("file");
  Boolean smart = (Boolean) map.get("smart");
  final File file = new File(fileName);
  final List<CsvFieldType> list = new ArrayList<CsvFieldType>();
  final RelDataType rowType2 =
      CsvTable.deduceRowType(schema.getTypeFactory(), file, list);
  final RelDataType rowType3 = rowType != null ? rowType : rowType2;
  return new CsvTable(schema, name, file, rowType3, list);
}
```

Implementing a custom table is often a simpler alternative to implementing
a custom schema. Both approaches might end up creating a similar implementation
of the <code>Table</code> interface, but for the custom table you don't
need to implement metadata discovery. (<code>CsvTableFactory</code>
creates a <code>CsvTable</code>, just as <code>CsvSchema</code> does,
but the table implementation does not scan the filesystem for .csv files.)

Custom tables require more work for the author of the model (the author
needs to specify each table and its file explicitly) but also give the author
more control (say, providing different parameters for each table).

## Optimizing queries using planner rules

The table implementations we have seen so far are fine as long as the tables
don't contain a great deal of data. But if your customer table has, say, a
hundred columns and a million rows, you would rather that the system did not
retrieve all of the data for every query. You would like Optiq to negotiate
with the adapter and find a more efficient way of accessing the data.

This negotiation is a simple form of query optimization. Optiq supports query
optimization by adding <i>planner rules</i>. Planner rules operate by
looking for patterns in the query parse tree (for instance a project on top
of a certain kind of table), and

Planner rules are also extensible, like schemas and tables. So, if you have a
data store that you want to access via SQL, you first define a custom table or
schema, and then you define some rules to make the access efficient.

To see this in action, let's use a planner rule to access
a subset of columns from a CSV file. Let's run the same query against two very
similar schemas:

```sql
sqlline> !connect jdbc:optiq:model=target/test-classes/model.json admin admin
sqlline> explain plan for select name from emps;
+-----------------------------------------------------+
| PLAN                                                |
+-----------------------------------------------------+
| EnumerableCalcRel(expr#0..9=[{inputs}], NAME=[$t1]) |
|   EnumerableTableAccessRel(table=[[SALES, EMPS]])   |
+-----------------------------------------------------+
sqlline> !connect jdbc:optiq:model=target/test-classes/smart.json admin admin
sqlline> explain plan for select name from emps;
+-----------------------------------------------------+
| PLAN                                                |
+-----------------------------------------------------+
| EnumerableCalcRel(expr#0..9=[{inputs}], NAME=[$t1]) |
|   CsvTableScan(table=[[SALES, EMPS]])               |
+-----------------------------------------------------+
```

What causes the difference in plan? Let's follow the trail of evidence. In the
<code>smart.json</code> model file, there is just one extra line:

```json
smart: true
```

This causes <code>CsvSchema</code> to be created with <code>smart = true</code>,
and its <code>createTable</code> method creates instances of
<a href="https://github.com/julianhyde/optiq-csv/blob/master/src/main/java/net/hydromatic/optiq/impl/csv/CsvSmartTable.java">CsvSmartTable</a>
rather than a <code>CsvTable</code>.

<code>CsvSmartTable</code> overrides the
<code><a href="http://www.hydromatic.net/optiq/apidocs/net/hydromatic/optiq/TranslatableTable#toRel()">TranslatableTable.toRel()</a></code>
method to create
<a href="https://github.com/julianhyde/optiq-csv/blob/master/src/main/java/net/hydromatic/optiq/impl/csv/CsvTableScan.java">CsvTableScan</a>.
Table scans are the leaves of a query operator tree.
The usual implementation is
<code><a href="http://www.hydromatic.net/optiq/apidocs/net/hydromatic/optiq/impl/java/JavaRules.EnumerableTableAccessRel.html">EnumerableTableAccessRel</a></code>,
but we have created a distinctive sub-type that will cause rules to fire.

Here is the rule in its entirety:

```java
public class CsvPushProjectOntoTableRule extends RelOptRule {
  public static final CsvPushProjectOntoTableRule INSTANCE =
    new CsvPushProjectOntoTableRule();

  private CsvPushProjectOntoTableRule() {
    super(
        new RelOptRuleOperand(
            ProjectRel.class,
            new RelOptRuleOperand(
                CsvTableScan.class)),
        "CsvPushProjectOntoTableRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectRel project = (ProjectRel) call.getRels()[0];
    final CsvTableScan scan = (CsvTableScan) call.getRels()[1];
    int[] fields = getProjectFields(project.getProjectExps());
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

  private int[] getProjectFields(RexNode[] exps) {
    final int[] fields = new int[exps.length];
    for (int i = 0; i < exps.length; i++) {
      final RexNode exp = exps[i];
      if (exp instanceof RexInputRef) {
        fields[i] = ((RexInputRef) exp).getIndex();
      } else {
        return null; // not a simple projection
      }
    }
    return fields;
  }
}
```

The constructor declares the pattern of relational expressions that will cause
the rule to fire.

The <code>onMatch</code> method generates a new relational expression and calls
<code><a href="http://www.hydromatic.net/optiq/apidocs/org/eigenbase/relopt/RelOptRuleCall.html#transformTo(org.eigenbase.rel.RelNode)">RelOptRuleCall.transformTo()</a></code>
to indicate that the rule has fired successfully.

## The query optimization process

There's a lot to say about how clever Optiq's query planner is, but we won't say
it here. The cleverness is designed to take the burden off you, the writer of
planner rules.

First, Optiq doesn't fire rules in a prescribed order. The query optimization process
follows many branches of a branching tree, just like a chess playing program
examines many possible sequences of moves. If rules A and B both match a given
section of the query operator tree, then Optiq can fire both.

Second, Optiq uses cost in choosing between plans, but the cost model doesn't
prevent rules from firing which may seem to be more expensive in the short term.

Many optimizers have a linear optimization scheme. Faced with a choice between
rule A and rule B, as above, such an optimizer needs to choose immediately. It
might have a policy such as "apply rule A to the whole tree, then apply rule B
to the whole tree", or apply a cost-based policy, applying the rule that
produces the cheaper result.

Optiq doesn't require such compromises.
This makes it simple to combine various sets of rules.
If, say you want to combine rules to recognize materialized views with rules to
read from CSV and JDBC source systems, you just give Optiq the set of all rules
and tell it to go at it.

Optiq does use a cost model. The cost model decides which plan to ultimately use,
and sometimes to prune the search tree to prevent the search space from
exploding, but it never forces you to choose between rule A and rule B. This is
important, because it avoids falling into local minima in the search space that
are not actually optimal.

Also (you guessed it) the cost model is pluggable, as are the table and query
operator statistics it is based upon. But that can be a subject for later.

## JDBC adapter

The JDBC adapter maps a schema in a JDBC data source as an Optiq schema.

For example, this schema reads from a MySQL "foodmart" database:

```json
{
  version: '1.0',
  defaultSchema: 'FOODMART',
  schemas: [
    {
      name: 'FOODMART',
      type: 'custom',
      factory: 'net.hydromatic.optiq.impl.jdbc.JdbcSchema.Factory',
      operand: {
        driver: 'com.mysql.jdbc.Driver',
        url: 'jdbc:mysql://localhost/foodmart',
        user: 'foodmart',
        password: 'foodmart'
      }
    }
  ]
}
```

(The FoodMart database will be familiar to those of you who have used
the Mondrian OLAP engine, because it is Mondrian's main test data
set. To load the data set, follow <a
href="http://mondrian.pentaho.com/documentation/installation.php#2_Set_up_test_data">Mondrian's
installation instructions</a>.)

<b>Current limitations</b>: The JDBC adapter currently only pushes
down table scan operations; all other processing (filtering, joins,
aggregations and so forth) occurs within Optiq. Our goal is to push
down as much processing as possible to the source system, translating
syntax, data types and built-in functions as we go. If an Optiq query
is based on tables from a single JDBC database, in principle the whole
query should go to that database. If tables are from multiple JDBC
sources, or a mixture of JDBC and non-JDBC, Optiq will use the most
efficient distributed query approach that it can.

## The cloning JDBC adapter

The cloning JDBC adapter creates a hybrid database. The data is
sourced from a JDBC database but is read into in-memory tables the
first time each table is accessed. Optiq evaluates queries based on
those in-memory tables, effectively a cache of the database.

For example, the following model reads tables from a MySQL
"foodmart" database:

```json
{
  version: '1.0',
  defaultSchema: 'FOODMART_CLONE',
  schemas: [
    {
      name: 'FOODMART_CLONE',
      type: 'custom',
      factory: 'net.hydromatic.optiq.impl.clone.CloneSchema.Factory',
      operand: {
        driver: 'com.mysql.jdbc.Driver',
        url: 'jdbc:mysql://localhost/foodmart',
        user: 'foodmart',
        password: 'foodmart'
      }
    }
  ]
}
```

Another technique is to build a clone schema on top of an existing
schema. You use the <code>source</code> property to reference a schema
defined earlier in the model, like this:

```json
{
  version: '1.0',
  defaultSchema: 'FOODMART_CLONE',
  schemas: [
    {
      name: 'FOODMART',
      type: 'custom',
      factory: 'net.hydromatic.optiq.impl.jdbc.JdbcSchema.Factory',
      operand: {
        driver: 'com.mysql.jdbc.Driver',
        url: 'jdbc:mysql://localhost/foodmart',
        user: 'foodmart',
        password: 'foodmart'
      }
    },
    {
      name: 'FOODMART_CLONE',
      type: 'custom',
      factory: 'net.hydromatic.optiq.impl.clone.CloneSchema.Factory',
      operand: {
        source: 'FOODMART'
      }
    }
  ]
}
```

You can use this approach to create a clone schema on any type of
schema, not just JDBC.

The cloning adapter isn't the be-all and end-all. We plan to develop
more sophisticated caching strategies, and a more complete and
efficient implementation of in-memory tables, but for now the cloning
JDBC adapter shows what is possible and allows us to try out our
initial implementations.

## Further topics

### Defining a custom schema

(To be written.)

### Modifying data

How to enable DML operations (INSERT, UPDATE and DELETE) on your schema.

(To be written.)

### Calling conventions

(To be written.)

### Statistics and cost

(To be written.)

### Defining and using user-defined functions

(To be written.)

###  Defining tables in a schema

(To be written.)

### Defining custom tables

(To be written.)

### Built-in SQL implementation

How does Optiq implement SQL, if an adapter does not implement all of the core relational operators?

(To be written.)

### Table functions

(To be written.)

## Further resources

* <a href="http://github.com/julianhyde/optiq">Optiq</a> home page
* <a href="http://github.com/julianhyde/optiq-csv">Optiq-csv</a> home page
