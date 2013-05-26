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
* user-defined table using the Table interface;
* determining the record type of a table;
* a simple implementation of Table that enumerates all rows directly;
* advanced implementation of Table that translates to relational operators using planner rules.

Without further ado, let's get started. Assuming you have already installed
(using <code>git clone</code> and <code>mvn install</code>, as described above),
we'll connect to Optiq using the
<code>sqlline</code> utility, which is included in the optiq-csv github project.

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

Here is the relevant code from <code>CsvSchema</code>:

```java
private Map<String, TableInSchema> map;

public Collection<TableInSchema> getTables() {
  return computeMap().values();
}

private synchronized Map<String, TableInSchema> computeMap() {
  if (map == null) {
    map = new HashMap<String, TableInSchema>();
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
      map.put(
          tableName,
          new TableInSchemaImpl(this, tableName, TableType.TABLE, table));
    }
  }
  return map;
}
```

The schema scans the directory and finds all files whose name ends
with ".csv" and creates tables for them. In this case, the directory
is <code>target/test-classes/sales</code> and contains files
<code>EMPS.csv</code> and <code>DEPTS.csv</code>, which these become
the tables <code>EMPS</code> and <code>DEPTS</code>.

Note how we did not need to define any tables in the model; the schema
generated the tables automatically. (Some schema types allow you to define
tables explicitly, using the <code>tables</code> property of a schema.)

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
down table scan operations; all other processing (filting, joins,
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

You can use this approach for any type of schema, not just JDBC.

We plan to develop more sophisticated caching strategies, and a more
complete and efficient implementation of in-memory tables, but for now
the cloning JDBC adapter shows what is possible and allows us to try
out our initial implementations.

## Further topics

### Defining a custom schema

(To be written.)

### Modifying data

How to enable DML operations (INSERT, UPDATE and DELETE) on your schema.

(To be written.)

### Defining views in a model file

(To be written.)

### Calling conventions

(To be written.)

## Statistics and cost

### Defining and using user-defined functions

(To be written.)

###  Defining tables in a schema

(To be written.)

### Defining custom tables

(To be written.)

### Built-in SQL implementation

How does Optiq implement SQL, if an adapter does not implement all of the core relational operators?

(To be written.)

## Further resources

* <a href="http://github.com/julianhyde/optiq">Optiq</a> home page
* <a href="http://github.com/julianhyde/optiq-csv">Optiq-csv</a> home page
