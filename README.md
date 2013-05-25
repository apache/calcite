optiq-csv
============

Optiq adapter that reads <a href="http://en.wikipedia.org/wiki/Comma-separated_values">CSV</a> files.

Optiq-csv is a nice simple example of how to connect <a
href="https://github.com/julianhyde/optiq">Optiq</a> to your own
data source and quickly get a full SQL/JDBC interface.

Download and build
==================

You need Java (1.5 or higher; 1.7 preferred) and maven (2 or higher).

    $ git clone git://github.com/julianhyde/optiq-csv.git
    $ cd optiq-csv
    $ mvn compile

Run sqlline
===========

Sqlline is a shell that connects to any JDBC data source and lets you execute SQL queries.
Connect to Optiq and try out some queries.

    $ ./sqlline
    sqlline> !connect jdbc:optiq:model=target/test-classes/model.json admin admin
    sqlline> !tables
    sqlline> !describe emps
    sqlline> SELECT * FROM emps;
    sqlline> EXPLAIN PLAN FOR SELECT * FROM emps;
    sqlline> !connect jdbc:optiq:model=target/test-classes/smart.json admin admin
    sqlline> EXPLAIN PLAN FOR SELECT * FROM emps;
    sqlline> SELECT depts.name, count(*)
    . . . .> FROM emps JOIN depts USING (deptno)
    . . . .> GROUP BY depts.name;
    sqlline> VALUES char_length('hello, ' || 'world!');
    sqlline> !quit


Advanced use
============

You can also register a CsvSchema as a schema within an Optiq instance.
Then you can combine with other data sources.

You can write a "vanity JDBC driver" with a different name.

You can add optimizer rules and new implementations of relational
operators to execute queries more efficiently.

More information
================

* License: Apache License, Version 2.0.
* Author: Julian Hyde
* Blog: http://julianhyde.blogspot.com
* Project page: http://www.hydromatic.net/optiq-csv
* Source code: http://github.com/julianhyde/optiq-csv
* Developers list: http://groups.google.com/group/optiq-dev

Tutorial
========

Optiq-csv is a fully functional adapter for <a
href="https://github.com/julianhyde/optiq">Optiq</a> that reads text
files in <a
href="http://en.wikipedia.org/wiki/Comma-separated_values">CSV
(comma-separated values)</a> format. It is remarkable that a couple of
hundred lines of Java code are sufficient to provide full SQL query
capability.

But optiq-csv also serves as a template for building adapters to other
data formats. It covers several important concepts:
* user-defined schema using SchemaFactory and Schema interfaces;
* declaring schemas in a model JSON file;
* user-defined table using the Table interface;
* determining the record type of a table;
* a simple implementation of Table that enumerates all rows directly;
* advanced implementation of Table that translates to relational operators using planner rules.

Without further ado, let's get started. Assuming you have already installed
(using <code>git clone</code> and <code>mvn install</code>),
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

As you can see there are 4 tables in the system: tables EMPS and DEPTS
in the current SALES schema, and COLUMNS and TABLES in the system
"metadata" schema. The system tables are always in Optiq, but the
other tables are provided by the specific implementation of the
schema; in this case, the EMPS and DEPTS tables are based on the
EMPS.csv and DEPTS.csv files in the target/test-classes directory.

Let's execute some queries on those tables, to show that Optiq is providing
a full implementation of SQL.

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

JOIN and GROUP BY:

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

The VALUES operator generates a single row:

```bash
  sqlline> VALUES CHAR_LENGTH('Hello, ' || 'world!');
  +---------+
  | EXPR$0  |
  +---------+
  | 13      |
  +---------+
```

Now, how did Optiq find these tables? Remember, core Optiq does not
know anything about CSV files. (As a "database without a storage
layer", Optiq doesn't know about any file formats.) Optiq knows about
those tables because we told it to run code in the optiq-csv
project. There are a couple of steps in that chain.

First, on the JDBC connect string we gave the path of a model in JSON
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
generated the tables automatically. (It is also possible to define
tables explicitly. We shall cover this <a href="#TODO">later</a>.)


Further topics (to be written): defining a custom schema; DML; defining views; calling conventions; statistics and cost; user-defined functions; defining tables explicitly; defining custom tables.
