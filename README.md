optiq
=====

Optiq is a dynamic data management framework.

Prerequisites
=============

Optiq requires git, maven, and JDK 1.6 or later (JDK 1.7 preferred).

Download and build
==================

```bash
$ git clone git://github.com/julianhyde/optiq.git
$ cd optiq
$ mvn install
```

Example
=======

Optiq makes data anywhere, of any format, look like a database. For
example, you can execute a complex ANSI-standard SQL statement on
in-memory collections:

```java
public static class HrSchema {
  public final Employee[] emps = ... ;
  public final Department[] depts = ...;
}

Class.forName("net.hydromatic.optiq.jdbc.Driver");
Connection connection = DriverManager.getConnection("jdbc:optiq:");
OptiqConnection optiqConnection =
    connection.unwrap(OptiqConnection.class);
ReflectiveSchema.create(optiqConnection,
    optiqConnection.getRootSchema(), "hr", new HrSchema());
Statement statement = optiqConnection.createStatement();
ResultSet resultSet = statement.executeQuery(
    "select d.\"deptno\", min(e.\"empid\")\n"
    + "from \"hr\".\"emps\" as e\n"
    + "join \"hr\".\"depts\" as d\n"
    + "  on e.\"deptno\" = d.\"deptno\"\n"
    + "group by d.\"deptno\"\n"
    + "having count(*) > 1");
print(resultSet);
resultSet.close();
statement.close();
connection.close();
```

Where is the database? There is no database. The connection is
completely empty until <code>ReflectiveSchema.create</code> registers
a Java object as a schema and its collection fields <code>emps</code>
and <code>depts</code> as tables.

Optiq does not want to own data; it does not even have favorite data
format. This example used in-memory data sets, and processed them
using operators such as <code>groupBy</code> and <code>join</code>
from the linq4j
library. But Optiq can also process data in other data formats, such
as JDBC. In the first example, replace

```java
ReflectiveSchema.create(optiqConnection,
    optiqConnection.getRootSchema(), "hr", new HrSchema());
```

with

```java
Class.forName("com.mysql.jdbc.Driver");
BasicDataSource dataSource = new BasicDataSource();
dataSource.setUrl("jdbc:mysql://localhost");
dataSource.setUsername("sa");
dataSource.setPassword("");
JdbcSchema.create(optiqConnection, dataSource,
    rootSchema, "hr", "");
```

and Optiq will execute the same query in JDBC. To the application, the
data and API are the same, but behind the scenes the implementation is
very different. Optiq uses optimizer rules
to push the <code>JOIN</code> and <code>GROUP BY</code> operations to
the source database.

In-memory and JDBC are just two familiar examples. Optiq can handle
any data source and data format. To add a data source, you need to
write an adapter that tells Optiq
what collections in the data source it should consider "tables".

For more advanced integration, you can write optimizer
rules. Optimizer rules allow Optiq to access data of a new format,
allow you to register new operators (such as a better join algorithm),
and allow Optiq to optimize how queries are translated to
operators. Optiq will combine your rules and operators with built-in
rules and operators, apply cost-based optimization, and generate an
efficient plan.

### Non-JDBC access

Optiq also allows front-ends other than SQL/JDBC. For example, you can
execute queries in <a href="https://github.com/julianhyde/linq4j">linq4j</a>:

```java
final OptiqConnection connection = ...;
ParameterExpression c = Expressions.parameter(Customer.class, "c");
for (Customer customer
    : connection.getRootSchema()
        .getSubSchema("foodmart")
        .getTable("customer", Customer.class)
        .where(
            Expressions.<Predicate1<Customer>>lambda(
                Expressions.lessThan(
                    Expressions.field(c, "customer_id"),
                    Expressions.constant(5)),
                c))) {
  System.out.println(c.name);
}
```

Linq4j understands the full query parse tree, and the Linq4j query
provider for Optiq invokes Optiq as an query optimizer. If the
<code>customer</code> table comes from a JDBC database (based on
this code fragment, we really can't tell) then the optimal plan
will be to send the query

```SQL
SELECT *
FROM "customer"
WHERE "customer_id" < 5
```

to the JDBC data source.

### Writing an adapter

The <a href="https://github.com/julianhyde/optiq-csv">optiq-csv</a>
project provides a CSV adapter, which is fully functional for use in applications
but is also simple enough to serve as a good template if you are writing
your own adapter.

See the <a href="https://github.com/julianhyde/optiq-csv/blob/master/TUTORIAL.md">optiq-csv tutorial</a>
for information on using optiq-csv and writing adapters.

See the <a href="HOWTO.md">HOWTO</a> for more information about using other
adapters, and about using Optiq in general.

## Status

The following features are complete.

* Query parser, validator and optimizer
* Support for reading models in JSON format
* Many standard functions and aggregate functions
* JDBC queries against Linq4j and JDBC back-ends
* <a href="https://github.com/julianhyde/linq4j">Linq4j</a> front-end

### Adapters

* <a href="https://github.com/julianhyde/optiq-splunk">Splunk adapter</a>
* <a href="https://github.com/julianhyde/optiq-csv">CSV adapter</a>
* MongoDB adapter
* JDBC adapter
* <a href="https://github.com/Cascading/lingual">Cascading adapter (Lingual)</a>
* <a href="https://github.com/apache/incubator-drill">Apache Drill adapter</a>

## Backlog

* Easy API to manage sets of optimizer rules
* Easy API to register calling conventions
* Make 'guaranteed' a constructor parameter to ConverterRule. (It's
  too easy to forget.)
* RelOptRule.convert should check whether there is a subset of desired
  traitSet before creating

## More information

* License: Apache License, Version 2.0.
* Author: Julian Hyde
* Blog: http://julianhyde.blogspot.com
* Project page: http://www.hydromatic.net/optiq
* Source code: http://github.com/julianhyde/optiq
* Developers list: http://groups.google.com/group/optiq-dev
* <a href="HOWTO.md">HOWTO</a>

### Presentations

* <a href="http://www.slideshare.net/julianhyde/how-to-integrate-splunk-with-any-data-solution">Splunk 2012 User Conference</a>
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-drill-user-group-2013.pdf?raw=true">Drill / SQL / Optiq</a>
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-richrelevance-2013.pdf?raw=true">SQL on Big Data using Optiq</a>
