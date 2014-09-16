[![Build Status](https://travis-ci.org/julianhyde/optiq.svg?branch=master)](https://travis-ci.org/julianhyde/optiq)

# Apache Optiq

Apache Optiq is a dynamic data management framework.

## Getting Optiq

To run Apache Optiq, you can either
[download and build from github](doc/HOWTO.md#building-from-git),
or [download a release](http://www.apache.org/dyn/closer.cgi/incubator/optiq)
then [build the source code](doc/HOWTO.md#building-from-a-source-distribution).

Pre-built jars are in
[the Apache maven repository](https://repository.apache.org/content/repositories/releases)
with the following Maven coordinates:

```xml
<dependency>
  <groupId>org.apache.optiq</groupId>
  <artifactId>optiq-core</artifactId>
  <version>0.9.0-incubating</version>
</dependency>
```

## Example

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
JdbcSchema.create(optiqConnection, dataSource, rootSchema, "hr", "");
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

See the <a href="doc/HOWTO.md">HOWTO</a> for more information about using other
adapters, and about using Optiq in general.

## Status

The following features are complete.

* Query parser, validator and optimizer
* Support for reading models in JSON format
* Many standard functions and aggregate functions
* JDBC queries against Linq4j and JDBC back-ends
* <a href="https://github.com/julianhyde/linq4j">Linq4j</a> front-end
* SQL features: SELECT, FROM (including JOIN syntax), WHERE, GROUP BY (and aggregate functions including COUNT(DISTINCT ...)), HAVING, ORDER BY (including NULLS FIRST/LAST), set operations (UNION, INTERSECT, MINUS), sub-queries (including correlated sub-queries), windowed aggregates, LIMIT (syntax as <a href="http://www.postgresql.org/docs/8.4/static/sql-select.html#SQL-LIMIT">Postgres</a>)

For more details, see the <a href="doc/REFERENCE.md">Reference guide</a>.

### Drivers

* <a href="http://www.hydromatic.net/optiq/optiq-core/apidocs/net/hydromatic/optiq/jdbc/package-summary.html">JDBC driver</a>

### Adapters

* <a href="https://github.com/apache/incubator-drill">Apache Drill adapter</a>
* Cascading adapter (<a href="https://github.com/Cascading/lingual">Lingual</a>)
* CSV adapter (<a href="https://github.com/julianhyde/optiq-csv">optiq-csv</a>)
* JDBC adapter (part of <a href="http://www.hydromatic.net/optiq/optiq-core/apidocs/net/hydromatic/optiq/impl/jdbc/package-summary.html">optiq-core</a>)
* MongoDB adapter (<a href="http://www.hydromatic.net/optiq/optiq-mongodb/apidocs/net/hydromatic/optiq/impl/mongodb/package-summary.html">optiq-mongodb</a>)
* Spark adapter (<a href="http://www.hydromatic.net/optiq/optiq-spark/apidocs/net/hydromatic/optiq/impl/spark/package-summary.html">optiq-spark</a>)
* Splunk adapter (<a href="http://www.hydromatic.net/optiq/optiq-splunk/apidocs/net/hydromatic/optiq/impl/splunk/package-summary.html">optiq-splunk</a>)
* Eclipse Memory Analyzer (MAT) adapter (<a href="https://github.com/vlsi/optiq-mat-plugin">optiq-mat-plugin</a>)

## More information

* License: Apache License, Version 2.0.
* Author: Julian Hyde
* Blog: http://julianhyde.blogspot.com
* Incubation status page: http://incubator.apache.org/projects/optiq.html
* Project page: http://www.hydromatic.net/optiq
* Source code: http://github.com/julianhyde/optiq
* Issues: <a href="https://issues.apache.org/jira/browse/OPTIQ">Apache JIRA</a>
* Developers list: <a href="mailto:dev@optiq.incubator.apache.org">dev at optiq.incubator.apache.org</a> (<a href="http://mail-archives.apache.org/mod_mbox/incubator-optiq-dev/">archive</a>, <a href="mailto:dev-subscribe@optiq.incubator.apache.org">subscribe</a>)
* <a href="doc/HOWTO.md">HOWTO</a>
* <a href="doc/MODEL.md">JSON model</a>
* <a href="doc/REFERENCE.md">Reference guide</a>
* <a href="doc/HISTORY.md">Release notes and history</a>

### Pre-Apache resources

These resources, which we used before Optiq was an Apache incubator
project, are for reference only.
They may be out of date.
Please don't post to the mailing list.

* Developers list: <a href="http://groups.google.com/group/optiq-dev">optiq-dev@googlegroups.com</a>

### Presentations

* <a href="http://www.slideshare.net/julianhyde/how-to-integrate-splunk-with-any-data-solution">How to integrate Splunk with any data solution</a> (Splunk User Conference, 2012)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-drill-user-group-2013.pdf?raw=true">Drill / SQL / Optiq</a> (2013)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-richrelevance-2013.pdf?raw=true">SQL on Big Data using Optiq</a> (2013)
* <a href="https://github.com/julianhyde/share/blob/master/slides/optiq-nosql-now-2013.pdf?raw=true">SQL Now!</a> (NoSQL Now! conference, 2013)
* <a href="https://github.com/julianhyde/share/blob/master/slides/hive-cbo-summit-2014.pdf?raw=true">Cost-based optimization in Hive</a> (<a href="https://www.youtube.com/watch?v=vpG5noIbEFs">video</a>) (Hadoop Summit, 2014)
* <a href="https://github.com/julianhyde/share/blob/master/slides/dmmq-summit-2014.pdf?raw=true">Discardable, in-memory materialized query for Hadoop</a> (<a href="https://www.youtube.com/watch?v=CziGOa8GXqI">video</a>) (Hadoop Summit, 2014)

## Disclaimer

Apache Optiq is an effort undergoing incubation at The Apache Software
Foundation (ASF), sponsored by the Apache Incubator. Incubation is
required of all newly accepted projects until a further review
indicates that the infrastructure, communications, and decision making
process have stabilized in a manner consistent with other successful
ASF projects. While incubation status is not necessarily a reflection
of the completeness or stability of the code, it does indicate that
the project has yet to be fully endorsed by the ASF.
