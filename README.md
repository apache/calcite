optiq
=====

Optiq is a dynamic data management framework.

Prerequisites
=============

Optiq requires git, maven, and JDK 1.7.

Optiq also depends on linq4j. Before you build optiq, you must download, build and install linq4j:

    $ git clone git://github.com/julianhyde/linq4j.git
    $ cd linq4j
    $ mvn install
    $ cd ..

Download and build
==================

    $ git clone git://github.com/julianhyde/optiq.git
    $ mvn compile

Example
=======

Optiq makes data anywhere, of any format, look like a database. For
example, you can execute a complex ANSI-standard SQL statement on
in-memory collections:

    public static class HrSchema {
        public final Employee[] emps = ... ;
        public final Department[] depts = ...;
    }

    Class.forName("net.hydromatic.optiq.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:optiq:");
    OptiqConnection optiqConnection =
        connection.unwrap(OptiqConnection.class);
    ReflectiveSchema.create(
        optiqConnection, optiqConnection.getRootSchema(),
        "hr", new HrSchema());
    Statement statement = optiqConnection.createStatement();
    ResultSet resultSet = statement.executeQuery(
        "select d.\"deptno\", min(e.\"empid\")\n"
        + "from \"hr\".\"emps\" as e\n"
        + "join \"hr\".\"depts\" as d\n"
        + "  on e.\"deptno\" = d.\"deptno\"\n"
        + "group by d.\"deptno\"\n"
        + "having count(*) > 1");
    toString(resultSet);
    resultSet.close();
    statement.close();
    connection.close();


Where is the database? There is no database. The connection is
completely empty until <code>ReflectiveSchema.create</code> registers
a Java object as a schema and its collection fields <code>emps</code>
and <code>depts</code> as tables.

Optiq does not want to own data; it does not even have favorite data
format. This example used in-memory data sets, and processed them
using operators such as <code>groupBy</code> and <code>join</code>
from the <a href="https://github.com/julianhyde/linq4j">linq4j</a>
library. But Optiq can also process data in other data formats, such
as JDBC. In the first example, replace

    ReflectiveSchema.create(
        optiqConnection, optiqConnection.getRootSchema(),
        "hr", new HrSchema());

with

    Class.forName("com.mysql.jdbc.Driver");
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl("jdbc:mysql://localhost");
    dataSource.setUsername("sa");
    dataSource.setPassword("");
    JdbcSchema.create(
        optiqConnection,
        dataSource,
        rootSchema,
        "hr",
        "");

and Optiq will execute the same query in JDBC. To the application, the
data and API are the same, but behind the scenes the implementation is
very different. Optiq uses optimizer rules
to push the <code>JOIN</code> and <code>GROUP BY</code> operations to
the source database.

In-memory and JDBC are just two familiar examples. Optiq can handle
any data source and data format.

To add a data source, you need to write an adapter that tells Optiq
what collections in the data source it should consider "tables".

For more advanced integration, you can write optimizer
rules. Optimizer rules allow Optiq to access data of a new format,
allow you to register new operators (such as a better join algorithm),
and allow Optiq to optimize how queries are translated to
operators. Optiq will combine your rules and operators with built-in
rules and operators, apply cost-based optimization, and generate an
efficient plan.

Status
======

The following features are complete.

* Query parser, validator and optimizer complete.
* Many standard functions and aggregate functions (limited number available in plans implemented in Java)
* JDBC queries against Linq4j and JDBC back-ends
* Linq4j front-end

Backlog
=======

* Rules to push down as many operations as possible to JDBC back-end (i.e. generate SQL)
* Splunk adapter
* Cascading adapter
* Easy API to register optimizer rules
* Easy API to register calling conventions

More information
================

* License: Apache License, Version 2.0.
* Author: Julian Hyde
* Blog: http://julianhyde.blogspot.com
* Project page: http://www.hydromatic.net/optiq
* Source code: http://github.com/julianhyde/optiq
