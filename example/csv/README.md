Calcite-csv
============

Calcite adapter that reads
<a href="http://en.wikipedia.org/wiki/Comma-separated_values">CSV</a> files.

Calcite-csv is a nice simple example of how to connect
<a href="https://github.com/apache/incubator-calcite">Apache Calcite</a>
to your own data source and quickly get a full SQL/JDBC interface.

Download and build
==================

You need Java (1.6 or higher; 1.7 preferred), git and maven (3.0.4 or later).

```bash
$ git clone https://github.com/apache/incubator-calcite.git
$ cd incubator-calcite
$ mvn install -DskipTests -Dcheckstyle.skip=true
$ cd example/csv
```

Kick the tires
==============

Let's take a quick look at calcite-csv's (and Calcite's) features.
We'll use <code>sqlline</code>, a SQL shell that connects to
any JDBC data source and is included with calcite-csv.

Connect to Calcite and try out some queries:

```SQL
$ ./sqlline
!connect jdbc:calcite:model=target/test-classes/model.json admin admin

VALUES char_length('Hello, ' || 'world!');

!tables

!describe emps

SELECT * FROM emps;

EXPLAIN PLAN FOR SELECT * FROM emps;

-- non-smart model does not optimize "fetch only required columns"
EXPLAIN PLAN FOR SELECT empno, name FROM emps;
```

Connect to "smart" model and see how it uses just the required columns from the
CSV.
```SQL
!connect jdbc:calcite:model=target/test-classes/smart.json admin admin
EXPLAIN PLAN FOR SELECT * FROM emps;

-- Smart model optimizes "fetch only required columns"
EXPLAIN PLAN FOR SELECT empno, name FROM emps;

SELECT depts.name, count(*)
  FROM emps JOIN depts USING (deptno)
 GROUP BY depts.name;

EXPLAIN PLAN FOR SELECT empno, name FROM emps;

-- However it is not very smart, so it cannot see through calculations
-- This is solved in CALCITE-477
EXPLAIN PLAN FOR SELECT empno*2, name FROM emps;
```
```
!quit
```

(On Windows, the command is `sqlline.bat`.)

As you can see, Calcite has a full SQL implementation that can efficiently
query any data source.

For a more leisurely walk through what Calcite can do and how it does it,
try <a href="TUTORIAL.md">the Tutorial</a>.

More information
================

* License: Apache License, Version 2.0.
* Author: Julian Hyde
* Blog: http://julianhyde.blogspot.com
* Project page: http://calcite.incubator.apache.org/
* Source code: https://git-wip-us.apache.org/repos/asf?p=incubator-calcite.git
* Github mirror: https://github.com/apache/incubator-calcite
* Developers list: http://mail-archives.apache.org/mod_mbox/incubator-calcite-dev/
* <a href="TUTORIAL.md">Tutorial</a>
* <a href="HISTORY.md">Release notes and history</a>
