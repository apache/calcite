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

# Testing Calcite using SQL Logic Tests

[SQL Logic Tests](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) is public domain suite of several million SQL tests
created as part of the sqlite engine.
The project [hydromatic/sql-logic-test](https://github.com/hydromatic/sql-logic-test/)
has packaged these tests into a Java framework which makes it easy to
run them against a JDBC provider.

## How does it work

In this project we test Calcite as a JDBC provider, coupled with [HSQLDB](http://hsqldb.org/) for storage.
Another storage layer, such as [Postgres](https://www.postgresql.org), could be substituted for HSQLDB.
(Please note that the hydromatic/sql-logic-test project is configured
to use the Postgres dialect version of the queries; for other dialects
you may need to tweak the source code.)

The test suite consists of 622 SQL scripts.  Each script contains SQL
statements and queries.  The statements are executed using HSQLDB, while
the queries are executed using Calcite.

A small number of tests fail because they use statements currently not supported by
HSQLDB (these are listed in the `SqlLogicTestsForCalciteTests.unsupported` set).

For each test file we have summarized the number of passed and failed tests in a "golden" file.
These results are checked in as part of the `sltttestfailures.txt` resource file.
Currently, there are quite a few errors, so we do not keep tab of the actual
errors that were encountered; we expect that, as bugs are fixed in Calcite,
the number of errors will shrink, and we will move to a more precise accounting
method.

The test `SqlLogicTestsForCalciteTests` will fail if any test script generates
*more* errors than the number from the golden file.

## How can I find out which tests fail?

The actual tests are invoked by this line from `SqlLogicTestsForCalciteTests.runOneTestFile`:
```
TestStatistics res = launchSqlLogicTest("-e", "calcite", testFile);
```
The arguments supplied are command-line arguments for the sql-logic-test
executable from the hydromatic project.  The verbosity of the
output can be increased by adding more "-v" flags to the command-line.
By changing the line above to:

```
TestStatistics res = launchSqlLogicTest("-v", -e", "calcite", testFile);
```

after each test file has been completed a summary of all the encountered errors
will be printed, which can help pinpoint the error.  Some exceptions
may cause messages on stderr before the execution is completed, but
the complete tally is shown at the end.

If you want to narrow down the analysis to a subset of the test files you
can modify the code in the `runAllTests` function to ignore some test files.

## Errors

There are several classes of errors that these test have discovered in Calcite:

* compilation or execution that does not terminate.  Currently, we have 2 such tests,
  appearing in the `SqlLogicTestsForCalciteTests.timeouts` set.
* crashes in Calcite compilation or execution.  These show up as `SqlException` exceptions
  and usually produce several lines of logs each
* incorrect outputs produced by Calcite.  By increasing the verbosity
  even further one can see the expected and produced output.  Note that
  not all tests specify the output in full, some only specify an MD5 checksum.
* incorrect outputs produced by Calcite detected through a different MD5 checksum.

Here is an example output fragment produced for one test script when using verbosity 1
(there are 12 errors, but we only show 4 different ones here):

```
12 failures
Total files processed: 1
Files not parsed: 0
Passed: 988
Failed: 12
Ignored: 0
12 failures:
ERROR: Required columns {2, 3} not subset of left columns {0, 1, 2}
	test: test/select2.test:1947
	SELECT d-e, (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d) FROM t1 WHERE (e>c OR e<d)
Litmus.java:31 org.apache.calcite.util.Litmus.lambda$static$0
ERROR: Error while executing SQL "SELECT d, abs(a), (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b) FROM t1 WHERE a IS NULL AND a>b": Unable to implement EnumerableCalc(expr#0..3=[{inputs}], expr#4=[ABS($t0)], expr#5=[IS NULL($t3)], expr#6=[0:BIGINT], expr#7=[CASE($t5, $t6, $t3)], D=[$t2], EXPR$1=[$t4], EXPR$2=[$t7]): rowcount = 1.0, cumulative cost = {465.45 rows, 574.2 cpu, 0.0 io}, id = 48555
  EnumerableCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}]): rowcount = 1.0, cumulative cost = {464.45 rows, 563.2 cpu, 0.0 io}, id = 48551
    EnumerableValues(tuples=[[]]): rowcount = 1.0, cumulative cost = {1.0 rows, 1.0 cpu, 0.0 io}, id = 48504
    JdbcToEnumerableConverter: rowcount = 1.0, cumulative cost = {231.225 rows, 281.1 cpu, 0.0 io}, id = 48549
      JdbcAggregate(group=[{}], EXPR$0=[COUNT()]): rowcount = 1.0, cumulative cost = {231.125 rows, 281.0 cpu, 0.0 io}, id = 48547
        JdbcFilter(condition=[<($0, $cor0.B)]): rowcount = 50.0, cumulative cost = {230.0 rows, 281.0 cpu, 0.0 io}, id = 48545
          JdbcProject(B=[$1]): rowcount = 100.0, cumulative cost = {180.0 rows, 181.0 cpu, 0.0 io}, id = 48543
            JdbcTableScan(table=[[SLT, T1]]): rowcount = 100.0, cumulative cost = {100.0 rows, 101.0 cpu, 0.0 io}, id = 48416

	test: test/select2.test:2436
	SELECT d, abs(a), (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b) FROM t1 WHERE a IS NULL AND a>b
Helper.java:56 org.apache.calcite.avatica.Helper.createException
ERROR: Hash of data does not match expected value
	test: test/select2.test:4852
	SELECT c-d, abs(b-c), b-c, (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b), d FROM t1 WHERE b>c OR c>d OR d NOT BETWEEN 110 AND 150
ERROR: Error while executing SQL "SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b), c-d, (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d), abs(b-c), b FROM t1 WHERE c>d AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)": null
	test: test/select2.test:5141
	SELECT (SELECT count(*) FROM t1 AS x WHERE x.b<t1.b), c-d, (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d<t1.d), abs(b-c), b FROM t1 WHERE c>d AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
Helper.java:56 org.apache.calcite.avatica.Helper.createException
```

Currently, there is no easy way to create a short reproduction for a test
failure, but this is a feature we plan to add.  Most failures can be reproduced
by taking a test script and keeping all DDL statements and only the failing query.

By increasing verbosity even more you can get in the output a complete stack trace
for each error caused by an exception.
