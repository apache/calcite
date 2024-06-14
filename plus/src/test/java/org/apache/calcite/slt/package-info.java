/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <a href="https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki">SQL Logic Tests</a>
 * is public-domain suite of several million SQL tests created as part of the sqlite engine.
 * The project <a href="https://github.com/hydromatic/sql-logic-test/">
 * hydromatic/sql-logic-test</a>
 * has packaged these tests into a Java framework which makes it easy to
 * run them against a JDBC provider.
 *
 * <p>In this project we test Calcite as a JDBC provider, coupled with
 * <a href="http://hsqldb.org/">HSQLDB</a> for storage.
 * Another storage layer, such as <a href="https://www.postgresql.org">Postgres</a>,
 * could be substituted for HSQLDB.
 * (Please note that the hydromatic/sql-logic-test project is configured
 * to use the Postgres dialect version of the queries; for other dialects
 * you may need to tweak the source code.)
 *
 * <p>The test suite consists of 622 SQL scripts.  Each script contains SQL
 * statements and queries.  The statements are executed using HSQLDB, while
 * the queries are executed using Calcite.
 *
 * <p><strong>Errors</strong>
 *
 * <p>There are several classes of errors that these test have discovered in Calcite:
 * <ul>
 * <li>compilation or execution that does not terminate.  Currently, we have 2 such tests,
 *   appearing in the `SqlLogicTestsForCalciteTests.timeouts` set.</li>
 * <li>crashes in Calcite compilation or execution.  These show up as `SqlException` exceptions
 *   and usually produce several lines of logs each</li>
 * <li>incorrect outputs produced by Calcite.  By increasing the verbosity
 *   even further one can see the expected and produced output.  Note that
 *   not all tests specify the output in full, some only specify an MD5 checksum.</li>
 * <li>incorrect outputs produced by Calcite detected through a different MD5 checksum.</li>
 * </ul>
 *
 * <p>Here is an example output fragment produced for one test script when using verbosity 1
 * (there are 12 errors, but we only show 4 different ones here):
 * <pre>
 * 12 failures
 * Total files processed: 1
 * Files not parsed: 0
 * Passed: 988
 * Failed: 12
 * Ignored: 0
 * 12 failures:
 * ERROR: Required columns {2, 3} not subset of left columns {0, 1, 2}
 * test: test/select2.test:1947
 * SELECT d-e, (SELECT count(*) FROM t1 AS x WHERE x.c>t1.c AND x.d &lt; t1.d) FROM t1 WHERE
 * (e &gt; c OR e &lt; d)
 * Litmus.java:31 org.apache.calcite.util.Litmus.lambda$static$0
 * ERROR: Error while executing SQL "SELECT d, abs(a), (SELECT count(*) FROM t1 AS x
 * WHERE x.b &lt; t1 .b) FROM t1 WHERE a IS NULL AND a &gt; b":
 * Unable to implement EnumerableCalc(expr#0..3=[{inputs}],
 * expr#4=[ABS($t0)], expr#5=[IS NULL($t3)], expr#6=[0:BIGINT], expr#7=[CASE($t5, $t6, $t3)],
 * D=[$t2], EXPR$1=[$t4], EXPR$2=[$t7]): rowcount = 1.0, cumulative cost = {465.45 rows, 574.2
 * cpu, 0.0 io}, id = 48555
 *   EnumerableCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}]): rowcount =
 *   1.0, cumulative cost = {464.45 rows, 563.2 cpu, 0.0 io}, id = 48551
 *     EnumerableValues(tuples=[[]]): rowcount = 1.0, cumulative cost = {1.0 rows, 1.0 cpu, 0.0
 *     io}, id = 48504
 *     JdbcToEnumerableConverter: rowcount = 1.0, cumulative cost = {231.225 rows, 281.1 cpu, 0.0
 *     io}, id = 48549
 *       JdbcAggregate(group=[{}], EXPR$0=[COUNT()]): rowcount = 1.0, cumulative cost = {231.125
 *       rows, 281.0 cpu, 0.0 io}, id = 48547
 *         JdbcFilter(condition=[<($0, $cor0.B)]): rowcount = 50.0, cumulative cost = {230.0
 *         rows, 281.0 cpu, 0.0 io}, id = 48545
 *           JdbcProject(B=[$1]): rowcount = 100.0, cumulative cost = {180.0 rows, 181.0 cpu, 0.0
 *           io}, id = 48543
 *             JdbcTableScan(table=[[SLT, T1]]): rowcount = 100.0, cumulative cost = {100.0 rows,
 *             101.0 cpu, 0.0 io}, id = 48416
 * test: test/select2.test:2436
 * SELECT d, abs(a), (SELECT count(*) FROM t1 AS x WHERE x.b &lt; t1.b)
 * FROM t1 WHERE a IS NULL AND a &gt; b
 * Helper.java:56 org.apache.calcite.avatica.Helper.createException
 * ERROR: Hash of data does not match expected value
 * test: test/select2.test:4852
 * SELECT c-d, abs(b-c), b-c, (SELECT count(*) FROM t1 AS x WHERE x.b &lt; t1.b), d FROM t1
 * WHERE b &gt; c OR c &gt; d OR d NOT BETWEEN 110 AND 150
 * ERROR: Error while executing SQL "SELECT (SELECT count(*) FROM t1 AS x WHERE x.b &lt; t1.b),
 * c-d, (SELECT count(*) FROM t1 AS x WHERE x.c &gt; t1.c AND x.d &lt; t1.d), abs(b-c), b
 * FROM t1 WHERE c &gt; d AND EXISTS(SELECT 1 FROM t1 AS x WHERE x.b &lt; t1.b)": null
 * test: test/select2.test:5141
 * SELECT (SELECT count(*) FROM t1 AS x WHERE x.b &lt; t1.b), c-d,
 * (SELECT count(*) FROM t1 AS x WHERE x.c &gt; t1.c AND x.d &lt; t1.d), abs(b-c), b FROM t1
 * WHERE c &gt; d AND EXISTS(SELECT 1 FROM t1 AS x WHERE x .b &lt; t1.b)
 * Helper.java:56 org.apache.calcite.avatica.Helper.createException
 * </pre>
 */
package org.apache.calcite.slt;
