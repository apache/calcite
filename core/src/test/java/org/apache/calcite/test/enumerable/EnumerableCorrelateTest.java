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
package org.apache.calcite.test.enumerable;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.JdbcTest;
import org.apache.calcite.util.Bug;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Tests {@link org.apache.calcite.adapter.enumerable.EnumerableCorrelate}
 */
public class EnumerableCorrelateTest {
  @Test public void simpleCorrelateDecorrelated() {
    tester(true, new JdbcTest.HrSchema())
        .query(
            "select empid, name from emps e where exists (select 1 from depts d where d.deptno=e.deptno)")
        .explainContains(
            "EnumerableCalc(expr#0..4=[{inputs}], empid=[$t0], name=[$t2])\n"
            + "  EnumerableSemiJoin(condition=[=($1, $5)], joinType=[inner])\n"
            + "    EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[true], $f01=[$t0], $f0=[$t4])\n"
            + "      EnumerableJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "        EnumerableAggregate(group=[{0}])\n"
            + "          EnumerableCalc(expr#0..4=[{inputs}], $f0=[$t1])\n"
            + "            EnumerableTableScan(table=[[s, emps]])\n"
            + "        EnumerableTableScan(table=[[s, depts]])")
        .returnsUnordered(
            "empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");

  }

  @Test public void simpleCorrelate() {
    tester(false, new JdbcTest.HrSchema())
        .query(
            "select empid, name from emps e where exists (select 1 from depts d where d.deptno=e.deptno)")
        .explainContains(
            "EnumerableCalc(expr#0..5=[{inputs}], expr#6=[IS NOT NULL($t5)], empid=[$t0], name=[$t2], $condition=[$t6])\n"
            + "  EnumerableCorrelate(correlation=[$cor0], joinType=[LEFT], requiredColumns=[{1}])\n"
            + "    EnumerableTableScan(table=[[s, emps]])\n"
            + "    EnumerableAggregate(group=[{}], agg#0=[MIN($0)])\n"
            + "      EnumerableCalc(expr#0..2=[{inputs}], expr#3=[true], expr#4=[$cor0], expr#5=[$t4.deptno], expr#6=[=($t0, $t5)], $f0=[$t3], $condition=[$t6])\n"
            + "        EnumerableTableScan(table=[[s, depts]])")
        .returnsUnordered(
            "empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian");

  }

  private CalciteAssert.AssertThat tester(final boolean forceDecorrelate,
      final Object schema) {
    Bug.remark(
        "CALCITE-489 - Teach CalciteAssert to respect multiple settings");
    final Properties p = new Properties();
    p.setProperty("lex", "JAVA");
    p.setProperty("forceDecorrelate", Boolean.toString(forceDecorrelate));
    return CalciteAssert.that()
        .with(new CalciteAssert.ConnectionFactory() {
          public CalciteConnection createConnection() throws Exception {
            Connection connection =
                DriverManager.getConnection("jdbc:calcite:", p);
            CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema =
                calciteConnection.getRootSchema();
            rootSchema.add("s", new ReflectiveSchema(schema));
            calciteConnection.setSchema("s");
            return calciteConnection;
          }
        });
  }
}

// End EnumerableCorrelateTest.java
