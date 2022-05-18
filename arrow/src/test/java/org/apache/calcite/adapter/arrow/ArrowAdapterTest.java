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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Tests for the Apache Arrow adapter.
 *
 * <p>The tests are only enabled on Linux, until
 * <a href="https://issues.apache.org/jira/browse/ARROW-11135">[ARROW-11135]
 * Using Maven Central artifacts as dependencies produce runtime errors</a>
 * is fixed. On macOS and Windows, the tests throw
 * {@link org.opentest4j.TestAbortedException}, which causes Junit to ignore
 * them.
 */
class ArrowAdapterTest {
  static final Map<String, String> ARROW =
      ImmutableMap.of("model",
          resourceFile("/arrow-model.json").getAbsolutePath());

  static File resourceFile(String resourcePath) {
    return Sources.of(ArrowAdapterTest.class.getResource(resourcePath)).file();
  }

  ArrowAdapterTest() {
//    assumeTrue(SystemUtils.IS_OS_LINUX,
//        "Arrow adapter requires Linux, until [ARROW-11135] is fixed");
  }

  /** Test to read an Arrow file and check its field names. */
  @Test void testArrowSchema() {
    ArrowSchema arrowSchema =
        new ArrowSchema(resourceFile("/arrow").getAbsoluteFile());
    Map<String, Table> tableMap = arrowSchema.getTableMap();
    RelDataTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType relDataType = tableMap.get("ARROWDATA").getRowType(typeFactory);

    assertThat(relDataType.getFieldNames().get(0), is("intField"));
    assertThat(relDataType.getFieldNames().get(1), is("stringField"));
    assertThat(relDataType.getFieldNames().get(2), is("floatField"));
  }

  @Test void testArrowProjectAllFields() {
    String sql = "select * from arrowdata\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
    String result = "intField=0; stringField=0; floatField=0.0\n"
        + "intField=1; stringField=1; floatField=1.0\n"
        + "intField=2; stringField=2; floatField=2.0\n"
        + "intField=3; stringField=3; floatField=3.0\n"
        + "intField=4; stringField=4; floatField=4.0\n"
        + "intField=5; stringField=5; floatField=5.0\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectSingleField() {
    String sql = "select \"intField\" from arrowdata\n";
    String result = "intField=0\nintField=1\nintField=2\n"
        + "intField=3\nintField=4\nintField=5\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectTwoFields() {
    String sql = "select \"intField\", \"stringField\" from arrowdata\n";
    String result = "intField=0; stringField=0\n"
        + "intField=1; stringField=1\n"
        + "intField=2; stringField=2\n"
        + "intField=3; stringField=3\n"
        + "intField=4; stringField=4\n"
        + "intField=5; stringField=5\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithIntegerFilter() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" < 4";
    String result = "intField=0; stringField=0\n"
        + "intField=1; stringField=1\n"
        + "intField=2; stringField=2\n"
        + "intField=3; stringField=3\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowFilter(condition=[<($0, 4)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .limit(4)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithMultipleFilters() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\"=12 and \"stringField\"='12'";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowFilter(condition=[AND(=($0, 12), =($1, '12'))])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
    String result = "intField=12; stringField=12\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .limit(3)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithFloatFilter() {
    String sql = "select * from arrowdata\n"
        + " where \"floatField\"=15.0";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowFilter(condition=[=(CAST($2):DOUBLE, 15.0)])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
    String result = "intField=15; stringField=15; floatField=15.0\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  // TODO: test a table whose field names contain spaces,
  // e.g. 'SELECT ... WHERE "my Field" > 5'
  // (code generator does not seem to quote field names currently)

  // TODO: test a character literal that contains a single-quote
  // (quoting of literals for Gandiva doesn't look safe to me)

  // TODO: test various data types (TINYINT, SMALLINT, INTEGER, BIGINT, REAL,
  // FLOAT, DATE, TIME, TIMESTAMP, INTERVAL SECOND, INTERVAL MONTH, CHAR,
  // VARCHAR, BINARY, VARBINARY, BOOLEAN) with and without NOT NULL

  // TODO: test casts, including lossy casts

  // TODO: test IS NULL, IS NOT NULL

  // TODO: test 3-valued boolean logic, e.g. 'WHERE (x > 5) IS NOT FALSE',
  // 'SELECT (x > 5) ...'

  // TODO: test string operations, e.g. SUBSTRING

  // TODO: test a join
  // (The implementor can only hold one table at a time, so I suspect this will
  // have problems.)

  // TODO: test a union
  // (The implementor can only hold one table at a time, so I suspect this will
  // have problems.)

  // TODO: test a filter on a project on a filter on a project
  // (The implementor may not be able to combine multiple selections and
  // projectors. Also, in some cases the optimal plan would combine selections
  // and projectors.)

  // TODO: test an OR condition

  // TODO: test an IN condition, e.g. x IN (1, 7, 8, 9)

  // TODO: test an aggregate without aggregate functions,
  // 'SELECT DISTINCT ...'; hopefully Arrow can do this for us

  // TODO: test an aggregate with aggregate functions,
  // 'SELECT SUM(x) ... GROUP BY y'; hopefully Arrow can do this for us

  // TODO: test an aggregate with filtered aggregate functions,
  // 'SELECT SUM(x) FILTER WHERE (z > 5) ... GROUP BY y';
  // hopefully Arrow can do this for us

  // TODO: test an aggregate that groups by a nullable column

  // TODO: test a limit (with no sort),
  // 'SELECT ... LIMIT 10'

  // TODO: test an offset and limit (with no sort),
  // 'SELECT ... OFFSET 20 LIMIT 10'
}
