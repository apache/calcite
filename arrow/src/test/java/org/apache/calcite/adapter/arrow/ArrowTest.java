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

import org.apache.commons.lang3.SystemUtils;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
class ArrowTest {
  static final Map<String, String> ARROW =
      ImmutableMap.of("model",
          resourceFile("/arrow.json").getAbsolutePath());

  static File resourceFile(String resourcePath) {
    return Sources.of(ArrowTest.class.getResource(resourcePath)).file();
  }

  public ArrowTest() {
    assumeTrue(SystemUtils.IS_OS_LINUX,
        "Arrow adapter requires Linux, until [ARROW-11135] is fixed");
  }

  /** Test to read an Arrow file and check its field names. */
  @Test void testArrowSchema() {
    ArrowSchema arrowSchema =
        new ArrowSchema(resourceFile("/arrow").getAbsoluteFile());
    Map<String, Table> tableMap = arrowSchema.getTableMap();
    RelDataTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType relDataType = tableMap.get("TEST").getRowType(typeFactory);

    assertThat(relDataType.getFieldNames().get(0), is("fieldOne"));
    assertThat(relDataType.getFieldNames().get(1), is("fieldTwo"));
    assertThat(relDataType.getFieldNames().get(2), is("fieldThree"));
  }

  @Test void testArrowProjectAllFields() {
    String sql = "select * from test\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowTableScan(table=[[arrow, TEST]], fields=[[0, 1, 2]])\n\n";
    String result = "fieldOne=1; fieldTwo=abc; fieldThree=1.2\n"
        + "fieldOne=2; fieldTwo=def; fieldThree=3.4\n"
        + "fieldOne=3; fieldTwo=xyz; fieldThree=5.6\n"
        + "fieldOne=4; fieldTwo=abcd; fieldThree=1.22\n"
        + "fieldOne=5; fieldTwo=defg; fieldThree=3.45\n"
        + "fieldOne=6; fieldTwo=xyza; fieldThree=5.67\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectTwoFields() {
    String sql = "select \"fieldOne\", \"fieldTwo\" from test\n";
    String result = "fieldOne=1; fieldTwo=abc\n"
        + "fieldOne=2; fieldTwo=def\n"
        + "fieldOne=3; fieldTwo=xyz\n"
        + "fieldOne=4; fieldTwo=abcd\n"
        + "fieldOne=5; fieldTwo=defg\n"
        + "fieldOne=6; fieldTwo=xyza\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(fieldOne=[$0], fieldTwo=[$1])\n"
        + "    ArrowTableScan(table=[[arrow, TEST]], fields=[[0, 1, 2]])\n\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectOneField() {
    String sql = "select \"fieldOne\" from test\n";
    String result = "fieldOne=1\n"
        + "fieldOne=2\n"
        + "fieldOne=3\n"
        + "fieldOne=4\n"
        + "fieldOne=5\n"
        + "fieldOne=6\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(fieldOne=[$0])\n"
        + "    ArrowTableScan(table=[[arrow, TEST]], fields=[[0, 1, 2]])\n\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithFilter() {
    String sql = "select \"fieldOne\", \"fieldTwo\"\n"
        + "from test\n"
        + "where \"fieldOne\" < 4";
    String result = "fieldOne=1; fieldTwo=abc\n"
        + "fieldOne=2; fieldTwo=def\n"
        + "fieldOne=3; fieldTwo=xyz\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(fieldOne=[$0], fieldTwo=[$1])\n"
        + "    ArrowFilter(condition=[<($0, 4)])\n"
        + "      ArrowTableScan(table=[[arrow, TEST]], fields=[[0, 1, 2]])\n\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .limit(3)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithMultipleFilters() {
    String sql = "select \"fieldOne\"\n"
        + "from test\n"
        + "where \"fieldOne\" > 2 and \"fieldOne\" < 6";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(fieldOne=[$0])\n"
        + "    ArrowFilter(condition=[AND(>($0, 2), <($0, 6))])\n"
        + "      ArrowTableScan(table=[[arrow, TEST]], fields=[[0, 1, 2]])\n\n";
    String result = "fieldOne=3\n"
        + "fieldOne=4\n"
        + "fieldOne=5\n";
    CalciteAssert.that()
        .with(ARROW)
        .query(sql)
        .limit(3)
        .returns(result)
        .explainContains(plan);
  }
}
