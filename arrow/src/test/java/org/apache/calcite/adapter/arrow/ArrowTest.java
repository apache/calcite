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
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * ArrowTest
 */
public class ArrowTest {

  /**
   * Test to read Arrow file and check it's field name.
   */
  @Test void testArrowSchema() {
    Source source = Sources.of(ArrowTest.class.getResource("/arrow"));
    ArrowSchema arrowSchema = new ArrowSchema(source.file().getAbsoluteFile());
    Map<String, Table> tableMap = arrowSchema.getTableMap();
    RelDataType relDataType = tableMap.get("TEST").getRowType(new
    JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT));

    Assertions.assertEquals(relDataType.getFieldNames().get(0), "fieldOne");
    Assertions.assertEquals(relDataType.getFieldNames().get(1), "fieldTwo");
    Assertions.assertEquals(relDataType.getFieldNames().get(2), "fieldThree");
  }

  static ImmutableMap<String, String> getDataset(String resourcePath) {
    return ImmutableMap.of("model",
        Sources.of(ArrowTest.class.getResource(resourcePath))
            .file().getAbsolutePath());
  }

  private static final ImmutableMap<String, String> ARROW =
      ArrowTest.getDataset("/arrow.json");

  @Test void testArrowProjectAllFields() {
    CalciteAssert.that()
        .with(ARROW)
        .query("select * from test\n")
        .limit(6)
        .returns("fieldOne=1; fieldTwo=abc; fieldThree=1.2\nfieldOne=2; fieldTwo=def; " +
            "fieldThree=3.4\nfieldOne=3; fieldTwo=xyz; fieldThree=5.6\n" +
            "fieldOne=4; fieldTwo=abcd; fieldThree=1.22\nfieldOne=5; fieldTwo=defg; " +
            "fieldThree=3.45\nfieldOne=6; fieldTwo=xyza; fieldThree=5.67\n")
        .explainContains("PLAN=ArrowToEnumerableConverter\n"
            + "  ArrowTableScan(table=[[arrow, TEST]], fields=[[0, 1, 2]])\n\n");
  }

  @Test void testArrowProjectTwoFields() {
    CalciteAssert.that()
        .with(ARROW)
        .query("select \"fieldOne\", \"fieldTwo\" from test\n")
        .limit(6)
        .returns("fieldOne=1; fieldTwo=abc\nfieldOne=2; fieldTwo=def\nfieldOne=3; fieldTwo=xyz\n" +
            "fieldOne=4; fieldTwo=abcd\nfieldOne=5; fieldTwo=defg\nfieldOne=6; fieldTwo=xyza\n")
        .explainContains("PLAN=ArrowToEnumerableConverter\n"
            + "  ArrowProject(fieldOne=[$0], fieldTwo=[$1])\n"
            + "    ArrowTableScan(table=[[arrow, TEST]], fields=[[0, 1, 2]])\n\n");
  }

  @Test void testArrowProjectOneField() {
    CalciteAssert.that()
        .with(ARROW)
        .query("select \"fieldOne\" from test\n")
        .limit(6)
        .returns("fieldOne=1\nfieldOne=2\nfieldOne=3\nfieldOne=4\nfieldOne=5\nfieldOne=6\n")
        .explainContains("PLAN=ArrowToEnumerableConverter\n"
            + "  ArrowProject(fieldOne=[$0])\n"
            + "    ArrowTableScan(table=[[arrow, TEST]], fields=[[0, 1, 2]])\n\n");
  }

  @Test void testArrowProjectFieldsWithFilter() {
    CalciteAssert.that()
        .with(ARROW)
        .query("select \"fieldOne\", \"fieldTwo\" from test where \"fieldOne\"<4")
        .limit(3)
        .returns("fieldOne=1; fieldTwo=abc\nfieldOne=2; fieldTwo=def\nfieldOne=3; fieldTwo=xyz\n")
        .explainContains("PLAN=EnumerableCalc(expr#0..2=[{inputs}], proj#0..1=[{exprs}])\n  ArrowToEnumerableConverter\n"
            + "    ArrowFilter(condition=[<($0, 4)])\n"
            + "      ArrowTableScan(table=[[arrow, TEST]], fields=[[0, 1, 2]])\n\n");
  }
}
