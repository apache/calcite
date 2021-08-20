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
 * Test of Calcite Arrow adapter reading from Arrow files.
 */
public class ArrowTest {

  /**
   * Test to read Arrow file and check it's field name.
   */
  @Test void testArrowSchema() {
    Source source = Sources.of(ArrowTest.class.getResource("/arrow"));
    ArrowSchema arrowSchema = new ArrowSchema(source.file().getAbsoluteFile());
    Map<String, Table> tableMap = arrowSchema.getTableMap();
    RelDataType relDataType = tableMap.get("ARROWDATA").getRowType(new
        JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT));

    Assertions.assertEquals(relDataType.getFieldNames().get(0), "intField");
    Assertions.assertEquals(relDataType.getFieldNames().get(1), "stringField");
    Assertions.assertEquals(relDataType.getFieldNames().get(2), "floatField");
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
        .query("select * from arrowdata\n")
        .limit(6)
        .returns("intField=0; stringField=0; floatField=0.0\n"
               + "intField=1; stringField=1; floatField=1.0\n"
               + "intField=2; stringField=2; floatField=2.0\n"
               + "intField=3; stringField=3; floatField=3.0\n"
               + "intField=4; stringField=4; floatField=4.0\n"
               + "intField=5; stringField=5; floatField=5.0\n")
        .explainContains("PLAN=ArrowToEnumerableConverter\n"
                       + "  ArrowTableScan(table=[[arrow, ARROWDATA]], fields=[[0, 1, 2]])\n\n");
  }

  @Test void testArrowProjectSingleField() {
    CalciteAssert.that()
        .with(ARROW)
        .query("select \"intField\" from arrowdata\n")
        .limit(6)
        .returns("intField=0\nintField=1\nintField=2\nintField=3\nintField=4\nintField=5\n")
        .explainContains("PLAN=ArrowToEnumerableConverter\n"
            + "  ArrowProject(intField=[$0])\n"
            + "    ArrowTableScan(table=[[arrow, ARROWDATA]], fields=[[0, 1, 2]])\n\n");
  }

  @Test void testArrowProjectTwoFields() {
    CalciteAssert.that()
        .with(ARROW)
        .query("select \"intField\", \"stringField\" from arrowdata\n")
        .limit(6)
        .returns("intField=0; stringField=0\n"
               + "intField=1; stringField=1\n"
               + "intField=2; stringField=2\n"
               + "intField=3; stringField=3\n"
               + "intField=4; stringField=4\n"
               + "intField=5; stringField=5\n")
        .explainContains("PLAN=ArrowToEnumerableConverter\n"
            + "  ArrowProject(intField=[$0], stringField=[$1])\n"
            + "    ArrowTableScan(table=[[arrow, ARROWDATA]], fields=[[0, 1, 2]])\n\n");
  }

  @Test void testArrowProjectFieldsWithIntegerFilter() {
    CalciteAssert.that()
        .with(ARROW)
        .query("select \"intField\", \"stringField\" from arrowdata where \"intField\"<4")
        .returns("intField=0; stringField=0\nintField=1; stringField=1\nintField=2; stringField=2\nintField=3; stringField=3\n")
        .explainContains("PLAN=ArrowToEnumerableConverter\n"
            + "  ArrowProject(intField=[$0], stringField=[$1])\n"
            + "    ArrowFilter(condition=[<($0, 4)])\n"
            + "      ArrowTableScan(table=[[arrow, ARROWDATA]], fields=[[0, 1, 2]])\n\n");
  }

  @Test void testArrowProjectFieldsWithMultipleFilters() {
    CalciteAssert.that()
        .with(ARROW)
        .query(
            "select \"intField\", \"stringField\" from arrowdata " +
                "where \"intField\"=12 and \"stringField\"='12'")
        .returns("intField=12; stringField=12\n")
        .explainContains("PLAN=ArrowToEnumerableConverter\n"
            + "  ArrowProject(intField=[$0], stringField=[$1])\n"
            + "    ArrowFilter(condition=[AND(=($0, 12), =($1, '12'))])\n"
            + "      ArrowTableScan(table=[[arrow, ARROWDATA]], fields=[[0, 1, 2]])\n\n");
  }

  @Test void testArrowProjectFieldsWithFloatFilter() {
    CalciteAssert.that()
        .with(ARROW)
        .query(
            "select * from arrowdata where \"floatField\"=15.0")
        .returns("intField=15; stringField=15; floatField=15.0\n")
        .explainContains("PLAN=ArrowToEnumerableConverter\n"
            + "  ArrowFilter(condition=[=(CAST($2):DOUBLE, 15.0)])\n"
            + "    ArrowTableScan(table=[[arrow, ARROWDATA]], fields=[[0, 1, 2]])\n\n");
  }
}
