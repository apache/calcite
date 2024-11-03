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

import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Test cases for Arrow adapter data types.
 */
public class ArrowAdapterDataTypesTest {

  private static Map<String, String> arrow;
  private static File arrowDataDirectory;

  @BeforeAll
  static void initializeArrowState(@TempDir Path sharedTempDir)
      throws IOException, SQLException {
    URL modelUrl =
        requireNonNull(
            ArrowAdapterTest.class.getResource("/arrow-model.json"), "url");
    Path sourceModelFilePath = Sources.of(modelUrl).file().toPath();
    Path modelFileTarget = sharedTempDir.resolve("arrow-model.json");
    Files.copy(sourceModelFilePath, modelFileTarget);

    Path arrowFilesDirectory = sharedTempDir.resolve("arrow");
    Files.createDirectory(arrowFilesDirectory);
    arrowDataDirectory = arrowFilesDirectory.toFile();

    File dataLocationFile = arrowFilesDirectory.resolve("arrowdatatype.arrow").toFile();
    ArrowData arrowDataGenerator = new ArrowData();
    arrowDataGenerator.writeArrowDataType(dataLocationFile);

    arrow = ImmutableMap.of("model", modelFileTarget.toAbsolutePath().toString());
  }

  @Test void testTinyIntProject() {
    String sql = "select \"tinyIntField\" from arrowdatatype";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(tinyIntField=[$0])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "tinyIntField=0\ntinyIntField=1\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testSmallIntProject() {
    String sql = "select \"smallIntField\" from arrowdatatype";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(smallIntField=[$1])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "smallIntField=0\nsmallIntField=1\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testIntProject() {
    String sql = "select \"intField\" from arrowdatatype";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$2])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "intField=0\nintField=1\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testLongProject() {
    String sql = "select \"longField\" from arrowdatatype";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(longField=[$5])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "longField=0\nlongField=1\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testFloatProject() {
    String sql = "select \"floatField\" from arrowdatatype";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(floatField=[$4])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "floatField=0.0\nfloatField=1.0\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testDoubleProject() {
    String sql = "select \"doubleField\" from arrowdatatype";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(doubleField=[$6])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "doubleField=0.0\ndoubleField=1.0\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testDecimalProject() {
    String sql = "select \"decimalField\" from arrowdatatype";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(decimalField=[$8])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "decimalField=0.00\ndecimalField=1.00\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testDateProject() {
    String sql = "select \"dateField\" from arrowdatatype";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(dateField=[$9])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "dateField=1970-01-01\n"
        + "dateField=1970-01-02\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testBooleanProject() {
    String sql = "select \"booleanField\" from arrowdatatype";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(booleanField=[$7])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "booleanField=null\nbooleanField=true\nbooleanField=false\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(3)
        .returns(result)
        .explainContains(plan);
  }

}
