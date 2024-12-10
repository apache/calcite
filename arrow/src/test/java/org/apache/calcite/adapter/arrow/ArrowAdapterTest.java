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
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;

import static org.apache.calcite.test.Matchers.isListOf;

import static org.hamcrest.MatcherAssert.assertThat;

import static java.util.Objects.requireNonNull;

/**
 * Tests for the Apache Arrow adapter.
 */
@Execution(ExecutionMode.SAME_THREAD)
@ExtendWith(ArrowExtension.class)
class ArrowAdapterTest {
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

    File dataLocationFile = arrowFilesDirectory.resolve("arrowdata.arrow").toFile();
    ArrowData arrowDataGenerator = new ArrowData();
    arrowDataGenerator.writeArrowData(dataLocationFile);
    arrowDataGenerator.writeScottEmpData(arrowFilesDirectory);

    File datatypeLocationFile = arrowFilesDirectory.resolve("arrowdatatype.arrow").toFile();
    ArrowData arrowtypeDataGenerator = new ArrowData();
    arrowtypeDataGenerator.writeArrowDataType(datatypeLocationFile);

    arrow = ImmutableMap.of("model", modelFileTarget.toAbsolutePath().toString());
  }

  /** Test to read an Arrow file and check its field names. */
  @Test void testArrowSchema() {
    ArrowSchema arrowSchema = new ArrowSchema(arrowDataDirectory);
    Map<String, Table> tableMap = arrowSchema.getTableMap();
    RelDataTypeFactory typeFactory =
        new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType relDataType = tableMap.get("ARROWDATA").getRowType(typeFactory);

    assertThat(relDataType.getFieldNames(),
        isListOf("intField", "stringField", "floatField", "longField"));
  }

  @Test void testArrowProjectAllFields() {
    String sql = "select * from arrowdata\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=0; stringField=0; floatField=0.0; longField=0\n"
        + "intField=1; stringField=1; floatField=1.0; longField=1\n"
        + "intField=2; stringField=2; floatField=2.0; longField=2\n"
        + "intField=3; stringField=3; floatField=3.0; longField=3\n"
        + "intField=4; stringField=4; floatField=4.0; longField=4\n"
        + "intField=5; stringField=5; floatField=5.0; longField=5\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectAllFieldsExplicitly() {
    String sql = "select \"intField\", \"stringField\", \"floatField\", \"longField\" "
        + "from arrowdata\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=0; stringField=0; floatField=0.0; longField=0\n"
        + "intField=1; stringField=1; floatField=1.0; longField=1\n"
        + "intField=2; stringField=2; floatField=2.0; longField=2\n"
        + "intField=3; stringField=3; floatField=3.0; longField=3\n"
        + "intField=4; stringField=4; floatField=4.0; longField=4\n"
        + "intField=5; stringField=5; floatField=5.0; longField=5\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectAllFieldsExplicitlyPermutation() {
    String sql = "select \"stringField\", \"intField\", \"longField\", \"floatField\" "
        + "from arrowdata\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(stringField=[$1], intField=[$0], longField=[$3], floatField=[$2])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "stringField=0; intField=0; longField=0; floatField=0.0\n"
        + "stringField=1; intField=1; longField=1; floatField=1.0\n"
        + "stringField=2; intField=2; longField=2; floatField=2.0\n"
        + "stringField=3; intField=3; longField=3; floatField=3.0\n"
        + "stringField=4; intField=4; longField=4; floatField=4.0\n"
        + "stringField=5; intField=5; longField=5; floatField=5.0\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectSingleField() {
    String sql = "select \"intField\" from arrowdata\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=0\nintField=1\nintField=2\n"
        + "intField=3\nintField=4\nintField=5\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectTwoFields() {
    String sql = "select \"intField\", \"stringField\" from arrowdata\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=0; stringField=0\n"
        + "intField=1; stringField=1\n"
        + "intField=2; stringField=2\n"
        + "intField=3; stringField=3\n"
        + "intField=4; stringField=4\n"
        + "intField=5; stringField=5\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithIntegerFilter() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" < 4";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowFilter(condition=[<($0, 4)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=0; stringField=0\n"
        + "intField=1; stringField=1\n"
        + "intField=2; stringField=2\n"
        + "intField=3; stringField=3\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithMultipleFilterSameField() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" > 1 and \"intField\" < 4";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowFilter(condition=[SEARCH($0, Sarg[(1..4)])])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=2; stringField=2\n"
        + "intField=3; stringField=3\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithConjunctiveFilters() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\"=12 and \"stringField\"='12'";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowFilter(condition=[AND(=($0, 12), =($1, '12'))])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=12; stringField=12\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithDisjunctiveFilter() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\"=12 or \"stringField\"='12'";
    String plan;
    if (Bug.CALCITE_6293_FIXED) {
      plan = "PLAN=ArrowToEnumerableConverter\n"
          + "  ArrowProject(intField=[$0], stringField=[$1])\n"
          + "    ArrowFilter(condition=[OR(=($0, 12), =($1, '12'))])\n"
          + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    } else {
      plan = "PLAN=EnumerableCalc(expr#0..1=[{inputs}], expr#2=[12], "
          + "expr#3=[=($t0, $t2)], expr#4=['12':VARCHAR], expr#5=[=($t1, $t4)], "
          + "expr#6=[OR($t3, $t5)], proj#0..1=[{exprs}], $condition=[$t6])\n"
          + "  ArrowToEnumerableConverter\n"
          + "    ArrowProject(intField=[$0], stringField=[$1])\n"
          + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    }
    String result = "intField=12; stringField=12\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithInFilter() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" in (0, 1, 2)";
    String plan;
    if (Bug.CALCITE_6294_FIXED) {
      plan = "PLAN=ArrowToEnumerableConverter\n"
          + "  ArrowProject(intField=[$0], stringField=[$1])\n"
          + "    ArrowFilter(condition=[OR(=($0, 0), =($0, 1), =($0, 2))])\n"
          + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    } else {
      plan = "PLAN=EnumerableCalc(expr#0..1=[{inputs}], expr#2=[Sarg[0, 1, 2]], "
          + "expr#3=[SEARCH($t0, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
          + "  ArrowToEnumerableConverter\n"
          + "    ArrowProject(intField=[$0], stringField=[$1])\n"
          + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    }
    String result = "intField=0; stringField=0\n"
        + "intField=1; stringField=1\n"
        + "intField=2; stringField=2\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6295">[CALCITE-6295]
   * Support IS NOT NULL in Arrow adapter</a>. */
  @Test void testArrowProjectFieldsWithIsNotNullFilter() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" is not null\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
          + "  ArrowProject(intField=[$0], stringField=[$1])\n"
          + "    ArrowFilter(condition=[IS NOT NULL($0)])\n"
          + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returnsCount(50)
        .explainContains(plan);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6295">[CALCITE-6295]
   * Support IS NOT NULL in Arrow adapter</a>. */
  @Test void testConjunctiveIsNotNullFilters() {
    String sql = "select * from arrowdata\n"
        + "where \"intField\" is not null and \"stringField\" is not null";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowFilter(condition=[AND(IS NOT NULL($0), IS NOT NULL($1))])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returnsCount(50)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithEqualFilter() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" = 12";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowFilter(condition=[=($0, 12)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=12; stringField=12\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6618">[CALCITE-6618]
   * Support Not Equal in Arrow adapterr</a>. */
  @Test void testArrowProjectFieldsWithNotEqualFilter() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" <> 12";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowFilter(condition=[<>($0, 12)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=0; stringField=0\n"
        + "intField=1; stringField=1\n"
        + "intField=2; stringField=2\n"
        + "intField=3; stringField=3\n"
        + "intField=4; stringField=4\n"
        + "intField=5; stringField=5\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(6)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithBetweenFilter() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" between 1 and 3";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowFilter(condition=[SEARCH($0, Sarg[[1..3]])])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=1; stringField=1\n"
        + "intField=2; stringField=2\n"
        + "intField=3; stringField=3\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  /** Test case for
  * <a href="https://issues.apache.org/jira/browse/CALCITE-6296">[CALCITE-6296]
  * Support IS NULL in Arrow adapter</a>. */
  @Test void testArrowProjectFieldsWithIsNullFilter() {
    String sql = "select \"intField\", \"stringField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" is null";
    String plan = "ArrowToEnumerableConverter\n"
          + "  ArrowProject(intField=[$0], stringField=[$1])\n"
          + "    ArrowFilter(condition=[IS NULL($0)])\n"
          + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returnsCount(0)
        .explainContains(plan);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6296">[CALCITE-6296]
   * Support IS NULL in Arrow adapter</a>. */
  @Test void testConjunctiveIsNullFilters() {
    String sql = "select * from arrowdata\n"
        + "where \"intField\" is null and \"stringField\" is null";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowFilter(condition=[AND(IS NULL($0), IS NULL($1))])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returnsCount(0)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithFloatFilter() {
    String sql = "select * from arrowdata\n"
        + " where \"floatField\"=15.0";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowFilter(condition=[=($2, 15.0E0)])\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=15; stringField=15; floatField=15.0; longField=15\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithFilterOnLaterBatch() {
    String sql = "select \"intField\"\n"
        + "from arrowdata\n"
        + "where \"intField\"=25";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0])\n"
        + "    ArrowFilter(condition=[=($0, 25)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=25\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowSubquery() {
    String sql = "select \"intField\"\n"
        + "from (select \"intField\", \"stringField\" from arrowdata where \"stringField\" = '2')\n"
        + "where \"intField\" = 2";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0])\n"
        + "    ArrowFilter(condition=[AND(=($1, '2'), =($0, 2))])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n"
        + "\n";
    String result = "intField=2\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Disabled("UNION does not work")
  @Test void testArrowUnion() {
    String sql = "(select \"intField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" = 2)\n"
        + "  union \n"
        + "(select \"intField\"\n"
        + "from arrowdata\n"
        + "where \"intField\" = 1)\n";
    String plan = "PLAN=EnumerableUnion(all=[false])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowProject(intField=[$0])\n"
        + "      ArrowFilter(condition=[=($0, 2)])\n"
        + "        ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowProject(intField=[$0])\n"
        + "      ArrowFilter(condition=[=($0, 1)])\n"
        + "        ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=1\nintField=2\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testFieldWithSpace() {
    String sql = "select \"my Field\" from (select \"intField\", \"stringField\" as \"my Field\"\n"
        + "from arrowdata)\n"
        + "where \"my Field\" = '2'";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(my Field=[$1])\n"
        + "    ArrowFilter(condition=[=($1, '2')])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "my Field=2\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Disabled("literal with space is not supported")
  @Test void testLiteralWithSpace() {
    String sql = "select \"intField\", \"stringField\" as \"my Field\"\n"
        + "from arrowdata\n"
        + "where \"stringField\" = 'literal with space'";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], my Field=[$1])\n"
        + "    ArrowFilter(condition=[=($1, '2')])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testLiteralWithQuote() {
    String sql = "select \"intField\", \"stringField\" as \"my Field\"\n"
        + "from arrowdata\n"
        + "where \"stringField\" = ''''";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowFilter(condition=[=($1, '''')])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testTinyIntProject() {
    String sql = "select DEPTNO from DEPT";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(DEPTNO=[$0])\n"
        + "    ArrowTableScan(table=[[ARROW, DEPT]], fields=[[0, 1, 2]])\n\n";
    String result = "DEPTNO=10\nDEPTNO=20\nDEPTNO=30\nDEPTNO=40\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testSmallIntProject() {
    String sql = "select EMPNO from EMP";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(EMPNO=[$0])\n"
        + "    ArrowTableScan(table=[[ARROW, EMP]], fields=[[0, 1, 2, 3, 4, 5, 6, 7]])\n\n";
    String result = "EMPNO=7369\nEMPNO=7499\nEMPNO=7521\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(3)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testCastDecimalToInt() {
    String sql = "select CAST(LOSAL AS INT) as \"trunc\" from SALGRADE";
    String plan =
        "PLAN=EnumerableCalc(expr#0..2=[{inputs}], expr#3=[CAST($t1):INTEGER], trunc=[$t3])\n"
            + "  ArrowToEnumerableConverter\n"
            + "    ArrowTableScan(table=[[ARROW, SALGRADE]], fields=[[0, 1, 2]])\n\n";
    String result = "trunc=700\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .typeIs("[trunc INTEGER]")
        .limit(1)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testCastDecimalToFloat() {
    String sql = "select CAST(LOSAL AS FLOAT) as \"extra\" from SALGRADE";
    String plan = "PLAN=EnumerableCalc(expr#0..2=[{inputs}],"
        + " expr#3=[CAST($t1):FLOAT], extra=[$t3])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowTableScan(table=[[ARROW, SALGRADE]], fields=[[0, 1, 2]])\n\n";
    String result = "extra=700.0\nextra=1201.0\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .typeIs("[extra FLOAT]")
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testCastDecimalToDouble() {
    String sql = "select CAST(LOSAL AS DOUBLE) as \"extra\" from SALGRADE";
    String plan =
        "PLAN=EnumerableCalc(expr#0..2=[{inputs}], expr#3=[CAST($t1):DOUBLE], extra=[$t3])\n"
            + "  ArrowToEnumerableConverter\n"
            + "    ArrowTableScan(table=[[ARROW, SALGRADE]], fields=[[0, 1, 2]])\n\n";
    String result = "extra=700.0\nextra=1201.0\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .typeIs("[extra DOUBLE]")
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testCastIntToDouble() {
    String sql = "select CAST(\"intField\" AS DOUBLE) as \"dbl\" from arrowdata";
    String plan =
        "PLAN=EnumerableCalc(expr#0..3=[{inputs}], expr#4=[CAST($t0):DOUBLE], dbl=[$t4])\n"
            + "  ArrowToEnumerableConverter\n"
            + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "dbl=0.0\ndbl=1.0\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .typeIs("[dbl DOUBLE]")
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testStringOperation() {
    String sql = "select\n"
        + "  \"stringField\" || '_suffix' as \"field1\"\n"
        + "from arrowdata";
    String plan = "PLAN=EnumerableCalc(expr#0..3=[{inputs}], expr#4=['_suffix'], "
            + "expr#5=[||($t1, $t4)], field1=[$t5])\n"
            + "  ArrowToEnumerableConverter\n"
            + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "field1=0_suffix\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(1)
        .returns(result)
        .explainContains(plan);
  }


  @Disabled("join is not supported yet")
  @Test void testJoin() {
    String sql = "select t1.\"intField\", t2.\"intField\" "
        + "from arrowdata t1 join arrowdata t2 on t1.\"intField\" = t2.\"intField\"";
    String plan = "PLAN=EnumerableJoin(condition=[=($0, $4)], joinType=[inner])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=0\nintField=1\nintField=2\nintField=3\nintField=4\nintField=5\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(1)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testAggWithoutAggFunctions() {
    String sql = "select DISTINCT(\"intField\") as \"dep\" from arrowdata";
    String plan = "PLAN=EnumerableAggregate(group=[{0}])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "dep=0\ndep=1\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testAggWithAggFunctions() {
    String sql = "select JOB, SUM(SAL) as TOTAL from EMP GROUP BY JOB";
    String plan = "PLAN=EnumerableAggregate(group=[{2}], TOTAL=[SUM($5)])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowTableScan(table=[[ARROW, EMP]], fields=[[0, 1, 2, 3, 4, 5, 6, 7]])\n\n";
    String result = "JOB=SALESMAN; TOTAL=5600.00\nJOB=ANALYST; TOTAL=6000.00\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testFilteredAgg() {
    String sql = "select SUM(SAL) FILTER (WHERE COMM > 400) as SALESSUM from EMP";
    String plan = "PLAN=EnumerableAggregate(group=[{}], SALESSUM=[SUM($0) FILTER $1])\n"
        + "  EnumerableCalc(expr#0..7=[{inputs}], expr#8=[400:DECIMAL(19, 0)], expr#9=[>($t6, $t8)], "
        + "expr#10=[IS TRUE($t9)], SAL=[$t5], $f1=[$t10])\n"
        + "    ArrowToEnumerableConverter\n"
        + "      ArrowTableScan(table=[[ARROW, EMP]], fields=[[0, 1, 2, 3, 4, 5, 6, 7]])\n\n";
    String result = "SALESSUM=2500.00\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testFilteredAggGroupBy() {
    String sql = "select SUM(SAL) FILTER (WHERE COMM > 400) as SALESSUM from EMP group by EMPNO";
    String plan = "PLAN=EnumerableCalc(expr#0..1=[{inputs}], SALESSUM=[$t1])\n"
        + "  EnumerableAggregate(group=[{0}], SALESSUM=[SUM($1) FILTER $2])\n"
        + "    EnumerableCalc(expr#0..7=[{inputs}], expr#8=[400:DECIMAL(19, 0)], expr#9=[>($t6, $t8)], "
        + "expr#10=[IS TRUE($t9)], EMPNO=[$t0], SAL=[$t5], $f2=[$t10])\n"
        + "      ArrowToEnumerableConverter\n"
        + "        ArrowTableScan(table=[[ARROW, EMP]], fields=[[0, 1, 2, 3, 4, 5, 6, 7]])\n\n";
    String result = "SALESSUM=1250.00\nSALESSUM=null\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testAggGroupedByNullable() {
    String sql = "select COMM, SUM(SAL) as SALESSUM from EMP GROUP BY COMM";
    String plan = "PLAN=EnumerableAggregate(group=[{6}], SALESSUM=[SUM($5)])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowTableScan(table=[[ARROW, EMP]], fields=[[0, 1, 2, 3, 4, 5, 6, 7]])\n\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returnsUnordered("COMM=0.00; SALESSUM=1500.00",
            "COMM=1400.00; SALESSUM=1250.00",
            "COMM=300.00; SALESSUM=1600.00",
            "COMM=500.00; SALESSUM=1250.00",
            "COMM=null; SALESSUM=23425.00")
        .explainContains(plan);
  }

  @Test void testArrowAdapterLimitNoSort() {
    String sql = "select \"intField\"\n"
        + "from arrowdata\n"
        + "limit 2";
    String plan = "PLAN=EnumerableCalc(expr#0..3=[{inputs}], intField=[$t0])\n"
        + "  EnumerableLimit(fetch=[2])\n"
        + "    ArrowToEnumerableConverter\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=0\nintField=1\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowLimitOffsetNoSort() {
    String sql = "select \"intField\"\n"
        + "from arrowdata\n"
        + "limit 2 offset 2";
    String plan = "PLAN=EnumerableCalc(expr#0..3=[{inputs}], intField=[$t0])\n"
        + "  EnumerableLimit(offset=[2], fetch=[2])\n"
        + "    ArrowToEnumerableConverter\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=2\nintField=3\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowSortOnLong() {
    String sql = "select \"intField\" from arrowdata order by \"longField\" desc";
    String plan = "PLAN=EnumerableSort(sort0=[$1], dir0=[DESC])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowProject(intField=[$0], longField=[$3])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2, 3]])\n\n";
    String result = "intField=49\nintField=48\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6637">[CALCITE-6637]
   * Date type results should not be automatically converted to timestamp in Arrow adapters</a>. */
  @Test void testDateProject() {
    String sql = "select HIREDATE from EMP";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(HIREDATE=[$4])\n"
        + "    ArrowTableScan(table=[[ARROW, EMP]], fields=[[0, 1, 2, 3, 4, 5, 6, 7]])\n\n";
    String result = "HIREDATE=1980-12-17\nHIREDATE=1981-02-20\nHIREDATE=1981-02-22\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(3)
        .returns(result)
        .explainContains(plan);
  }


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6638">[CALCITE-6638]
   * Arrow adapter should support IS FALSE Operator and IS TRUE Operator</a>. */
  @Test void testArrowProjectWithIsTrueFilter() {
    String sql = "select \"booleanField\"\n"
        + "from arrowdatatype\n"
        + "where \"booleanField\" is true";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(booleanField=[$7])\n"
        + "    ArrowFilter(condition=[$7])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "booleanField=true\nbooleanField=true\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectWithIsTrueFilter2() {
    String sql = "select \"intField\"\n"
        + "from arrowdatatype\n"
        + "where (\"intField\" > 10) is true";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$2])\n"
        + "    ArrowFilter(condition=[>($2, 10)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "intField=11\nintField=12\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithIsFalseFilter() {
    String sql = "select \"booleanField\"\n"
        + "from arrowdatatype\n"
        + "where \"booleanField\" is false";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(booleanField=[$7])\n"
        + "    ArrowFilter(condition=[NOT($7)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "booleanField=false\nbooleanField=false\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }


  @Test void testArrowProjectFieldsWithIsNotTrueFilter() {
    String sql = "select \"booleanField\"\n"
        + "from arrowdatatype\n"
        + "where \"booleanField\" is not true";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(booleanField=[$7])\n"
        + "    ArrowFilter(condition=[IS NOT TRUE($7)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "booleanField=null\nbooleanField=false\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithIsNotFalseFilter() {
    String sql = "select \"booleanField\"\n"
        + "from arrowdatatype\n"
        + "where \"booleanField\" is not false";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(booleanField=[$7])\n"
        + "    ArrowFilter(condition=[IS NOT FALSE($7)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "booleanField=null\nbooleanField=true\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithIsUnknownFilter() {
    String sql = "select \"booleanField\"\n"
        + "from arrowdatatype\n"
        + "where \"booleanField\" is unknown";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(booleanField=[$7])\n"
        + "    ArrowFilter(condition=[IS NULL($7)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "booleanField=null\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(1)
        .returns(result)
        .explainContains(plan);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6684">[CALCITE-6684]
   * Arrow adapter should supports filter conditions of Decimal type</a>. */
  @Test void testArrowProjectFieldsWithDecimalFilter() {
    String sql = "select \"decimalField\"\n"
        + "from arrowdatatype\n"
        + "where \"decimalField\" = 1.00";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(decimalField=[$8])\n"
        + "    ArrowFilter(condition=[=($8, 1)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "decimalField=1.00\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithDoubleFilter() {
    String sql = "select \"doubleField\"\n"
        + "from arrowdatatype\n"
        + "where \"doubleField\" = 1.00";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(doubleField=[$6])\n"
        + "    ArrowFilter(condition=[=($6, 1.0E0)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "doubleField=1.0\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testArrowProjectFieldsWithStringFilter() {
    String sql = "select \"stringField\"\n"
        + "from arrowdatatype\n"
        + "where \"stringField\" = '1'";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(stringField=[$3])\n"
        + "    ArrowFilter(condition=[=($3, '1')])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATATYPE]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n\n";
    String result = "stringField=1\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }
}
