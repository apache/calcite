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
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Tests for the Apache Arrow adapter.
 */
class ArrowAdapterTest {
  private static Map<String, String> arrow;
  private static File arrowDataDirectory;

  @BeforeAll
  static void initializeArrowState(@TempDir Path sharedTempDir) throws IOException, SQLException {
    URL modelUrl = Objects.requireNonNull(
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

    arrow = ImmutableMap.of("model", modelFileTarget.toAbsolutePath().toString());
  }

  /** Test to read an Arrow file and check its field names. */
  @Test void testArrowSchema() {
    ArrowSchema arrowSchema = new ArrowSchema(arrowDataDirectory);
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
        .with(arrow)
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
        .with(arrow)
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
    String result = "intField=0; stringField=0\n"
        + "intField=1; stringField=1\n"
        + "intField=2; stringField=2\n"
        + "intField=3; stringField=3\n";
    String plan = "PLAN=ArrowToEnumerableConverter\n"
        + "  ArrowProject(intField=[$0], stringField=[$1])\n"
        + "    ArrowFilter(condition=[<($0, 4)])\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
    CalciteAssert.that()
        .with(arrow)
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
        .with(arrow)
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
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
    String result = "intField=25\n";
    CalciteAssert.that()
        .with(arrow)
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

  // TODO: test casts, including lossy casts
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
    String result = "dbl=0.0\ndbl=1.0\n";
    String plan =
        "PLAN=EnumerableCalc(expr#0..2=[{inputs}], expr#3=[CAST($t0):DOUBLE], dbl=[$t3])\n"
            + "  ArrowToEnumerableConverter\n"
            + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .typeIs("[dbl DOUBLE]")
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  // TODO: test IS NULL, IS NOT NULL

  // TODO: test 3-valued boolean logic, e.g. 'WHERE (x > 5) IS NOT FALSE',
  // 'SELECT (x > 5) ...'

  @Test void testStringOperation() {
    String sql = "select\n"
        + "  \"stringField\" || '_suffix' as \"field1\"\n"
        + "from arrowdata";
    String plan = "PLAN=EnumerableCalc(expr#0..2=[{inputs}], expr#3=['_suffix'], "
            + "expr#4=[||($t1, $t3)], field1=[$t4])\n"
            + "  ArrowToEnumerableConverter\n"
            + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
    String result = "field1=0_suffix\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(1)
        .returns(result)
        .explainContains(plan);
  }

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

  @Test void testAggWithoutAggFunctions() {
    String sql = "select DISTINCT(\"intField\") as \"dep\" from arrowdata";
    String result = "dep=0\ndep=1\n";

    String plan = "PLAN=EnumerableAggregate(group=[{0}])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testAggWithAggFunctions() {
    String sql = "select JOB, SUM(SAL) as TOTAL from EMP GROUP BY JOB";
    String result = "JOB=SALESMAN; TOTAL=5600.00\nJOB=ANALYST; TOTAL=6000.00\n";

    String plan = "PLAN=EnumerableAggregate(group=[{2}], TOTAL=[SUM($5)])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowTableScan(table=[[ARROW, EMP]], fields=[[0, 1, 2, 3, 4, 5, 6, 7]])\n\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testFilteredAgg() {
    // TODO add group by
    String sql = "select SUM(SAL) FILTER (WHERE COMM > 400) as SALESSUM from EMP";
    String result = "SALESSUM=2500.00\n";

    String plan = "PLAN=EnumerableAggregate(group=[{}], SALESSUM=[SUM($0) FILTER $1])\n"
        + "  EnumerableCalc(expr#0..7=[{inputs}], expr#8=[400], expr#9=[>($t6, $t8)], "
        + "expr#10=[IS TRUE($t9)], SAL=[$t5], $f1=[$t10])\n"
        + "    ArrowToEnumerableConverter\n"
        + "      ArrowTableScan(table=[[ARROW, EMP]], fields=[[0, 1, 2, 3, 4, 5, 6, 7]])\n\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .limit(2)
        .returns(result)
        .explainContains(plan);
  }

  @Test void testAggGroupedByNullable() {
    String sql = "select COMM, SUM(SAL) as SALESSUM from EMP GROUP BY COMM";
    String result = "COMM=0.00; SALESSUM=1500.00\n"
        + "COMM=1400.00; SALESSUM=1250.00\n"
        + "COMM=300.00; SALESSUM=1600.00\n"
        + "COMM=500.00; SALESSUM=1250.00\n"
        + "COMM=null; SALESSUM=23425.00";

    String plan = "PLAN=EnumerableAggregate(group=[{6}], SALESSUM=[SUM($5)])\n"
        + "  ArrowToEnumerableConverter\n"
        + "    ArrowTableScan(table=[[ARROW, EMP]], fields=[[0, 1, 2, 3, 4, 5, 6, 7]])\n\n";

    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returnsUnordered(result)
        .explainContains(plan);
  }

  @Test void testArrowAdapterLimitNoSort() {
    String sql = "select \"intField\"\n"
        + "from arrowdata\n"
        + "limit 2";
    String plan = "PLAN=EnumerableCalc(expr#0..2=[{inputs}], intField=[$t0])\n"
        + "  EnumerableLimit(fetch=[2])\n"
        + "    ArrowToEnumerableConverter\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
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
    String plan = "PLAN=EnumerableCalc(expr#0..2=[{inputs}], intField=[$t0])\n"
        + "  EnumerableLimit(offset=[2], fetch=[2])\n"
        + "    ArrowToEnumerableConverter\n"
        + "      ArrowTableScan(table=[[ARROW, ARROWDATA]], fields=[[0, 1, 2]])\n\n";
    String result = "intField=2\nintField=3\n";
    CalciteAssert.that()
        .with(arrow)
        .query(sql)
        .returns(result)
        .explainContains(plan);
  }
}
