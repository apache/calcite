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
package org.apache.calcite.adapter.file;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.TestUtil;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.calcite.adapter.file.FileAdapterTests.sql;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * System test of the Calcite file adapter, which can read and parse
 * HTML tables over HTTP, and also read CSV and JSON files from the filesystem.
 */
@ExtendWith(RequiresNetworkExtension.class)
class FileAdapterTest {

  static Stream<String> explainFormats() {
    return Stream.of("text", "dot");
  }

  /** Reads from a local file and checks the result. */
  @Test void testFileSelect() {
    final String sql = "select H1 from T1 where H0 = 'R1C0'";
    sql("testModel", sql).returns("H1=R1C1").ok();
  }

  /** Reads from a local file without table headers &lt;TH&gt; and checks the
   * result. */
  @Test @RequiresNetwork void testNoThSelect() {
    final String sql = "select \"col1\" from T1_NO_TH where \"col0\" like 'R0%'";
    sql("testModel", sql).returns("col1=R0C1").ok();
  }

  /** Reads from a local file - finds larger table even without &lt;TH&gt;
   * elements. */
  @Test void testFindBiggerNoTh() {
    final String sql = "select \"col4\" from TABLEX2 where \"col0\" like 'R1%'";
    sql("testModel", sql).returns("col4=R1C4").ok();
  }

  /** Reads from a URL and checks the result. */
  @Disabled("[CALCITE-1789] Wikipedia format change breaks file adapter test")
  @Test @RequiresNetwork void testUrlSelect() {
    final String sql = "select \"State\", \"Statehood\" from \"States_as_of\"\n"
        + "where \"State\" = 'California'";
    sql("wiki", sql).returns("State=California; Statehood=1850-09-09").ok();
  }

  /** Reads the EMPS table. */
  @Test void testSalesEmps() {
    final String sql = "select * from sales.emps";
    sql("sales", sql)
        .returns("EMPNO=100; NAME=Fred; DEPTNO=30",
            "EMPNO=110; NAME=Eric; DEPTNO=20",
            "EMPNO=110; NAME=John; DEPTNO=40",
            "EMPNO=120; NAME=Wilma; DEPTNO=20",
            "EMPNO=130; NAME=Alice; DEPTNO=40")
        .ok();
  }

  /** Reads the DEPTS table. */
  @Test void testSalesDepts() {
    final String sql = "select * from sales.depts";
    sql("sales", sql)
        .returns("DEPTNO=10; NAME=Sales",
            "DEPTNO=20; NAME=Marketing",
            "DEPTNO=30; NAME=Accounts")
        .ok();
  }

  /** Reads the DEPTS table from the CSV schema. */
  @Test void testCsvSalesDepts() {
    final String sql = "select * from sales.depts";
    sql("sales-csv", sql)
        .returns("DEPTNO=10; NAME=Sales",
            "DEPTNO=20; NAME=Marketing",
            "DEPTNO=30; NAME=Accounts")
        .ok();
  }

  /** Reads the EMPS table from the CSV schema. */
  @Test void testCsvSalesEmps() {
    final String sql = "select * from sales.emps";
    final String[] lines = {
        "EMPNO=100; NAME=Fred; DEPTNO=10; GENDER=; CITY=; EMPID=30; AGE=25; SLACKER=true; MANAGER=false; JOINEDAT=1996-08-03",
        "EMPNO=110; NAME=Eric; DEPTNO=20; GENDER=M; CITY=San Francisco; EMPID=3; AGE=80; SLACKER=null; MANAGER=false; JOINEDAT=2001-01-01",
        "EMPNO=110; NAME=John; DEPTNO=40; GENDER=M; CITY=Vancouver; EMPID=2; AGE=null; SLACKER=false; MANAGER=true; JOINEDAT=2002-05-03",
        "EMPNO=120; NAME=Wilma; DEPTNO=20; GENDER=F; CITY=; EMPID=1; AGE=5; SLACKER=null; MANAGER=true; JOINEDAT=2005-09-07",
        "EMPNO=130; NAME=Alice; DEPTNO=40; GENDER=F; CITY=Vancouver; EMPID=2; AGE=null; SLACKER=false; MANAGER=true; JOINEDAT=2007-01-01",
    };
    sql("sales-csv", sql).returns(lines).ok();
  }

  /** Reads the HEADER_ONLY table from the CSV schema. The CSV file has one
   * line - the column headers - but no rows of data. */
  @Test void testCsvSalesHeaderOnly() {
    final String sql = "select * from sales.header_only";
    sql("sales-csv", sql).returns().ok();
  }

  /** Reads the EMPTY table from the CSV schema. The CSV file has no lines,
   * therefore the table has a system-generated column called
   * "EmptyFileHasNoColumns". */
  @Test void testCsvSalesEmpty() {
    final String sql = "select * from sales.\"EMPTY\"";
    sql("sales-csv", sql)
        .checking(FileAdapterTest::checkEmpty)
        .ok();
  }

  private static void checkEmpty(ResultSet resultSet) {
    try {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertThat(metaData.getColumnCount(), is(1));
      assertThat(metaData.getColumnName(1), is("EmptyFileHasNoColumns"));
      assertThat(metaData.getColumnType(1), is(Types.BOOLEAN));
      String actual = FileAdapterTests.toString(resultSet);
      assertThat(actual, is(""));
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1754">[CALCITE-1754]
   * In Csv adapter, convert DATE and TIME values to int, and TIMESTAMP values
   * to long</a>. */
  @Test void testCsvGroupByTimestampAdd() {
    final String sql = "select count(*) as c,\n"
        + "  {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT) } as t\n"
        + "from EMPS group by {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT ) } ";
    sql("sales-csv", sql)
        .returnsUnordered("C=1; T=1996-08-04",
            "C=1; T=2002-05-04",
            "C=1; T=2005-09-08",
            "C=1; T=2007-01-02",
            "C=1; T=2001-01-02")
        .ok();
    final String sql2 = "select count(*) as c,\n"
        + "  {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT) } as t\n"
        + "from EMPS group by {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT ) } ";
    sql("sales-csv", sql2)
        .returnsUnordered("C=1; T=2002-06-03",
            "C=1; T=2005-10-07",
            "C=1; T=2007-02-01",
            "C=1; T=2001-02-01",
            "C=1; T=1996-09-03").ok();
    final String sql3 = "select\n"
        + " distinct {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT) } as t\n"
        + "from EMPS";
    sql("sales-csv", sql3)
        .returnsUnordered("T=2002-06-03",
            "T=2005-10-07",
            "T=2007-02-01",
            "T=2001-02-01",
            "T=1996-09-03").ok();
  }

  /** Reads the DEPTS table from the JSON schema. */
  @Test void testJsonSalesDepts() {
    final String sql = "select * from sales.depts";
    sql("sales-json", sql)
        .returns("DEPTNO=10; NAME=Sales",
            "DEPTNO=20; NAME=Marketing",
            "DEPTNO=30; NAME=Accounts")
        .ok();
  }

  /** Reads the EMPS table from the JSON schema. */
  @Test void testJsonSalesEmps() {
    final String sql = "select * from sales.emps";
    final String[] lines = {
        "EMPNO=100; NAME=Fred; DEPTNO=10; GENDER=; CITY=; EMPID=30; AGE=25; SLACKER=true; MANAGER=false; JOINEDAT=1996-08-03",
        "EMPNO=110; NAME=Eric; DEPTNO=20; GENDER=M; CITY=San Francisco; EMPID=3; AGE=80; SLACKER=null; MANAGER=false; JOINEDAT=2001-01-01",
        "EMPNO=110; NAME=John; DEPTNO=40; GENDER=M; CITY=Vancouver; EMPID=2; AGE=null; SLACKER=false; MANAGER=true; JOINEDAT=2002-05-03",
        "EMPNO=120; NAME=Wilma; DEPTNO=20; GENDER=F; CITY=; EMPID=1; AGE=5; SLACKER=null; MANAGER=true; JOINEDAT=2005-09-07",
        "EMPNO=130; NAME=Alice; DEPTNO=40; GENDER=F; CITY=Vancouver; EMPID=2; AGE=null; SLACKER=false; MANAGER=true; JOINEDAT=2007-01-01",
    };
    sql("sales-json", sql).returns(lines).ok();
  }

  /** Reads the EMPTY table from the JSON schema. The JSON file has no lines,
   * therefore the table has a system-generated column called
   * "EmptyFileHasNoColumns". */
  @Test void testJsonSalesEmpty() {
    final String sql = "select * from sales.\"EMPTY\"";
    sql("sales-json", sql)
        .checking(FileAdapterTest::checkEmpty)
        .ok();
  }

  /** Test returns the result of two json file joins. */
  @Test void testJsonJoinOnString() {
    final String sql = "select emps.EMPNO, emps.NAME, depts.deptno from emps\n"
        + "join depts on emps.deptno = depts.deptno";
    final String[] lines = {
        "EMPNO=100; NAME=Fred; DEPTNO=10",
        "EMPNO=110; NAME=Eric; DEPTNO=20",
        "EMPNO=120; NAME=Wilma; DEPTNO=20",
    };
    sql("sales-json", sql).returns(lines).ok();
  }

  /** The folder contains both JSON files and CSV files joins. */
  @Test void testJsonWithCsvJoin() {
    final String sql = "select emps.empno,\n"
        + " NAME,\n"
        + " \"DATE\".JOINEDAT\n"
        + " from \"DATE\"\n"
        + "join emps on emps.empno = \"DATE\".EMPNO\n"
        + "order by empno, name, joinedat limit 3";
    final String[] lines = {
        "EMPNO=100; NAME=Fred; JOINEDAT=1996-08-03",
        "EMPNO=110; NAME=Eric; JOINEDAT=2001-01-01",
        "EMPNO=110; NAME=Eric; JOINEDAT=2002-05-03",
    };
    sql("sales-json", sql)
        .returns(lines)
        .ok();
  }

  /** Tests an inline schema with a non-existent directory. */
  @Test void testBadDirectory() throws SQLException {
    Properties info = new Properties();
    info.put("model",
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'bad',\n"
            + "       factory: 'org.apache.calcite.adapter.file.FileSchemaFactory',\n"
            + "       operand: {\n"
            + "         directory: '/does/not/exist'\n"
            + "       }\n"
            + "     }\n"
            + "   ]\n"
            + "}");

    Connection connection =
        DriverManager.getConnection("jdbc:calcite:", info);
    // must print "directory ... not found" to stdout, but not fail
    ResultSet tables =
        connection.getMetaData().getTables(null, null, null, null);
    tables.next();
    tables.close();
    connection.close();
  }

  /**
   * Reads from a table.
   */
  @Test void testSelect() {
    sql("model", "select * from EMPS").ok();
  }

  @Test void testSelectSingleProjectGz() {
    sql("smart", "select name from EMPS").ok();
  }

  @Test void testSelectSingleProject() {
    sql("smart", "select name from DEPTS").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-898">[CALCITE-898]
   * Type inference multiplying Java long by SQL INTEGER</a>. */
  @Test void testSelectLongMultiplyInteger() {
    final String sql = "select empno * 3 as e3\n"
        + "from long_emps where empno = 100";

    sql("bug", sql).checking(resultSet -> {
      try {
        assertThat(resultSet.next(), is(true));
        Long o = (Long) resultSet.getObject(1);
        assertThat(o, is(300L));
        assertThat(resultSet.next(), is(false));
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    }).ok();
  }

  @Test void testCustomTable() {
    sql("model-with-custom-table", "select * from CUSTOM_TABLE.EMPS").ok();
  }

  @Test void testPushDownProject() {
    final String sql = "explain plan for select * from EMPS";
    final String expected = "PLAN=CsvTableScan(table=[[SALES, EMPS]], "
        + "fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n";
    sql("smart", sql).returns(expected).ok();
  }

  @Test void testPushDownProject2() {
    sql("smart", "explain plan for select name, empno from EMPS")
        .returns("PLAN=CsvTableScan(table=[[SALES, EMPS]], fields=[[1, 0]])\n")
        .ok();
    // make sure that it works...
    sql("smart", "select name, empno from EMPS")
        .returns("NAME=Fred; EMPNO=100",
            "NAME=Eric; EMPNO=110",
            "NAME=John; EMPNO=110",
            "NAME=Wilma; EMPNO=120",
            "NAME=Alice; EMPNO=130")
        .ok();
  }

  @ParameterizedTest
  @MethodSource("explainFormats")
  void testPushDownProjectAggregate(String format) {
    String expected = null;
    String extra = null;
    switch (format) {
    case "dot":
      expected = "PLAN=digraph {\n"
          + "\"CsvTableScan\\ntable = [SALES, EMPS\\n]\\nfields = [3]\\n\" -> "
          + "\"EnumerableAggregate\\ngroup = {0}\\nEXPR$1 = COUNT()\\n\" [label=\"0\"]\n"
          + "}\n";
      extra = " as dot ";
      break;
    case "text":
      expected = "PLAN="
          + "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT()])\n"
          + "  CsvTableScan(table=[[SALES, EMPS]], fields=[[3]])\n";
      extra = "";
      break;
    }
    final String sql = "explain plan " + extra + " for\n"
        + "select gender, count(*) from EMPS group by gender";
    sql("smart", sql).returns(expected).ok();
  }

  @ParameterizedTest
  @MethodSource("explainFormats")
  void testPushDownProjectAggregateWithFilter(String format) {
    String expected = null;
    String extra = null;
    switch (format) {
    case "dot":
      expected = "PLAN=digraph {\n"
          + "\"EnumerableCalc\\nexpr#0..1 = {inputs}\\nexpr#2 = 'F':VARCHAR\\nexpr#3 = =($t1, $t2)"
          + "\\nproj#0..1 = {exprs}\\n$condition = $t3\" -> \"EnumerableAggregate\\ngroup = "
          + "{}\\nEXPR$0 = MAX($0)\\n\" [label=\"0\"]\n"
          + "\"CsvTableScan\\ntable = [SALES, EMPS\\n]\\nfields = [0, 3]\\n\" -> "
          + "\"EnumerableCalc\\nexpr#0..1 = {inputs}\\nexpr#2 = 'F':VARCHAR\\nexpr#3 = =($t1, $t2)"
          + "\\nproj#0..1 = {exprs}\\n$condition = $t3\" [label=\"0\"]\n"
          + "}\n";
      extra = " as dot ";
      break;
    case "text":
      expected = "PLAN="
          + "EnumerableAggregate(group=[{}], EXPR$0=[MAX($0)])\n"
          + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=['F':VARCHAR], "
          + "expr#3=[=($t1, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
          + "    CsvTableScan(table=[[SALES, EMPS]], fields=[[0, 3]])\n";
      extra = "";
      break;
    }
    final String sql = "explain plan " + extra + " for\n"
        + "select max(empno) from EMPS where gender='F'";
    sql("smart", sql).returns(expected).ok();
  }

  @ParameterizedTest
  @MethodSource("explainFormats")
  void testPushDownProjectAggregateNested(String format) {
    String expected = null;
    String extra = null;
    switch (format) {
    case "dot":
      expected = "PLAN=digraph {\n"
          + "\"EnumerableAggregate\\ngroup = {0, 1}\\nQTY = COUNT()\\n\" -> "
          + "\"EnumerableAggregate\\ngroup = {1}\\nEXPR$1 = MAX($2)\\n\" [label=\"0\"]\n"
          + "\"CsvTableScan\\ntable = [SALES, EMPS\\n]\\nfields = [1, 3]\\n\" -> "
          + "\"EnumerableAggregate\\ngroup = {0, 1}\\nQTY = COUNT()\\n\" [label=\"0\"]\n"
          + "}\n";
      extra = " as dot ";
      break;
    case "text":
      expected = "PLAN="
          + "EnumerableAggregate(group=[{1}], EXPR$1=[MAX($2)])\n"
          + "  EnumerableAggregate(group=[{0, 1}], QTY=[COUNT()])\n"
          + "    CsvTableScan(table=[[SALES, EMPS]], fields=[[1, 3]])\n";
      extra = "";
      break;
    }
    final String sql = "explain plan " + extra + " for\n"
        + "select gender, max(qty)\n"
        + "from (\n"
        + "  select name, gender, count(*) qty\n"
        + "  from EMPS\n"
        + "  group by name, gender) t\n"
        + "group by gender";
    sql("smart", sql).returns(expected).ok();
  }

  @Test void testFilterableSelect() {
    sql("filterable-model", "select name from EMPS").ok();
  }

  @Test void testFilterableSelectStar() {
    sql("filterable-model", "select * from EMPS").ok();
  }

  /** Filter that can be fully handled by CsvFilterableTable. */
  @Test void testFilterableWhere() {
    final String sql =
        "select empno, gender, name from EMPS where name = 'John'";
    sql("filterable-model", sql)
        .returns("EMPNO=110; GENDER=M; NAME=John").ok();
  }

  /** Filter that can be partly handled by CsvFilterableTable. */
  @Test void testFilterableWhere2() {
    final String sql = "select empno, gender, name from EMPS\n"
        + " where gender = 'F' and empno > 125";
    sql("filterable-model", sql)
        .returns("EMPNO=130; GENDER=F; NAME=Alice").ok();
  }

  /** Filter that can be slightly handled by CsvFilterableTable. */
  @Test void testFilterableWhere3() {
    final String sql = "select empno, gender, name from EMPS\n"
        + " where gender <> 'M' and empno > 125";
    sql("filterable-model", sql)
        .returns("EMPNO=130; GENDER=F; NAME=Alice")
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2272">[CALCITE-2272]
   * Incorrect result for {@code name like '%E%' and city not like '%W%'}</a>.
   */
  @Test void testFilterableWhereWithNot1() {
    sql("filterable-model",
        "select name, empno from EMPS "
            + "where name like '%E%' and city not like '%W%' ")
        .returns("NAME=Eric; EMPNO=110")
        .ok();
  }

  /** Similar to {@link #testFilterableWhereWithNot1()};
   * But use the same column. */
  @Test void testFilterableWhereWithNot2() {
    sql("filterable-model",
        "select name, empno from EMPS "
            + "where name like '%i%' and name not like '%W%' ")
        .returns("NAME=Eric; EMPNO=110",
            "NAME=Alice; EMPNO=130")
        .ok();
  }

  @Test void testJson() {
    final String sql = "select * from archers\n";
    final String[] lines = {
        "id=19990101; dow=Friday; longDate=New Years Day; title=Tractor trouble.; "
            + "characters=[Alice, Bob, Xavier]; script=Julian Hyde; summary=; "
            + "lines=[Bob's tractor got stuck in a field., "
            + "Alice and Xavier hatch a plan to surprise Charlie.]",
        "id=19990103; dow=Sunday; longDate=Sunday 3rd January; "
            + "title=Charlie's surprise.; characters=[Alice, Zebedee, Charlie, Xavier]; "
            + "script=William Shakespeare; summary=; "
            + "lines=[Charlie is very surprised by Alice and Xavier's surprise plan.]",
    };
    sql("bug", sql)
        .returns(lines)
        .ok();
  }

  @Test void testJoinOnString() {
    final String sql = "select * from emps\n"
        + "join depts on emps.name = depts.name";
    sql("smart", sql).ok();
  }

  @Test void testWackyColumns() {
    final String sql = "select * from wacky_column_names where false";
    sql("bug", sql).returns().ok();

    final String sql2 = "select \"joined at\", \"naME\"\n"
        + "from wacky_column_names\n"
        + "where \"2gender\" = 'F'";
    sql("bug", sql2)
        .returns("joined at=2005-09-07; naME=Wilma",
            "joined at=2007-01-01; naME=Alice")
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1754">[CALCITE-1754]
   * In Csv adapter, convert DATE and TIME values to int, and TIMESTAMP values
   * to long</a>. */
  @Test void testGroupByTimestampAdd() {
    final String sql = "select count(*) as c,\n"
        + "  {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT) } as t\n"
        + "from EMPS group by {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT ) } ";
    sql("model", sql)
        .returnsUnordered("C=1; T=1996-08-04",
            "C=1; T=2002-05-04",
            "C=1; T=2005-09-08",
            "C=1; T=2007-01-02",
            "C=1; T=2001-01-02")
        .ok();

    final String sql2 = "select count(*) as c,\n"
        + "  {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT) } as t\n"
        + "from EMPS group by {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT ) } ";
    sql("model", sql2)
        .returnsUnordered("C=1; T=2002-06-03",
            "C=1; T=2005-10-07",
            "C=1; T=2007-02-01",
            "C=1; T=2001-02-01",
            "C=1; T=1996-09-03")
        .ok();
  }

  @Test void testUnionGroupByWithoutGroupKey() {
    final String sql = "select count(*) as c1 from EMPS group by NAME\n"
        + "union\n"
        + "select count(*) as c1 from EMPS group by NAME";
    sql("model", sql).ok();
  }

  @Test void testBoolean() {
    sql("smart", "select empno, slacker from emps where slacker")
        .returns("EMPNO=100; SLACKER=true").ok();
  }

  @Test void testReadme() {
    final String sql = "SELECT d.name, COUNT(*) cnt"
        + " FROM emps AS e"
        + " JOIN depts AS d ON e.deptno = d.deptno"
        + " GROUP BY d.name";
    sql("smart", sql)
        .returns("NAME=Sales; CNT=1", "NAME=Marketing; CNT=2").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-824">[CALCITE-824]
   * Type inference when converting IN clause to semijoin</a>. */
  @Test void testInToSemiJoinWithCast() {
    // Note that the IN list needs at least 20 values to trigger the rewrite
    // to a semijoin. Try it both ways.
    final String sql = "SELECT e.name\n"
        + "FROM emps AS e\n"
        + "WHERE cast(e.empno as bigint) in ";
    final int threshold = SqlToRelConverter.DEFAULT_IN_SUB_QUERY_THRESHOLD;
    sql("smart", sql + range(130, threshold - 5))
        .returns("NAME=Alice").ok();
    sql("smart", sql + range(130, threshold))
        .returns("NAME=Alice").ok();
    sql("smart", sql + range(130, threshold + 1000))
        .returns("NAME=Alice").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1051">[CALCITE-1051]
   * Underflow exception due to scaling IN clause literals</a>. */
  @Test void testInToSemiJoinWithoutCast() {
    final String sql = "SELECT e.name\n"
        + "FROM emps AS e\n"
        + "WHERE e.empno in "
        + range(130, SqlToRelConverter.DEFAULT_IN_SUB_QUERY_THRESHOLD);
    sql("smart", sql).returns("NAME=Alice").ok();
  }

  private String range(int first, int count) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(i == 0 ? "(" : ", ").append(first + i);
    }
    return sb.append(')').toString();
  }

  @Test void testDateType() throws SQLException {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug"));

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      ResultSet res = connection.getMetaData().getColumns(null, null,
          "DATE", "JOINEDAT");
      res.next();
      assertEquals(res.getInt("DATA_TYPE"), Types.DATE);

      res = connection.getMetaData().getColumns(null, null,
          "DATE", "JOINTIME");
      res.next();
      assertEquals(res.getInt("DATA_TYPE"), Types.TIME);

      res = connection.getMetaData().getColumns(null, null,
          "DATE", "JOINTIMES");
      res.next();
      assertEquals(res.getInt("DATA_TYPE"), Types.TIMESTAMP);

      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(
          "select \"JOINEDAT\", \"JOINTIME\", \"JOINTIMES\" from \"DATE\" where EMPNO = 100");
      resultSet.next();

      // date
      assertEquals(Date.class, resultSet.getDate(1).getClass());
      assertEquals(Date.valueOf("1996-08-03"), resultSet.getDate(1));

      // time
      assertEquals(Time.class, resultSet.getTime(2).getClass());
      assertEquals(Time.valueOf("00:01:02"), resultSet.getTime(2));

      // timestamp
      assertEquals(Timestamp.class, resultSet.getTimestamp(3).getClass());
      assertEquals(Timestamp.valueOf("1996-08-03 00:01:02"),
          resultSet.getTimestamp(3));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1072">[CALCITE-1072]
   * CSV adapter incorrectly parses TIMESTAMP values after noon</a>. */
  @Test void testDateType2() throws SQLException {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug"));

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      Statement statement = connection.createStatement();
      final String sql = "select * from \"DATE\"\n"
          + "where EMPNO >= 140 and EMPNO < 200";
      ResultSet resultSet = statement.executeQuery(sql);
      int n = 0;
      while (resultSet.next()) {
        ++n;
        final int empId = resultSet.getInt(1);
        final String date = resultSet.getString(2);
        final String time = resultSet.getString(3);
        final String timestamp = resultSet.getString(4);
        assertThat(date, is("2015-12-31"));
        switch (empId) {
        case 140:
          assertThat(time, is("07:15:56"));
          assertThat(timestamp, is("2015-12-31 07:15:56"));
          break;
        case 150:
          assertThat(time, is("13:31:21"));
          assertThat(timestamp, is("2015-12-31 13:31:21"));
          break;
        default:
          throw new AssertionError();
        }
      }
      assertThat(n, is(2));
      resultSet.close();
      statement.close();
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1673">[CALCITE-1673]
   * Query with ORDER BY or GROUP BY on TIMESTAMP column throws
   * CompileException</a>. */
  @Test void testTimestampGroupBy() throws SQLException {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug"));
    // Use LIMIT to ensure that results are deterministic without ORDER BY
    final String sql = "select \"EMPNO\", \"JOINTIMES\"\n"
        + "from (select * from \"DATE\" limit 1)\n"
        + "group by \"EMPNO\",\"JOINTIMES\"";
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(resultSet.next(), is(true));
      final Timestamp timestamp = resultSet.getTimestamp(2);
      assertThat(timestamp, isA(Timestamp.class));
      // Note: This logic is time zone specific, but the same time zone is
      // used in the CSV adapter and this test, so they should cancel out.
      assertThat(timestamp, is(Timestamp.valueOf("1996-08-03 00:01:02.0")));
    }
  }

  /** As {@link #testTimestampGroupBy()} but with ORDER BY. */
  @Test void testTimestampOrderBy() throws SQLException {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug"));
    final String sql = "select \"EMPNO\",\"JOINTIMES\" from \"DATE\"\n"
        + "order by \"JOINTIMES\"";
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(resultSet.next(), is(true));
      final Timestamp timestamp = resultSet.getTimestamp(2);
      assertThat(timestamp, is(Timestamp.valueOf("1996-08-03 00:01:02")));
    }
  }

  /** As {@link #testTimestampGroupBy()} but with ORDER BY as well as GROUP
   * BY. */
  @Test void testTimestampGroupByAndOrderBy() throws SQLException {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug"));
    final String sql = "select \"EMPNO\", \"JOINTIMES\" from \"DATE\"\n"
        + "group by \"EMPNO\",\"JOINTIMES\" order by \"JOINTIMES\"";
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(resultSet.next(), is(true));
      final Timestamp timestamp = resultSet.getTimestamp(2);
      assertThat(timestamp, is(Timestamp.valueOf("1996-08-03 00:01:02")));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1031">[CALCITE-1031]
   * In prepared statement, CsvScannableTable.scan is called twice</a>. To see
   * the bug, place a breakpoint in CsvScannableTable.scan, and note that it is
   * called twice. It should only be called once. */
  @Test void testPrepared() throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("caseSensitive", "true");
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", properties)) {
      final CalciteConnection calciteConnection = connection.unwrap(
          CalciteConnection.class);

      final Schema schema =
          FileSchemaFactory.INSTANCE
              .create(calciteConnection.getRootSchema(), null,
                  ImmutableMap.of("directory",
                      FileAdapterTests.resourcePath("sales-csv"), "flavor", "scannable"));
      calciteConnection.getRootSchema().add("TEST", schema);
      final String sql = "select * from \"TEST\".\"DEPTS\" where \"NAME\" = ?";
      final PreparedStatement statement2 =
          calciteConnection.prepareStatement(sql);

      statement2.setString(1, "Sales");
      final ResultSet resultSet1 = statement2.executeQuery();
      Consumer<ResultSet> expect = FileAdapterTests.expect("DEPTNO=10; NAME=Sales");
      expect.accept(resultSet1);
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1054">[CALCITE-1054]
   * NPE caused by wrong code generation for Timestamp fields</a>. */
  @Test void testFilterOnNullableTimestamp() throws Exception {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug"));

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      final Statement statement = connection.createStatement();

      // date
      final String sql1 = "select JOINEDAT from \"DATE\"\n"
          + "where JOINEDAT < {d '2000-01-01'}\n"
          + "or JOINEDAT >= {d '2017-01-01'}";
      final ResultSet joinedAt = statement.executeQuery(sql1);
      assertThat(joinedAt.next(), is(true));
      assertThat(joinedAt.getDate(1), is(Date.valueOf("1996-08-03")));

      // time
      final String sql2 = "select JOINTIME from \"DATE\"\n"
          + "where JOINTIME >= {t '07:00:00'}\n"
          + "and JOINTIME < {t '08:00:00'}";
      final ResultSet joinTime = statement.executeQuery(sql2);
      assertThat(joinTime.next(), is(true));
      assertThat(joinTime.getTime(1), is(Time.valueOf("07:15:56")));

      // timestamp
      final String sql3 = "select JOINTIMES,\n"
          + "  {fn timestampadd(SQL_TSI_DAY, 1, JOINTIMES)}\n"
          + "from \"DATE\"\n"
          + "where (JOINTIMES >= {ts '2003-01-01 00:00:00'}\n"
          + "and JOINTIMES < {ts '2006-01-01 00:00:00'})\n"
          + "or (JOINTIMES >= {ts '2003-01-01 00:00:00'}\n"
          + "and JOINTIMES < {ts '2007-01-01 00:00:00'})";
      final ResultSet joinTimes = statement.executeQuery(sql3);
      assertThat(joinTimes.next(), is(true));
      assertThat(joinTimes.getTimestamp(1),
          is(Timestamp.valueOf("2005-09-07 00:00:00")));
      assertThat(joinTimes.getTimestamp(2),
          is(Timestamp.valueOf("2005-09-08 00:00:00")));

      final String sql4 = "select JOINTIMES, extract(year from JOINTIMES)\n"
          + "from \"DATE\"";
      final ResultSet joinTimes2 = statement.executeQuery(sql4);
      assertThat(joinTimes2.next(), is(true));
      assertThat(joinTimes2.getTimestamp(1),
          is(Timestamp.valueOf("1996-08-03 00:01:02")));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1118">[CALCITE-1118]
   * NullPointerException in EXTRACT with WHERE ... IN clause if field has null
   * value</a>. */
  @Test void testFilterOnNullableTimestamp2() throws Exception {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug"));

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      final Statement statement = connection.createStatement();
      final String sql1 = "select extract(year from JOINTIMES)\n"
          + "from \"DATE\"\n"
          + "where extract(year from JOINTIMES) in (2006, 2007)";
      final ResultSet joinTimes = statement.executeQuery(sql1);
      assertThat(joinTimes.next(), is(true));
      assertThat(joinTimes.getInt(1), is(2007));

      final String sql2 = "select extract(year from JOINTIMES),\n"
          + "  count(0) from \"DATE\"\n"
          + "where extract(year from JOINTIMES) between 2007 and 2016\n"
          + "group by extract(year from JOINTIMES)";
      final ResultSet joinTimes2 = statement.executeQuery(sql2);
      assertThat(joinTimes2.next(), is(true));
      assertThat(joinTimes2.getInt(1), is(2007));
      assertThat(joinTimes2.getLong(2), is(1L));
      assertThat(joinTimes2.next(), is(true));
      assertThat(joinTimes2.getInt(1), is(2015));
      assertThat(joinTimes2.getLong(2), is(2L));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1427">[CALCITE-1427]
   * Code generation incorrect (does not compile) for DATE, TIME and TIMESTAMP
   * fields</a>. */
  @Test void testNonNullFilterOnDateType() throws SQLException {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug"));

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      final Statement statement = connection.createStatement();

      // date
      final String sql1 = "select JOINEDAT from \"DATE\"\n"
          + "where JOINEDAT is not null";
      final ResultSet joinedAt = statement.executeQuery(sql1);
      assertThat(joinedAt.next(), is(true));
      assertThat(joinedAt.getDate(1).getClass(), equalTo(Date.class));
      assertThat(joinedAt.getDate(1), is(Date.valueOf("1996-08-03")));

      // time
      final String sql2 = "select JOINTIME from \"DATE\"\n"
          + "where JOINTIME is not null";
      final ResultSet joinTime = statement.executeQuery(sql2);
      assertThat(joinTime.next(), is(true));
      assertThat(joinTime.getTime(1).getClass(), equalTo(Time.class));
      assertThat(joinTime.getTime(1), is(Time.valueOf("00:01:02")));

      // timestamp
      final String sql3 = "select JOINTIMES from \"DATE\"\n"
          + "where JOINTIMES is not null";
      final ResultSet joinTimes = statement.executeQuery(sql3);
      assertThat(joinTimes.next(), is(true));
      assertThat(joinTimes.getTimestamp(1).getClass(),
          equalTo(Timestamp.class));
      assertThat(joinTimes.getTimestamp(1),
          is(Timestamp.valueOf("1996-08-03 00:01:02")));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1427">[CALCITE-1427]
   * Code generation incorrect (does not compile) for DATE, TIME and TIMESTAMP
   * fields</a>. */
  @Test void testGreaterThanFilterOnDateType() throws SQLException {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug"));

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      final Statement statement = connection.createStatement();

      // date
      final String sql1 = "select JOINEDAT from \"DATE\"\n"
          + "where JOINEDAT > {d '1990-01-01'}";
      final ResultSet joinedAt = statement.executeQuery(sql1);
      assertThat(joinedAt.next(), is(true));
      assertThat(joinedAt.getDate(1).getClass(), equalTo(Date.class));
      assertThat(joinedAt.getDate(1), is(Date.valueOf("1996-08-03")));

      // time
      final String sql2 = "select JOINTIME from \"DATE\"\n"
          + "where JOINTIME > {t '00:00:00'}";
      final ResultSet joinTime = statement.executeQuery(sql2);
      assertThat(joinTime.next(), is(true));
      assertThat(joinTime.getTime(1).getClass(), equalTo(Time.class));
      assertThat(joinTime.getTime(1), is(Time.valueOf("00:01:02")));

      // timestamp
      final String sql3 = "select JOINTIMES from \"DATE\"\n"
          + "where JOINTIMES > {ts '1990-01-01 00:00:00'}";
      final ResultSet joinTimes = statement.executeQuery(sql3);
      assertThat(joinTimes.next(), is(true));
      assertThat(joinTimes.getTimestamp(1).getClass(),
          equalTo(Timestamp.class));
      assertThat(joinTimes.getTimestamp(1),
          is(Timestamp.valueOf("1996-08-03 00:01:02")));
    }
  }
}
