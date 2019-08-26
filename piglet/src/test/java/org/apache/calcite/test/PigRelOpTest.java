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

package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.util.Util;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link org.apache.calcite.piglet.PigRelOpVisitor}.
 */
public class PigRelOpTest extends PigRelTestBase {
  /**
   * SQL dilect for the tests.
    */
  private static class PigRelSqlDialect extends SqlDialect {
    static final SqlDialect DEFAULT =
        new CalciteSqlDialect(emptyContext()
                                  .withDatabaseProduct(DatabaseProduct.CALCITE));

    private PigRelSqlDialect(Context context) {
      super(context);
    }
  }


  private void testPigRelOpTranslation(String pigScript, String expectedRelPlan,
      String expectedOptimizedPlan) throws IOException {
    testPigRelOpTranslation(null, pigScript, expectedRelPlan, false);
    if (!expectedRelPlan.equals(expectedOptimizedPlan)) {
      testPigRelOpTranslation(null, pigScript, expectedOptimizedPlan, true);
    }
  }

  private void testPigRelOpTranslation(String pigScript, String expectedRelPlan)
      throws IOException {
    testPigRelOpTranslation(pigScript, expectedRelPlan, expectedRelPlan);
  }

  private void testPigRelOpTranslation(String pigAlias, String pigScript,
      String expectedRelPlan, boolean optimized) throws IOException {
    RelNode rel;
    if (pigAlias == null) {
      rel = converter.pigQuery2Rel(pigScript, optimized).get(0);
    } else {
      converter.pigQuery2Rel(pigScript, optimized);
      rel = converter.getBuilder().getRel(pigAlias);
    }
    assertThat(Util.toLinux(RelOptUtil.toString(rel)), is(expectedRelPlan));
  }

  private void testSQLTranslation(String pigScript, String expectedSQL) {
    try {
      String sql = converter.pigToSql(pigScript, PigRelSqlDialect.DEFAULT).get(0);
      assertThat(sql, is(expectedSQL));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private void testRunPigScript(String pigScript, String expectedResult) throws IOException {
    RelNode rel = converter.pigQuery2Rel(pigScript, false).get(0);
    final StringWriter sw = new StringWriter();
    CalciteHandler.dump(rel, new PrintWriter(sw));
    assertThat(Util.toLinux(sw.toString()), is(expectedResult));
  }

  private static void writeToFile(File f, String[] inputData) throws IOException {
    PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(f), "UTF-8"));
    for (String input : inputData) {
      pw.print(input);
      pw.print("\n");
    }
    pw.close();
  }

  @Test
  public void testLoadFromFile() throws IOException {
    String datadir = "/tmp/pigdata";
    String schema = "{\"fields\":["
                        + "{\"name\":\"x\",\"type\":55,\"schema\":null},"
                        + "{\"name\":\"y\",\"type\":10,\"schema\":null},"
                        + "{\"name\":\"z\",\"type\":25,\"schema\":null}],"
                        + "\"version\":0,\"sortKeys\":[],\"sortKeyOrders\":[]}";
    File inputDir = new File(datadir, "testTable");
    inputDir.mkdirs();
    File inputSchemaFile = new File(inputDir, ".pig_schema");
    writeToFile(inputSchemaFile, new String[]{schema});

    String script = ""
                        + "A = LOAD '" + inputDir.getAbsolutePath() + "' using PigStorage();\n"
                        + "B = FILTER A BY z > 5.5;\n"
                        + "C = GROUP B BY x;\n";
    String expectedPlan = ""
                              + "LogicalProject(group=[$0], B=[$1])\n"
                              + "  LogicalAggregate(group=[{0}], B=[COLLECT($1)])\n"
                              + "    LogicalProject(x=[$0], $f1=[ROW($0, $1, $2)])\n"
                              + "      LogicalFilter(condition=[>($2, 5.5E0)])\n"
                              + "        LogicalTableScan(table=[[/tmp/pigdata/testTable]])\n";
    testPigRelOpTranslation(script, expectedPlan);
  }

  @Test
  public void testLoadWithoutSchema() throws IOException {
    String noSchema = "A = LOAD 'scott.DEPT';";
    String expectedPlan = "LogicalTableScan(table=[[scott, DEPT]])\n";
    String expectedResult = ""
                                + "(10,ACCOUNTING,NEW YORK)\n"
                                + "(20,RESEARCH,DALLAS)\n"
                                + "(30,SALES,CHICAGO)\n"
                                + "(40,OPERATIONS,BOSTON)\n";
    testPigRelOpTranslation(noSchema, expectedPlan);
    testRunPigScript(noSchema, expectedResult);
  }

  @Test
  public void testLoadWithSchema() throws IOException {
    String pigScript = ""
                           + "A = LOAD 'testSchema.testTable' as (a:int, b:long, c:float, "
                           + "d:double, e:chararray, "
                           + "f:bytearray, g:boolean, "
                           + "h:datetime, i:biginteger, j:bigdecimal, k1:tuple(), k2:tuple"
                           + "(k21:int, k22:float), "
                           + "l1:bag{}, "
                           + "l2:bag{l21:(l22:int, l23:float)}, m1:map[], m2:map[int], m3:map["
                           + "(m3:float)])\n;";
    testPigRelOpTranslation(pigScript,
        "LogicalTableScan(table=[[testSchema, testTable]])\n");

    testPigRelOpTranslation(
        "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);",
        "LogicalTableScan(table=[[scott, DEPT]])\n");
  }

  @Test
  public void testFilter() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
                        + "B = FILTER A BY DEPTNO == 10;\n";
    String expectedPlan = ""
                              + "LogicalFilter(condition=[=($0, 10)])\n"
                              + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    String expectedResult = "(10,ACCOUNTING,NEW YORK)\n";
    testPigRelOpTranslation(script, expectedPlan);
    testRunPigScript(script, expectedResult);
    testSQLTranslation(script, "SELECT *\n"
                                   + "FROM scott.DEPT\n"
                                   + "WHERE DEPTNO = 10");
  }

  @Test
  public void testSample() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
                        + "B = SAMPLE A 0.5;\n";
    String expectedPlan = ""
                              + "LogicalFilter(condition=[<(RAND(), 5E-1)])\n"
                              + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    testPigRelOpTranslation(script, expectedPlan);
    String sql = ""
                     + "SELECT *\n"
                     + "FROM scott.DEPT\n"
                     + "WHERE RAND() < 0.5";
    testSQLTranslation(script, sql);
  }

  @Test
  public void testSplit() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.EMP'as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                        + "MGR:int, "
                        + "    HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                        + "SPLIT A INTO B1 IF DEPTNO == 10, B2 IF  DEPTNO == 20;\n"
                        + "B = UNION B1, B2;\n";
    String expectedScan = "  LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedPlan = ""
                              + "LogicalUnion(all=[true])\n"
                              + "  LogicalFilter(condition=[=($7, 10)])\n"
                              + "    LogicalTableScan(table=[[scott, EMP]])\n"
                              + "  LogicalFilter(condition=[=($7, 20)])\n"
                              + "    LogicalTableScan(table=[[scott, EMP]])\n";

    String expectedResult = ""
                                + "(7782,CLARK,MANAGER,7839,1981-06-09,2450.00,null,10)\n"
                                + "(7839,KING,PRESIDENT,null,1981-11-17,5000.00,null,10)\n"
                                + "(7934,MILLER,CLERK,7782,1982-01-23,1300.00,null,10)\n"
                                + "(7369,SMITH,CLERK,7902,1980-12-17,800.00,null,20)\n"
                                + "(7566,JONES,MANAGER,7839,1981-02-04,2975.00,null,20)\n"
                                + "(7788,SCOTT,ANALYST,7566,1987-04-19,3000.00,null,20)\n"
                                + "(7876,ADAMS,CLERK,7788,1987-05-23,1100.00,null,20)\n"
                                + "(7902,FORD,ANALYST,7566,1981-12-03,3000.00,null,20)\n";

    testPigRelOpTranslation("B1", script, "LogicalFilter(condition=[=($7, 10)])\n"
                                              + expectedScan, false);
    testPigRelOpTranslation("B2", script, "LogicalFilter(condition=[=($7, 20)])\n"
                                              + expectedScan, false);
    testPigRelOpTranslation(script, expectedPlan);
    testRunPigScript(script, expectedResult);
    String expectedSql = ""
                             + "SELECT *\n"
                             + "  FROM scott.EMP\n"
                             + "  WHERE DEPTNO = 10\n"
                             + "UNION ALL\n"
                             + "  SELECT *\n"
                             + "  FROM scott.EMP\n"
                             + "  WHERE DEPTNO = 20";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testUDF() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
                        + "B = FILTER A BY ENDSWITH(DNAME, 'LES');\n";
    String expectedPlan = ""
                              + "LogicalFilter(condition=[ENDSWITH(PIG_TUPLE($1, 'LES'))])\n"
                              + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    String expectedResult = "(30,SALES,CHICAGO)\n";
    testPigRelOpTranslation(script, expectedPlan);
    testRunPigScript(script, expectedResult);
    String expectedSql = ""
                             + "SELECT *\n"
                             + "FROM scott.DEPT\n"
                             + "WHERE ENDSWITH(PIG_TUPLE(DNAME, 'LES'))";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testSimpleForEach1() throws IOException {
    String pigScript = ""
                           + "A = LOAD 'testSchema.testTable' as (a:int, b:long, c:float, "
                           + "d:double, e:chararray, "
                           + "f:bytearray, g:boolean, "
                           + "h:datetime, i:biginteger, j:bigdecimal, k1:tuple(), k2:tuple"
                           + "(k21:int, k22:float), "
                           + "l1:bag{}, "
                           + "l2:bag{l21:(l22:int, l23:float)}, m1:map[], m2:map[int], m3:map["
                           + "(m3:float)]);\n"
                           + "B = FOREACH A GENERATE a, a as a2, b, c, d, e, f, g, h, i, j, k2, "
                           + "l2, m2, null as "
                           + "n:chararray; \n";
    String expected =
        "LogicalProject(a=[$0], a2=[$0], b=[$1], c=[$2], d=[$3], e=[$4], f=[$5], g=[$6], h=[$7], "
            + "i=[$8], j=[$9], "
            + "k2=[$11], l2=[$13], m2=[$15], n=[null:VARCHAR])\n"
            + "  LogicalTableScan(table=[[testSchema, testTable]])\n";
    testPigRelOpTranslation(pigScript, expected);
    String expectedSql = ""
                             + "SELECT a, a AS a2, b, c, d, e, f, g, h, i, j, k2, l2, m2, NULL AS"
                             + " n\n"
                             + "FROM testSchema.testTable";
    testSQLTranslation(pigScript, expectedSql);
  }

  @Test
  public void testSimpleForEach2() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                        + "MGR:int, "
                        + "    HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                        + "B = FOREACH A GENERATE DEPTNO + 10 as dept, MGR;\n";
    String expectedPlan = ""
                              + "LogicalProject(dept=[+($7, 10)], MGR=[$3])\n"
                              + "  LogicalTableScan(table=[[scott, EMP]])\n";

    String expectedResult = ""
                                + "(30,7902)\n"
                                + "(40,7698)\n"
                                + "(40,7698)\n"
                                + "(30,7839)\n"
                                + "(40,7698)\n"
                                + "(40,7839)\n"
                                + "(20,7839)\n"
                                + "(30,7566)\n"
                                + "(20,null)\n"
                                + "(40,7698)\n"
                                + "(30,7788)\n"
                                + "(40,7698)\n"
                                + "(30,7566)\n"
                                + "(20,7782)\n";
    testPigRelOpTranslation(script, expectedPlan);
    testRunPigScript(script, expectedResult);
    String expectedSql = ""
                             + "SELECT DEPTNO + 10 AS dept, MGR\n"
                             + "FROM scott.EMP";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testSimpleForEach3() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                        + "MGR:int, "
                        + "    HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                        + "B = FILTER A BY JOB != 'CLERK';\n"
                        + "C = GROUP B BY (DEPTNO, JOB);\n"
                        + "D = FOREACH C GENERATE flatten(group) as (dept, job), flatten(B);\n"
                        + "E = ORDER D BY dept, job;\n";
    String expectedPlan = ""
                              + "LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
                              + "  LogicalProject(dept=[$0], job=[$1], EMPNO=[$3], ENAME=[$4], "
                              + "JOB=[$5], MGR=[$6], "
                              + "HIREDATE=[$7], SAL=[$8], COMM=[$9], DEPTNO=[$10])\n"
                              + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], "
                              + "requiredColumns=[{2}])\n"
                              + "      LogicalProject(dept=[$0.DEPTNO], job=[$0.JOB], B=[$1])\n"
                              + "        LogicalProject(group=[ROW($0, $1)], B=[$2])\n"
                              + "          LogicalAggregate(group=[{0, 1}], B=[COLLECT($2)])\n"
                              + "            LogicalProject(DEPTNO=[$7], JOB=[$2], $f2=[ROW($0, "
                              + "$1, $2, $3, $4, $5, "
                              + "$6, $7)])\n"
                              + "              LogicalFilter(condition=[<>($2, 'CLERK')])\n"
                              + "                LogicalTableScan(table=[[scott, EMP]])\n"
                              + "      Uncollect\n"
                              + "        LogicalProject($f0=[$cor0.B])\n"
                              + "          LogicalValues(tuples=[[{ 0 }]])\n";

    testPigRelOpTranslation(script, expectedPlan);

    String expectedSql = ""
                             + "SELECT $cor1.DEPTNO AS dept, $cor1.JOB AS job, $cor1.EMPNO, $cor1"
                             + ".ENAME, $cor1.JOB0 "
                             + "AS JOB, $cor1.MGR, $cor1.HIREDATE, $cor1.SAL, $cor1.COMM, $cor1"
                             + ".DEPTNO0 AS DEPTNO\n"
                             + "FROM (SELECT DEPTNO, JOB, COLLECT(ROW(EMPNO, ENAME, JOB, MGR, "
                             + "HIREDATE, SAL, COMM, "
                             + "DEPTNO)) AS $f2\n"
                             + "      FROM scott.EMP\n"
                             + "      WHERE JOB <> 'CLERK'\n"
                             + "      GROUP BY DEPTNO, JOB) AS $cor1,\n"
                             + "    LATERAL UNNEST (SELECT $cor1.$f2 AS $f0\n"
                             + "        FROM (VALUES  (0)) AS t (ZERO)) AS t3 (EMPNO, ENAME, JOB,"
                             + " MGR, HIREDATE, SAL,"
                             + " COMM, DEPTNO) AS t30\n"
                             + "ORDER BY $cor1.DEPTNO, $cor1.JOB";
    testSQLTranslation(script, expectedSql);

    //TODO fix Calite execution
//    String expectedResult = ""
//        + "(10,7782,CLARK,MANAGER,7839,1981-06-09,2450.00,null,10)\n"
//        + "(10,7839,KING,PRESIDENT,null,1981-11-17,5000.00,null,10)\n"
//        + "(20,7566,JONES,MANAGER,7839,1981-02-04,2975.00,null,20)\n"
//        + "(20,7788,SCOTT,ANALYST,7566,1987-04-19,3000.00,null,20)\n"
//        + "(20,7902,FORD,ANALYST,7566,1981-12-03,3000.00,null,20)\n"
//        + "(30,7499,ALLEN,SALESMAN,7698,1981-02-20,1600.00,300.00,30)\n"
//        + "(30,7521,WARD,SALESMAN,7698,1981-02-22,1250.00,500.00,30)\n"
//        + "(30,7654,MARTIN,SALESMAN,7698,1981-09-28,1250.00,1400.00,30)\n"
//        + "(30,7698,BLAKE,MANAGER,7839,1981-01-05,2850.00,null,30)\n"
//        + "(30,7844,TURNER,SALESMAN,7698,1981-09-08,1500.00,0.00,30)\n";
//     testRunPigScript(script, expectedResult);
  }

  @Test
  public void testForEachNested() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                        + "MGR:int, "
                        + "HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                        + "B = GROUP A BY DEPTNO;\n"
                        + "C = FOREACH B {\n"
                        + "      S = FILTER A BY JOB != 'CLERK';\n"
                        + "      Y = FOREACH S GENERATE ENAME, JOB, DEPTNO, SAL;\n"
                        + "      X = ORDER Y BY SAL;\n"
                        + "      GENERATE group, COUNT(X) as cnt, flatten(X), BigDecimalMax(X.SAL);"
                        + "}\n"
                        + "D = ORDER C BY $0;\n";
    String expectedPlan =
        ""
            + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  LogicalProject(group=[$0], cnt=[$1], ENAME=[$4], JOB=[$5], DEPTNO=[$6], SAL=[$7], "
            + "$f3=[$3])\n"
            + "    LogicalCorrelate(correlation=[$cor1], joinType=[inner], requiredColumns=[{2}])\n"
            + "      LogicalProject(group=[$0], cnt=[COUNT(PIG_BAG($2))], X=[$2], "
            + "$f3=[BigDecimalMax(PIG_BAG(MULTISET_PROJECTION($2, 3)))])\n"
            + "        LogicalCorrelate(correlation=[$cor0], joinType=[inner], "
            + "requiredColumns=[{1}])\n"
            + "          LogicalProject(group=[$0], A=[$1])\n"
            + "            LogicalAggregate(group=[{0}], A=[COLLECT($1)])\n"
            + "              LogicalProject(DEPTNO=[$7], $f1=[ROW($0, $1, $2, "
            + "$3, $4, $5, $6, $7)])\n"
            + "                LogicalTableScan(table=[[scott, EMP]])\n"
            + "          LogicalProject(X=[$1])\n"
            + "            LogicalAggregate(group=[{0}], X=[COLLECT($1)])\n"
            + "              LogicalProject($f0=['all'], $f1=[ROW($0, $1, $2, $3)])\n"
            + "                LogicalSort(sort0=[$3], dir0=[ASC])\n"
            + "                  LogicalProject(ENAME=[$1], JOB=[$2], "
            + "DEPTNO=[$7], SAL=[$5])\n"
            + "                    LogicalFilter(condition=[<>($2, 'CLERK')])\n"
            + "                      Uncollect\n"
            + "                        LogicalProject($f0=[$cor0.A])\n"
            + "                          LogicalValues(tuples=[[{ 0 }]])\n"
            + "      Uncollect\n"
            + "        LogicalProject($f0=[$cor1.X])\n"
            + "          LogicalValues(tuples=[[{ 0 }]])\n";

    String expectedResult = ""
                                + "(10,2,CLARK,MANAGER,10,2450.00,5000.00)\n"
                                + "(10,2,KING,PRESIDENT,10,5000.00,5000.00)\n"
                                + "(20,3,JONES,MANAGER,20,2975.00,3000.00)\n"
                                + "(20,3,SCOTT,ANALYST,20,3000.00,3000.00)\n"
                                + "(20,3,FORD,ANALYST,20,3000.00,3000.00)\n"
                                + "(30,5,WARD,SALESMAN,30,1250.00,2850.00)\n"
                                + "(30,5,MARTIN,SALESMAN,30,1250.00,2850.00)\n"
                                + "(30,5,TURNER,SALESMAN,30,1500.00,2850.00)\n"
                                + "(30,5,ALLEN,SALESMAN,30,1600.00,2850.00)\n"
                                + "(30,5,BLAKE,MANAGER,30,2850.00,2850.00)\n";
    testPigRelOpTranslation(script, expectedPlan);
    testRunPigScript(script, expectedResult);

    String expectedSql =
        ""
            + "SELECT $cor5.group, $cor5.cnt, $cor5.ENAME, $cor5.JOB, $cor5.DEPTNO, $cor5.SAL, "
            + "$cor5.$f3\n"
            + "FROM (SELECT $cor4.DEPTNO AS group, COUNT(PIG_BAG($cor4.X)) AS cnt, $cor4.X, "
            + "BigDecimalMax(PIG_BAG(MULTISET_PROJECTION($cor4.X, 3))) AS $f3\n"
            + "      FROM (SELECT DEPTNO, COLLECT(ROW(EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, "
            + "COMM, DEPTNO)) AS A\n"
            + "            FROM scott.EMP\n"
            + "            GROUP BY DEPTNO) AS $cor4,\n"
            + "          LATERAL (SELECT COLLECT(ROW(ENAME, JOB, DEPTNO, SAL)) AS X\n"
            + "            FROM (SELECT ENAME, JOB, DEPTNO, SAL\n"
            + "                  FROM UNNEST (SELECT $cor4.A AS $f0\n"
            + "                        FROM (VALUES  (0)) AS t (ZERO)) AS t2 (EMPNO, ENAME, JOB, "
            + "MGR, HIREDATE, SAL, COMM, DEPTNO)\n"
            + "                  WHERE JOB <> 'CLERK'\n"
            + "                  ORDER BY SAL) AS t5\n"
            + "            GROUP BY 'all') AS t8) AS $cor5,\n"
            + "    LATERAL UNNEST (SELECT $cor5.X AS $f0\n"
            + "        FROM (VALUES  (0)) AS t (ZERO)) AS t11 (ENAME, JOB, DEPTNO, SAL) AS t110\n"
            + "ORDER BY $cor5.group";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testUnionSameSchema() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                        + "MGR:int, "
                        + "    HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                        + "B = FILTER A BY DEPTNO == 10;\n"
                        + "C = FILTER A BY DEPTNO == 20;\n"
                        + "D = UNION B, C;\n";
    String expectedPlan = ""
                              + "LogicalUnion(all=[true])\n"
                              + "  LogicalFilter(condition=[=($7, 10)])\n"
                              + "    LogicalTableScan(table=[[scott, EMP]])\n"
                              + "  LogicalFilter(condition=[=($7, 20)])\n"
                              + "    LogicalTableScan(table=[[scott, EMP]])\n";

    String expectedResult = ""
                                + "(7782,CLARK,MANAGER,7839,1981-06-09,2450.00,null,10)\n"
                                + "(7839,KING,PRESIDENT,null,1981-11-17,5000.00,null,10)\n"
                                + "(7934,MILLER,CLERK,7782,1982-01-23,1300.00,null,10)\n"
                                + "(7369,SMITH,CLERK,7902,1980-12-17,800.00,null,20)\n"
                                + "(7566,JONES,MANAGER,7839,1981-02-04,2975.00,null,20)\n"
                                + "(7788,SCOTT,ANALYST,7566,1987-04-19,3000.00,null,20)\n"
                                + "(7876,ADAMS,CLERK,7788,1987-05-23,1100.00,null,20)\n"
                                + "(7902,FORD,ANALYST,7566,1981-12-03,3000.00,null,20)\n";
    testPigRelOpTranslation(script, expectedPlan);
    testRunPigScript(script, expectedResult);

    String expectedSql = ""
                             + "SELECT *\n"
                             + "  FROM scott.EMP\n"
                             + "  WHERE DEPTNO = 10\n"
                             + "UNION ALL\n"
                             + "  SELECT *\n"
                             + "  FROM scott.EMP\n"
                             + "  WHERE DEPTNO = 20";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testUnionDifferentSchemas1() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
                        + "B = FOREACH A GENERATE DEPTNO, DNAME;\n"
                        + "C = UNION ONSCHEMA A, B;\n";
    String expectedPlan = ""
                              + "LogicalUnion(all=[true])\n"
                              + "  LogicalTableScan(table=[[scott, DEPT]])\n"
                              + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[null:VARCHAR])\n"
                              + "    LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
                              + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    String expectedOptimizedPlan = ""
                                       + "LogicalUnion(all=[true])\n"
                                       + "  LogicalTableScan(table=[[scott, DEPT]])\n"
                                       + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], "
                                       + "LOC=[null:VARCHAR])\n"
                                       + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    testPigRelOpTranslation(script, expectedPlan, expectedOptimizedPlan);

    String expectedResult = ""
                                + "(10,ACCOUNTING,NEW YORK)\n"
                                + "(20,RESEARCH,DALLAS)\n"
                                + "(30,SALES,CHICAGO)\n"
                                + "(40,OPERATIONS,BOSTON)\n"
                                + "(10,ACCOUNTING,null)\n"
                                + "(20,RESEARCH,null)\n"
                                + "(30,SALES,null)\n"
                                + "(40,OPERATIONS,null)\n";
    testRunPigScript(script, expectedResult);

    String expectedSql = ""
                             + "SELECT *\n"
                             + "  FROM scott.DEPT\n"
                             + "UNION ALL\n"
                             + "  SELECT DEPTNO, DNAME, NULL AS LOC\n"
                             + "  FROM scott.DEPT";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testUnionDifferentSchemas2() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                        + "MGR:int, "
                        + "HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                        + "B = FILTER A BY DEPTNO == 10;\n"
                        + "C = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
                        + "D = UNION ONSCHEMA B, C;\n";
    String expectedPlan = ""
                              + "LogicalUnion(all=[true])\n"
                              + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], "
                              + "HIREDATE=[$4], "
                              + "SAL=[$5], COMM=[$6], DEPTNO=[$7], DNAME=[null:VARCHAR], "
                              + "LOC=[null:VARCHAR])\n"
                              + "    LogicalFilter(condition=[=($7, 10)])\n"
                              + "      LogicalTableScan(table=[[scott, EMP]])\n"
                              + "  LogicalProject(EMPNO=[null:INTEGER], ENAME=[null:VARCHAR], "
                              + "JOB=[null:VARCHAR], "
                              + "MGR=[null:INTEGER], HIREDATE=[null:DATE], SAL=[null:DECIMAL(19, "
                              + "0)], "
                              + "COMM=[null:DECIMAL(19, 0)], DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
                              + "    LogicalTableScan(table=[[scott, DEPT]])\n";


    String expectedResult = ""
                                + "(7782,CLARK,MANAGER,7839,1981-06-09,2450.00,null,10,null,null)\n"
                                + "(7839,KING,PRESIDENT,null,1981-11-17,5000.00,null,10,null,"
                                + "null)\n"
                                + "(7934,MILLER,CLERK,7782,1982-01-23,1300.00,null,10,null,null)\n"
                                + "(null,null,null,null,null,null,null,10,ACCOUNTING,NEW YORK)\n"
                                + "(null,null,null,null,null,null,null,20,RESEARCH,DALLAS)\n"
                                + "(null,null,null,null,null,null,null,30,SALES,CHICAGO)\n"
                                + "(null,null,null,null,null,null,null,40,OPERATIONS,BOSTON)\n";
    testPigRelOpTranslation(script, expectedPlan);
    testRunPigScript(script, expectedResult);

    String expectedSql = ""
                             + "SELECT EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO, NULL "
                             + "AS DNAME, NULL AS "
                             + "LOC\n"
                             + "  FROM scott.EMP\n"
                             + "  WHERE DEPTNO = 10\n"
                             + "UNION ALL\n"
                             + "  SELECT NULL AS EMPNO, NULL AS ENAME, NULL AS JOB, NULL AS MGR, "
                             + "NULL AS HIREDATE, "
                             + "NULL AS SAL, NULL AS COMM, DEPTNO, DNAME, LOC\n"
                             + "  FROM scott.DEPT";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testJoin2Rels() throws IOException {
    String scans = ""
                       + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                       + "MGR:int, "
                       + "HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                       + "B = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n";
    String expectedScans = ""
                               + "  LogicalTableScan(table=[[scott, EMP]])\n"
                               + "  LogicalTableScan(table=[[scott, DEPT]])\n";

    String inner = scans + "C = JOIN A BY DEPTNO, B BY DEPTNO;\n";
    testPigRelOpTranslation(inner, "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
                                       + expectedScans);
    String innerSQL = ""
                          + "SELECT *\n"
                          + "FROM scott.EMP\n"
                          + "  INNER JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO";
    testSQLTranslation(inner, innerSQL);

    String leftOuter = scans + "C = JOIN A BY DEPTNO LEFT OUTER, B BY DEPTNO;\n";
    testPigRelOpTranslation(leftOuter, "LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
                                           + expectedScans);
    String leftSQL = ""
                         + "SELECT *\n"
                         + "FROM scott.EMP\n"
                         + "  LEFT JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO";
    testSQLTranslation(leftOuter, leftSQL);

    String rightOuter = scans + "C = JOIN A BY DEPTNO RIGHT OUTER, B BY DEPTNO;\n";
    testPigRelOpTranslation(rightOuter, "LogicalJoin(condition=[=($7, $8)], joinType=[right])\n"
                                            + expectedScans);
    String rightSQL = ""
                          + "SELECT *\n"
                          + "FROM scott.EMP\n"
                          + "  RIGHT JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO";
    testSQLTranslation(rightOuter, rightSQL);

    String full = scans + "C = JOIN A BY DEPTNO FULL, B BY DEPTNO;\n";
    testPigRelOpTranslation(full, "LogicalJoin(condition=[=($7, $8)], joinType=[full])\n"
                                      + expectedScans);
    String fullSQL = ""
                         + "SELECT *\n"
                         + "FROM scott.EMP\n"
                         + "  FULL JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO";
    testSQLTranslation(full, fullSQL);

    String expectedFullJoin = ""
                                  + "(7369,SMITH,CLERK,7902,1980-12-17,800.00,null,20,20,"
                                  + "RESEARCH,DALLAS)\n"
                                  + "(7499,ALLEN,SALESMAN,7698,1981-02-20,1600.00,300.00,30,30,"
                                  + "SALES,CHICAGO)\n"
                                  + "(7521,WARD,SALESMAN,7698,1981-02-22,1250.00,500.00,30,30,"
                                  + "SALES,CHICAGO)\n"
                                  + "(7566,JONES,MANAGER,7839,1981-02-04,2975.00,null,20,20,"
                                  + "RESEARCH,DALLAS)\n"
                                  + "(7654,MARTIN,SALESMAN,7698,1981-09-28,1250.00,1400.00,30,30,"
                                  + "SALES,CHICAGO)\n"
                                  + "(7698,BLAKE,MANAGER,7839,1981-01-05,2850.00,null,30,30,"
                                  + "SALES,CHICAGO)\n"
                                  + "(7782,CLARK,MANAGER,7839,1981-06-09,2450.00,null,10,10,"
                                  + "ACCOUNTING,NEW YORK)\n"
                                  + "(7788,SCOTT,ANALYST,7566,1987-04-19,3000.00,null,20,20,"
                                  + "RESEARCH,DALLAS)\n"
                                  + "(7839,KING,PRESIDENT,null,1981-11-17,5000.00,null,10,10,"
                                  + "ACCOUNTING,NEW YORK)\n"
                                  + "(7844,TURNER,SALESMAN,7698,1981-09-08,1500.00,0.00,30,30,"
                                  + "SALES,CHICAGO)\n"
                                  + "(7876,ADAMS,CLERK,7788,1987-05-23,1100.00,null,20,20,"
                                  + "RESEARCH,DALLAS)\n"
                                  + "(7900,JAMES,CLERK,7698,1981-12-03,950.00,null,30,30,SALES,"
                                  + "CHICAGO)\n"
                                  + "(7902,FORD,ANALYST,7566,1981-12-03,3000.00,null,20,20,"
                                  + "RESEARCH,DALLAS)\n"
                                  + "(7934,MILLER,CLERK,7782,1982-01-23,1300.00,null,10,10,"
                                  + "ACCOUNTING,NEW YORK)\n"
                                  + "(null,null,null,null,null,null,null,null,40,OPERATIONS,"
                                  + "BOSTON)\n";
    testRunPigScript(scans + "C = JOIN A BY DEPTNO FULL, B BY DEPTNO;\n",
        expectedFullJoin);
  }

  @Test
  public void testJoin3Rels() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                        + "MGR:int, "
                        + "    HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                        + "B = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
                        + "C = FILTER B BY LOC == 'CHICAGO';\n"
                        + "D = JOIN A BY DEPTNO, B BY DEPTNO, C BY DEPTNO;\n";
    String expectedPlan = ""
                              + "LogicalJoin(condition=[=($7, $11)], joinType=[inner])\n"
                              + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
                              + "    LogicalTableScan(table=[[scott, EMP]])\n"
                              + "    LogicalTableScan(table=[[scott, DEPT]])\n"
                              + "  LogicalFilter(condition=[=($2, 'CHICAGO')])\n"
                              + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    testPigRelOpTranslation(script, expectedPlan);
    String expectedSql = ""
                             + "SELECT *\n"
                             + "FROM scott.EMP\n"
                             + "  INNER JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO\n"
                             + "  INNER JOIN (SELECT *\n"
                             + "      FROM scott.DEPT\n"
                             + "      WHERE LOC = 'CHICAGO') AS t ON EMP.DEPTNO = t.DEPTNO";

    testSQLTranslation(script, expectedSql);

    String expectedResult = ""
                                + "(7499,ALLEN,SALESMAN,7698,1981-02-20,1600.00,300.00,30,30,"
                                + "SALES,CHICAGO,30,SALES,"
                                + "CHICAGO)\n"
                                + "(7521,WARD,SALESMAN,7698,1981-02-22,1250.00,500.00,30,30,"
                                + "SALES,CHICAGO,30,SALES,"
                                + "CHICAGO)\n"
                                + "(7654,MARTIN,SALESMAN,7698,1981-09-28,1250.00,1400.00,30,30,"
                                + "SALES,CHICAGO,30,"
                                + "SALES,CHICAGO)\n"
                                + "(7698,BLAKE,MANAGER,7839,1981-01-05,2850.00,null,30,30,SALES,"
                                + "CHICAGO,30,SALES,"
                                + "CHICAGO)\n"
                                + "(7844,TURNER,SALESMAN,7698,1981-09-08,1500.00,0.00,30,30,"
                                + "SALES,CHICAGO,30,SALES,"
                                + "CHICAGO)\n"
                                + "(7900,JAMES,CLERK,7698,1981-12-03,950.00,null,30,30,SALES,"
                                + "CHICAGO,30,SALES,"
                                + "CHICAGO)\n";
    testRunPigScript(script, expectedResult);

    String script2 = ""
                         + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                         + "MGR:int, "
                         + "HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                         + "B = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY)"
                         + ";\n"
                         + "C = FILTER B BY LOC == 'CHICAGO';\n"
                         + "D = JOIN A BY (DEPTNO, ENAME), B BY (DEPTNO, DNAME), C BY (DEPTNO, "
                         + "DNAME);\n";
    String expectedPlan2 = ""
                               + "LogicalJoin(condition=[AND(=($7, $11), =($9, $12))], "
                               + "joinType=[inner])\n"
                               + "  LogicalJoin(condition=[AND(=($7, $8), =($1, $9))], "
                               + "joinType=[inner])\n"
                               + "    LogicalTableScan(table=[[scott, EMP]])\n"
                               + "    LogicalTableScan(table=[[scott, DEPT]])\n"
                               + "  LogicalFilter(condition=[=($2, 'CHICAGO')])\n"
                               + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    testPigRelOpTranslation(script2, expectedPlan2);

    String expectedSql2 = ""
                              + "SELECT *\n"
                              + "FROM scott.EMP\n"
                              + "  INNER JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO AND EMP"
                              + ".ENAME = DEPT.DNAME\n"
                              + "  INNER JOIN (SELECT *\n"
                              + "      FROM scott.DEPT\n"
                              + "      WHERE LOC = 'CHICAGO') AS t ON EMP.DEPTNO = t.DEPTNO AND "
                              + "DEPT.DNAME = t.DNAME";
    testSQLTranslation(script2, expectedSql2);
  }

  @Test
  public void testCross() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
                        + "B = FOREACH A GENERATE DEPTNO; \n"
                        + "C = FILTER B BY DEPTNO <= 20;\n"
                        + "D = CROSS B, C;\n";
    String expectedPlan = ""
                              + "LogicalJoin(condition=[true], joinType=[inner])\n"
                              + "  LogicalProject(DEPTNO=[$0])\n"
                              + "    LogicalTableScan(table=[[scott, DEPT]])\n"
                              + "  LogicalFilter(condition=[<=($0, 20)])\n"
                              + "    LogicalProject(DEPTNO=[$0])\n"
                              + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    testPigRelOpTranslation(script, expectedPlan);

    String expectedSql = ""
                             + "SELECT *\n"
                             + "FROM (SELECT DEPTNO\n"
                             + "      FROM scott.DEPT) AS t,\n"
                             + "      (SELECT DEPTNO\n"
                             + "      FROM scott.DEPT\n"
                             + "      WHERE DEPTNO <= 20) AS t1";
    testSQLTranslation(script, expectedSql);


    String expectedResult = ""
                                + "(10,10)\n"
                                + "(10,20)\n"
                                + "(20,10)\n"
                                + "(20,20)\n"
                                + "(30,10)\n"
                                + "(30,20)\n"
                                + "(40,10)\n"
                                + "(40,20)\n";
    testRunPigScript(script, expectedResult);

    String script2 = ""
                         + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY)"
                         + ";\n"
                         + "B = FOREACH A GENERATE DEPTNO; \n"
                         + "C = FILTER B BY DEPTNO <= 20;\n"
                         + "D = FILTER B BY DEPTNO > 20;\n"
                         + "E = CROSS B, C, D;\n";

    String expectedPlan2 = ""
                               + "LogicalJoin(condition=[true], joinType=[inner])\n"
                               + "  LogicalJoin(condition=[true], joinType=[inner])\n"
                               + "    LogicalProject(DEPTNO=[$0])\n"
                               + "      LogicalTableScan(table=[[scott, DEPT]])\n"
                               + "    LogicalFilter(condition=[<=($0, 20)])\n"
                               + "      LogicalProject(DEPTNO=[$0])\n"
                               + "        LogicalTableScan(table=[[scott, DEPT]])\n"
                               + "  LogicalFilter(condition=[>($0, 20)])\n"
                               + "    LogicalProject(DEPTNO=[$0])\n"
                               + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    testPigRelOpTranslation(script2, expectedPlan2);

    String expectedResult2 = ""
                                 + "(10,10,30)\n"
                                 + "(10,10,40)\n"
                                 + "(10,20,30)\n"
                                 + "(10,20,40)\n"
                                 + "(20,10,30)\n"
                                 + "(20,10,40)\n"
                                 + "(20,20,30)\n"
                                 + "(20,20,40)\n"
                                 + "(30,10,30)\n"
                                 + "(30,10,40)\n"
                                 + "(30,20,30)\n"
                                 + "(30,20,40)\n"
                                 + "(40,10,30)\n"
                                 + "(40,10,40)\n"
                                 + "(40,20,30)\n"
                                 + "(40,20,40)\n";
    testRunPigScript(script2, expectedResult2);
    String expectedSql2 = ""
                              + "SELECT *\n"
                              + "FROM (SELECT DEPTNO\n"
                              + "      FROM scott.DEPT) AS t,\n"
                              + "      (SELECT DEPTNO\n"
                              + "      FROM scott.DEPT\n"
                              + "      WHERE DEPTNO <= 20) AS t1,\n"
                              + "      (SELECT DEPTNO\n"
                              + "      FROM scott.DEPT\n"
                              + "      WHERE DEPTNO > 20) AS t3";

    testSQLTranslation(script2, expectedSql2);
  }

  @Test
  public void testGroupby() throws IOException {
    String base = "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n";
    String expectedBasePlan = "      LogicalTableScan(table=[[scott, DEPT]])\n";
    String script = base + "B = GROUP A BY DEPTNO;\n";
    String expectedPlan = ""
                              + "LogicalProject(group=[$0], A=[$1])\n"
                              + "  LogicalAggregate(group=[{0}], A=[COLLECT($1)])\n"
                              + "    LogicalProject(DEPTNO=[$0], $f1=[ROW($0, $1, $2)])\n"
                              + expectedBasePlan;
    String expectedResult = ""
                                + "(20,{(20,RESEARCH,DALLAS)})\n"
                                + "(40,{(40,OPERATIONS,BOSTON)})\n"
                                + "(10,{(10,ACCOUNTING,NEW YORK)})\n"
                                + "(30,{(30,SALES,CHICAGO)})\n";
    testPigRelOpTranslation(script, expectedPlan);
    testRunPigScript(script, expectedResult);

    String expectedSql = ""
                             + "SELECT DEPTNO, COLLECT(ROW(DEPTNO, DNAME, LOC)) AS A\n"
                             + "FROM scott.DEPT\n"
                             + "GROUP BY DEPTNO";
    testSQLTranslation(script, expectedSql);

    script = base + "B = GROUP A ALL;\n";
    expectedPlan = ""
                       + "LogicalProject(group=[$0], A=[$1])\n"
                       + "  LogicalAggregate(group=[{0}], A=[COLLECT($1)])\n"
                       + "    LogicalProject($f0=['all'], $f1=[ROW($0, $1, $2)])\n"
                       + expectedBasePlan;
    expectedResult =
        "(all,{(10,ACCOUNTING,NEW YORK),(20,RESEARCH,DALLAS),(30,SALES,CHICAGO),(40,OPERATIONS,"
            + "BOSTON)})"
            + "\n";
    testRunPigScript(script, expectedResult);
    testPigRelOpTranslation(script, expectedPlan);
  }

  @Test
  public void testGroupby2() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                        + "MGR:int, "
                        + "HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                        + "B = FOREACH A GENERATE EMPNO, ENAME, JOB, MGR, SAL, COMM, DEPTNO;\n"
                        + "C = GROUP B BY (DEPTNO, JOB);\n";
    String expectedPlan = ""
                              + "LogicalProject(group=[ROW($0, $1)], B=[$2])\n"
                              + "  LogicalAggregate(group=[{0, 1}], B=[COLLECT($2)])\n"
                              + "    LogicalProject(DEPTNO=[$6], JOB=[$2], $f2=[ROW($0, $1, $2, "
                              + "$3, $4, $5, $6)])\n"
                              + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
                              + " SAL=[$5], "
                              + "COMM=[$6], DEPTNO=[$7])\n"
                              + "        LogicalTableScan(table=[[scott, EMP]])\n";

    testPigRelOpTranslation(script, expectedPlan);

    String expectedResult = ""
                                + "({10, MANAGER},{(7782,CLARK,MANAGER,7839,2450.00,null,10)})\n"
                                + "({10, PRESIDENT},{(7839,KING,PRESIDENT,null,5000.00,null,10)})\n"
                                + "({20, CLERK},{(7369,SMITH,CLERK,7902,800.00,null,20),(7876,"
                                + "ADAMS,CLERK,7788,1100"
                                + ".00,null,20)})\n"
                                + "({30, MANAGER},{(7698,BLAKE,MANAGER,7839,2850.00,null,30)})\n"
                                + "({20, ANALYST},{(7788,SCOTT,ANALYST,7566,3000.00,null,20),"
                                + "(7902,FORD,ANALYST,7566,"
                                + "3000.00,null,20)})\n"
                                + "({30, SALESMAN},{(7499,ALLEN,SALESMAN,7698,1600.00,300.00,30),"
                                + "(7521,WARD,SALESMAN,"
                                + "7698,1250.00,500.00,30),(7654,MARTIN,SALESMAN,7698,1250.00,"
                                + "1400.00,30),(7844,"
                                + "TURNER,SALESMAN,7698,1500.00,0.00,30)})\n"
                                + "({30, CLERK},{(7900,JAMES,CLERK,7698,950.00,null,30)})\n"
                                + "({20, MANAGER},{(7566,JONES,MANAGER,7839,2975.00,null,20)})\n"
                                + "({10, CLERK},{(7934,MILLER,CLERK,7782,1300.00,null,10)})\n";

    testRunPigScript(script, expectedResult);
  }

  @Test
  public void testCubeCube() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                        + "MGR:int, "
                        + "    HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                        + "B = CUBE A BY CUBE(DEPTNO, JOB);\n"
                        + "C = FOREACH B GENERATE group, COUNT(cube.ENAME);\n";
    String expectedPlan = ""
                              + "LogicalProject(group=[$0], $f1=[COUNT(PIG_BAG"
                              + "(MULTISET_PROJECTION($1, 3)))])\n"
                              + "  LogicalProject(group=[ROW($0, $1)], cube=[$2])\n"
                              + "    LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, {0}, {1}, "
                              + "{}]], cube=[COLLECT"
                              + "($2)])\n"
                              + "      LogicalProject(DEPTNO=[$7], JOB=[$2], $f2=[ROW($7, $2, $0,"
                              + " $1, $3, $4, $5, $6)"
                              + "])\n"
                              + "        LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedOptimizedPlan = ""
                                       + "LogicalProject(group=[ROW($0, $1)], $f1=[CAST($2)"
                                       + ":BIGINT])\n"
                                       + "  LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, "
                                       + "{0}, {1}, {}]], "
                                       + "agg#0=[COUNT($2)])\n"
                                       + "    LogicalProject(DEPTNO=[$7], JOB=[$2], ENAME=[$1])\n"
                                       + "      LogicalTableScan(table=[[scott, EMP]])\n";
    testPigRelOpTranslation(script, expectedPlan, expectedOptimizedPlan);

    String expectedResult = ""
                                + "({30, SALESMAN},4)\n"
                                + "({30, null},6)\n"
                                + "({10, null},3)\n"
                                + "({null, PRESIDENT},1)\n"
                                + "({30, MANAGER},1)\n"
                                + "({20, MANAGER},1)\n"
                                + "({20, ANALYST},2)\n"
                                + "({10, MANAGER},1)\n"
                                + "({null, CLERK},4)\n"
                                + "({null, null},14)\n"
                                + "({20, null},5)\n"
                                + "({10, PRESIDENT},1)\n"
                                + "({null, ANALYST},2)\n"
                                + "({null, SALESMAN},4)\n"
                                + "({30, CLERK},1)\n"
                                + "({10, CLERK},1)\n"
                                + "({20, CLERK},2)\n"
                                + "({null, MANAGER},3)\n";

    testRunPigScript(script, expectedResult);

    String expectedSql = ""
                             + "SELECT ROW(DEPTNO, JOB) AS group, CAST(COUNT(ENAME) AS BIGINT) AS"
                             + " $f1\n"
                             + "FROM scott.EMP\n"
                             + "GROUP BY CUBE(DEPTNO, JOB)";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testCubeRollup() throws IOException {
    String script =
        ""
            + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, MGR:int, "
            + "    HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
            + "B = CUBE A BY ROLLUP(DEPTNO, JOB);\n"
            + "C = FOREACH B GENERATE group, COUNT(cube.ENAME);\n";
    String expectedPlan =
        ""
            + "LogicalProject(group=[$0], $f1=[COUNT(PIG_BAG"
            + "(MULTISET_PROJECTION($1, 3)))])\n"
            + "  LogicalProject(group=[ROW($0, $1)], cube=[$2])\n"
            + "    LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, {1}, {}]],"
            + " cube=[COLLECT($2)])\n"
            + "      LogicalProject(DEPTNO=[$7], JOB=[$2], $f2=[ROW($7, $2, $0,"
            + " $1, $3, $4, $5, $6)"
            + "])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedOptimizedPlan =
        ""
            + "LogicalProject(group=[ROW($0, $1)], $f1=[CAST($2)"
            + ":BIGINT])\n"
            + "  LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, "
            + "{1}, {}]], agg#0=[COUNT"
            + "($2)])\n"
            + "    LogicalProject(DEPTNO=[$7], JOB=[$2], ENAME=[$1])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    testPigRelOpTranslation(script, expectedPlan, expectedOptimizedPlan);

    String expectedResult = ""
                                + "({30, SALESMAN},4)\n"
                                + "({null, PRESIDENT},1)\n"
                                + "({30, MANAGER},1)\n"
                                + "({20, MANAGER},1)\n"
                                + "({20, ANALYST},2)\n"
                                + "({10, MANAGER},1)\n"
                                + "({null, CLERK},4)\n"
                                + "({null, null},14)\n"
                                + "({10, PRESIDENT},1)\n"
                                + "({null, ANALYST},2)\n"
                                + "({null, SALESMAN},4)\n"
                                + "({30, CLERK},1)\n"
                                + "({10, CLERK},1)\n"
                                + "({20, CLERK},2)\n"
                                + "({null, MANAGER},3)\n";
    testRunPigScript(script, expectedResult);
    String expectedSql = ""
                             + "SELECT ROW(DEPTNO, JOB) AS group, CAST(COUNT(ENAME) AS BIGINT) AS"
                             + " $f1\n"
                             + "FROM scott.EMP\n"
                             + "GROUP BY ROLLUP(DEPTNO, JOB)";
    testSQLTranslation(script, expectedSql);
  }


  @Test
  public void testMultisetProjection() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
                        + "B = GROUP A BY DEPTNO;\n"
                        + "C = FOREACH B GENERATE A.(DEPTNO, DNAME);\n";
    String expectedPlan = ""
                              + "LogicalProject($f0=[MULTISET_PROJECTION($1, 0, 1)])\n"
                              + "  LogicalProject(group=[$0], A=[$1])\n"
                              + "    LogicalAggregate(group=[{0}], A=[COLLECT($1)])\n"
                              + "      LogicalProject(DEPTNO=[$0], $f1=[ROW($0, $1, $2)])\n"
                              + "        LogicalTableScan(table=[[scott, DEPT]])\n";
    // CALCITE-3297: PigToSqlAggregateRule should be applied to produce the following optimized plan
//    String expectedOptimizedPlan =
//        ""
//            + "LogicalProject($f0=[$1])\n"
//            + "  LogicalAggregate(group=[{0}], agg#0=[COLLECT($2)])\n"
//            + "    LogicalProject(DEPTNO=[$0], DNAME=[$1], $f2=[ROW($0, $1)])\n"
//            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    testPigRelOpTranslation(script, expectedPlan, expectedPlan);

    String expectedResult = ""
                                + "({(20,RESEARCH)})\n"
                                + "({(40,OPERATIONS)})\n"
                                + "({(10,ACCOUNTING)})\n"
                                + "({(30,SALES)})\n";
    testRunPigScript(script, expectedResult);
    String expectedSql =
        ""
            + "SELECT MULTISET_PROJECTION(COLLECT(ROW(DEPTNO, DNAME, LOC)), 0, 1) AS $f0\n"
            + "FROM scott.DEPT\n"
            + "GROUP BY DEPTNO";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testOrderBy() throws IOException {
    String scan = "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n";
    String expectedScan = "  LogicalTableScan(table=[[scott, DEPT]])\n";

    testPigRelOpTranslation(scan + "B = ORDER A BY DNAME;\n",
        "LogicalSort(sort0=[$1], dir0=[ASC])\n"
            + expectedScan);
    testSQLTranslation(scan
                           + "B = ORDER A BY DNAME;\n",
        "SELECT *\n"
            + "FROM scott.DEPT\n"
            + "ORDER BY DNAME");

    testPigRelOpTranslation(scan + "B = ORDER A BY DNAME DESC;\n",
        "LogicalSort(sort0=[$1], dir0=[DESC])\n"
            + expectedScan);
    testSQLTranslation(scan + "B = ORDER A BY DNAME DESC;\n",
        "SELECT *\n"
            + "FROM scott.DEPT\n"
            + "ORDER BY DNAME DESC");

    testPigRelOpTranslation(scan + "B = ORDER A BY LOC DESC, DEPTNO;\n",
        "LogicalSort(sort0=[$2], sort1=[$0], dir0=[DESC], dir1=[ASC])\n"
            + expectedScan);
    testSQLTranslation(scan + "B = ORDER A BY LOC DESC, DEPTNO;\n",
        "SELECT *\n"
            + "FROM scott.DEPT\n"
            + "ORDER BY LOC DESC, DEPTNO");

    testPigRelOpTranslation(scan + "B = ORDER A BY *;\n",
        "LogicalSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC])\n"
            + expectedScan);
    testSQLTranslation(scan + "B = ORDER A BY *;\n",
        "SELECT *\n"
            + "FROM scott.DEPT\n"
            + "ORDER BY DEPTNO, DNAME, LOC");

    testPigRelOpTranslation(scan + "B = ORDER A BY * DESC;\n",
        "LogicalSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[DESC], dir1=[DESC], dir2=[DESC])\n"
            + expectedScan);

    String expectedResult = ""
                                + "(10,ACCOUNTING,NEW YORK)\n"
                                + "(40,OPERATIONS,BOSTON)\n"
                                + "(20,RESEARCH,DALLAS)\n"
                                + "(30,SALES,CHICAGO)\n";
    testRunPigScript(scan + "B = ORDER A BY DNAME;\n", expectedResult);

    expectedResult = ""
                         + "(40,OPERATIONS,BOSTON)\n"
                         + "(30,SALES,CHICAGO)\n"
                         + "(20,RESEARCH,DALLAS)\n"
                         + "(10,ACCOUNTING,NEW YORK)\n";
    testRunPigScript(scan + "B = ORDER A BY * DESC;\n", expectedResult);
  }

  @Test
  public void testRank() throws IOException {
    String base = ""
                      + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                      + "MGR:int, "
                      + "HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                      + "B = FOREACH A GENERATE EMPNO, JOB, DEPTNO;\n";
    String expectedBase = ""
                              + "  LogicalProject(EMPNO=[$0], JOB=[$2], DEPTNO=[$7])\n"
                              + "    LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedOptimizedRank = ""
                                       + "LogicalProject(rank_C=[$3], EMPNO=[$0], JOB=[$1], "
                                       + "DEPTNO=[$2])\n"
                                       + "  LogicalWindow(window#0=[window(partition {} order by "
                                       + "[2, 1 DESC] range "
                                       + "between UNBOUNDED PRECEDING and CURRENT ROW aggs [RANK"
                                       + "()])])\n"
                                       + "    LogicalProject(EMPNO=[$0], JOB=[$2], DEPTNO=[$7])\n"
                                       + "      LogicalTableScan(table=[[scott, EMP]])\n";

    String script = base + "C = RANK B BY DEPTNO ASC, JOB DESC;\n";
    testPigRelOpTranslation(script,
        "LogicalProject(rank_C=[RANK() OVER (ORDER BY $2, $1 DESC RANGE BETWEEN UNBOUNDED "
            + "PRECEDING AND CURRENT ROW)"
            + "], EMPNO=[$0], JOB=[$1], DEPTNO=[$2])\n"
            + expectedBase, expectedOptimizedRank);

    String expectedResult = ""
                                + "(1,7839,PRESIDENT,10)\n"
                                + "(2,7782,MANAGER,10)\n"
                                + "(3,7934,CLERK,10)\n"
                                + "(4,7566,MANAGER,20)\n"
                                + "(5,7369,CLERK,20)\n"
                                + "(5,7876,CLERK,20)\n"
                                + "(7,7788,ANALYST,20)\n"
                                + "(7,7902,ANALYST,20)\n"
                                + "(9,7499,SALESMAN,30)\n"
                                + "(9,7521,SALESMAN,30)\n"
                                + "(9,7654,SALESMAN,30)\n"
                                + "(9,7844,SALESMAN,30)\n"
                                + "(13,7698,MANAGER,30)\n"
                                + "(14,7900,CLERK,30)\n";
    testRunPigScript(script, expectedResult);
    String expectedSql = ""
                             + "SELECT RANK() OVER (ORDER BY DEPTNO, JOB DESC RANGE BETWEEN "
                             + "UNBOUNDED PRECEDING AND "
                             + "CURRENT ROW) AS rank_C, EMPNO, JOB, DEPTNO\n"
                             + "FROM scott.EMP";
    testSQLTranslation(script, expectedSql);

    script = base + "C = RANK B BY DEPTNO ASC, JOB DESC DENSE;\n";
    String expectedOptimizedDesneRank = ""
                                            + "LogicalProject(rank_C=[$3], EMPNO=[$0], JOB=[$1], "
                                            + "DEPTNO=[$2])\n"
                                            + "  LogicalWindow(window#0=[window(partition {} "
                                            + "order by [2, 1 DESC] "
                                            + "range between UNBOUNDED PRECEDING and CURRENT ROW "
                                            + "aggs [DENSE_RANK()])"
                                            + "])\n"
                                            + "    LogicalProject(EMPNO=[$0], JOB=[$2], "
                                            + "DEPTNO=[$7])\n"
                                            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    testPigRelOpTranslation(script,
        "LogicalProject(rank_C=[DENSE_RANK() OVER (ORDER BY $2, $1 DESC RANGE BETWEEN UNBOUNDED "
            + "PRECEDING AND CURRENT"
            + " ROW)], EMPNO=[$0], JOB=[$1], DEPTNO=[$2])\n"
            + expectedBase, expectedOptimizedDesneRank);
    expectedResult = ""
                         + "(1,7839,PRESIDENT,10)\n"
                         + "(2,7782,MANAGER,10)\n"
                         + "(3,7934,CLERK,10)\n"
                         + "(4,7566,MANAGER,20)\n"
                         + "(5,7369,CLERK,20)\n"
                         + "(5,7876,CLERK,20)\n"
                         + "(6,7788,ANALYST,20)\n"
                         + "(6,7902,ANALYST,20)\n"
                         + "(7,7499,SALESMAN,30)\n"
                         + "(7,7521,SALESMAN,30)\n"
                         + "(7,7654,SALESMAN,30)\n"
                         + "(7,7844,SALESMAN,30)\n"
                         + "(8,7698,MANAGER,30)\n"
                         + "(9,7900,CLERK,30)\n";
    testRunPigScript(script, expectedResult);

    expectedSql = ""
                      + "SELECT DENSE_RANK() OVER (ORDER BY DEPTNO, JOB DESC RANGE BETWEEN "
                      + "UNBOUNDED PRECEDING AND "
                      + "CURRENT ROW) AS rank_C, EMPNO, JOB, DEPTNO\n"
                      + "FROM scott.EMP";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testLimit() throws IOException {
    String scan = "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n";
    String expectedScan = "  LogicalTableScan(table=[[scott, DEPT]])\n";

    testPigRelOpTranslation(scan
                                + "B = ORDER A BY DNAME;\n"
                                + "C = LIMIT B 2;\n",
        "LogicalSort(sort0=[$1], dir0=[ASC], fetch=[2])\n"
            + expectedScan);
    testSQLTranslation(scan
                           + "B = ORDER A BY DNAME;\n"
                           + "C = LIMIT B 2;\n",
        "SELECT *\n"
            + "FROM scott.DEPT\n"
            + "ORDER BY DNAME\n"
            + "FETCH NEXT 2 ROWS ONLY");

    testPigRelOpTranslation(scan
                                + "B = LIMIT A 2;\n",
        "LogicalSort(fetch=[2])\n"
            + expectedScan);
    testSQLTranslation(scan
                           + "B = LIMIT A 2;\n",
        "SELECT *\n"
            + "FROM scott.DEPT\n"
            + "FETCH NEXT 2 ROWS ONLY");

    String expectedResult = ""
                                + "(10,ACCOUNTING,NEW YORK)\n"
                                + "(40,OPERATIONS,BOSTON)\n";
    testRunPigScript(scan
                         + "B = ORDER A BY DNAME;\n"
                         + "C = LIMIT B 2;\n", expectedResult);
  }

  @Test
  public void testDistinct() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
                        + "MGR:int, "
                        + "HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                        + "B = FOREACH A GENERATE DEPTNO;\n"
                        + "C = DISTINCT B;\n";
    String expectedPlan = ""
                              + "LogicalAggregate(group=[{0}])\n"
                              + "  LogicalProject(DEPTNO=[$7])\n"
                              + "    LogicalTableScan(table=[[scott, EMP]])\n";

    String expectedResult = ""
                                + "(20)\n"
                                + "(10)\n"
                                + "(30)\n";
    testPigRelOpTranslation(script, expectedPlan);
    testRunPigScript(script, expectedResult);
    testSQLTranslation(script, "SELECT DEPTNO\n"
                                   + "FROM scott.EMP\n"
                                   + "GROUP BY DEPTNO");
  }

  @Test
  public void testAggregate() throws IOException {
    String script =
        ""
            + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, MGR:int, "
            + "    HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
            + "B = GROUP A BY DEPTNO;\n"
            + "C = FOREACH B GENERATE group, COUNT(A), BigDecimalSum(A.SAL);\n";
    String expectedPlan =
        ""
            + "LogicalProject(group=[$0], $f1=[COUNT(PIG_BAG($1))], "
            + "$f2=[BigDecimalSum(PIG_BAG(MULTISET_PROJECTION($1, 5)))])\n"
            + "  LogicalProject(group=[$0], A=[$1])\n"
            + "    LogicalAggregate(group=[{0}], A=[COLLECT($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], $f1=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedOptimizedPlan =
        ""
            + "LogicalProject(group=[$0], $f1=[CAST($1):BIGINT], $f2=[CAST($2):DECIMAL(19, 0)])\n"
            + "  LogicalAggregate(group=[{0}], agg#0=[COUNT()], agg#1=[SUM($1)])\n"
            + "    LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    testPigRelOpTranslation(script, expectedPlan, expectedOptimizedPlan);

    String expectedResult = ""
                                + "(20,5,10875.00)\n"
                                + "(10,3,8750.00)\n"
                                + "(30,6,9400.00)\n";
    testRunPigScript(script, expectedResult);

    String expectedSql =
        ""
            + "SELECT DEPTNO AS group, CAST(COUNT(*) AS BIGINT) AS $f1, CAST(SUM(SAL) AS "
            + "DECIMAL(19, 0)) AS $f2\n"
            + "FROM scott.EMP\n"
            + "GROUP BY DEPTNO";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testAggregate2() throws IOException {
    String script =
        ""
            + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, MGR:int, "
            + "HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
            + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
            + "C = FOREACH B GENERATE group, COUNT(A), SUM(A.SAL) as salSum;\n"
            + "D = ORDER C BY salSum;\n";
    String expectedPlan =
        ""
            + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
            + "  LogicalProject(group=[$0], $f1=[COUNT(PIG_BAG($1))], "
            + "salSum=[BigDecimalSum(PIG_BAG(MULTISET_PROJECTION($1, 5)))])\n"
            + "    LogicalProject(group=[ROW($0, $1, $2)], A=[$3])\n"
            + "      LogicalAggregate(group=[{0, 1, 2}], A=[COLLECT($3)])\n"
            + "        LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], "
            + "$f3=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedOptimizedPlan =
        ""
            + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
            + "  LogicalProject(group=[ROW($0, $1, $2)], $f1=[CAST($3):BIGINT], "
            + "salSum=[CAST($4):DECIMAL(19, 0)])\n"
            + "    LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT()], agg#1=[SUM($3)])\n"
            + "      LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    testPigRelOpTranslation(script, expectedPlan, expectedOptimizedPlan);

    String expectedResult = ""
                                + "({20, 7902, 1980-12-17},1,800.00)\n"
                                + "({30, 7698, 1981-12-03},1,950.00)\n"
                                + "({20, 7788, 1987-05-23},1,1100.00)\n"
                                + "({30, 7698, 1981-09-28},1,1250.00)\n"
                                + "({30, 7698, 1981-02-22},1,1250.00)\n"
                                + "({10, 7782, 1982-01-23},1,1300.00)\n"
                                + "({30, 7698, 1981-09-08},1,1500.00)\n"
                                + "({30, 7698, 1981-02-20},1,1600.00)\n"
                                + "({10, 7839, 1981-06-09},1,2450.00)\n"
                                + "({30, 7839, 1981-01-05},1,2850.00)\n"
                                + "({20, 7839, 1981-02-04},1,2975.00)\n"
                                + "({20, 7566, 1981-12-03},1,3000.00)\n"
                                + "({20, 7566, 1987-04-19},1,3000.00)\n"
                                + "({10, null, 1981-11-17},1,5000.00)\n";
    testRunPigScript(script, expectedResult);

    String expectedSql =
        ""
            + "SELECT ROW(DEPTNO, MGR, HIREDATE) AS group, CAST(COUNT(*) AS "
            + "BIGINT) AS $f1, CAST(SUM(SAL) AS DECIMAL(19, 0)) AS salSum\n"
            + "FROM scott.EMP\n"
            + "GROUP BY DEPTNO, MGR, HIREDATE\n"
            + "ORDER BY CAST(SUM(SAL) AS DECIMAL(19, 0))";
    testSQLTranslation(script, expectedSql);
  }


  @Test
  public void testAggregate2half() throws IOException {
    String script =
        ""
            + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
            + "MGR:int, HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
            + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
            + "C = FOREACH B GENERATE flatten(group) as (DEPTNO, MGR, HIREDATE), COUNT(A), "
            + "SUM(A.SAL) as salSum, MAX(A.DEPTNO) as maxDep, MIN(A.HIREDATE) as minHire;\n"
            + "D = ORDER C BY salSum;\n";
    String expectedPlan =
        ""
            + "LogicalSort(sort0=[$4], dir0=[ASC])\n"
            + "  LogicalProject(DEPTNO=[$0.DEPTNO], MGR=[$0.MGR], HIREDATE=[$0.HIREDATE], "
            + "$f3=[COUNT(PIG_BAG($1))], salSum=[BigDecimalSum(PIG_BAG(MULTISET_PROJECTION($1, 5)))]"
            + ", maxDep=[IntMax(PIG_BAG(MULTISET_PROJECTION($1, 7)))], minHire=[DateTimeMin(PIG_BAG"
            + "(MULTISET_PROJECTION($1, 4)))])\n"
            + "    LogicalProject(group=[ROW($0, $1, $2)], A=[$3])\n"
            + "      LogicalAggregate(group=[{0, 1, 2}], A=[COLLECT($3)])\n"
            + "        LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], "
            + "$f3=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";

    String expectedOptimizedPlan =
        ""
            + "LogicalSort(sort0=[$4], dir0=[ASC])\n"
            + "  LogicalProject(DEPTNO=[$0], MGR=[$1], HIREDATE=[$2], $f3=[CAST($3):BIGINT], "
            + "salSum=[CAST($4):DECIMAL(19, 0)], maxDep=[CAST($5):INTEGER], minHire=[$6])\n"
            + "    LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT()"
            + "], agg#1=[SUM($3)], agg#2=[MAX($0)], agg#3=[MIN($2)])\n"
            + "      LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    testPigRelOpTranslation(script, expectedPlan, expectedOptimizedPlan);

    String script2 =
        ""
            + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
            + "MGR:int, HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, "
            + "DEPTNO:int);\n"
            + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
            + "C = FOREACH B GENERATE group.DEPTNO, COUNT(A), SUM(A.SAL) as salSum, "
            + "group.MGR, MAX(A.DEPTNO) as maxDep, MIN(A.HIREDATE) as minHire;\n"
            + "D = ORDER C BY salSum;\n";
    String expectedPlan2 =
        ""
            + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
            + "  LogicalProject(DEPTNO=[$0.DEPTNO], $f1=[COUNT(PIG_BAG($1))], "
            + "salSum=[BigDecimalSum(PIG_BAG(MULTISET_PROJECTION($1, 5)))], "
            + "MGR=[$0.MGR], maxDep=[IntMax(PIG_BAG(MULTISET_PROJECTION($1, 7)"
            + "))], minHire=[DateTimeMin(PIG_BAG(MULTISET_PROJECTION($1, 4)))])\n"
            + "    LogicalProject(group=[ROW($0, $1, $2)], A=[$3])\n"
            + "      LogicalAggregate(group=[{0, 1, 2}], A=[COLLECT($3)])\n"
            + "        LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], "
            + "$f3=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";

    String expectedOptimizedPlan2 =
        ""
            + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
            + "  LogicalProject(DEPTNO=[$0], $f1=[CAST($3):BIGINT], salSum=[CAST($4):DECIMAL(19, 0)]"
            + ", MGR=[$1], maxDep=[CAST($5):INTEGER], minHire=[$6])\n"
            + "    LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT()], agg#1=[SUM($3)], "
            + "agg#2=[MAX($0)], agg#3=[MIN($2)])\n"
            + "      LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    testPigRelOpTranslation(script2, expectedPlan2, expectedOptimizedPlan2);
  }

  @Test
  public void testAggregate3() throws IOException {
    String script =
        ""
            + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
            + "MGR:int, HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
            + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
            + "C = FOREACH B GENERATE group, COUNT(A) + 1, BigDecimalSum(A.SAL) as "
            + "salSum, BigDecimalSum(A.SAL) / COUNT(A) as salAvg;\n"
            + "D = ORDER C BY salSum;\n";
    String expectedPlan =
        ""
            + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
            + "  LogicalProject(group=[$0], $f1=[+(COUNT(PIG_BAG($1)), 1)], "
            + "salSum=[BigDecimalSum(PIG_BAG(MULTISET_PROJECTION($1, 5)))], "
            + "salAvg=[/(BigDecimalSum(PIG_BAG(MULTISET_PROJECTION($1, 5))), "
            + "CAST(COUNT(PIG_BAG($1))):DECIMAL(19, 0))])\n"
            + "    LogicalProject(group=[ROW($0, $1, $2)], A=[$3])\n"
            + "      LogicalAggregate(group=[{0, 1, 2}], A=[COLLECT($3)])\n"
            + "        LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], "
            + "$f3=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedOptimizedPlan =
        ""
            + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
            + "  LogicalProject(group=[ROW($0, $1, $2)], $f1=[+(CAST($3):BIGINT, 1)], "
            + "salSum=[CAST($4):DECIMAL(19, 0)], salAvg=[/(CAST($4):DECIMAL(19, 0), CAST(CAST($3)"
            + ":BIGINT):DECIMAL(19, 0))])\n"
            + "    LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT()], agg#1=[SUM($3)])\n"
            + "      LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    testPigRelOpTranslation(script, expectedPlan, expectedOptimizedPlan);

    String expectedResult = ""
                                + "({20, 7902, 1980-12-17},2,800.00,800.00)\n"
                                + "({30, 7698, 1981-12-03},2,950.00,950.00)\n"
                                + "({20, 7788, 1987-05-23},2,1100.00,1100.00)\n"
                                + "({30, 7698, 1981-09-28},2,1250.00,1250.00)\n"
                                + "({30, 7698, 1981-02-22},2,1250.00,1250.00)\n"
                                + "({10, 7782, 1982-01-23},2,1300.00,1300.00)\n"
                                + "({30, 7698, 1981-09-08},2,1500.00,1500.00)\n"
                                + "({30, 7698, 1981-02-20},2,1600.00,1600.00)\n"
                                + "({10, 7839, 1981-06-09},2,2450.00,2450.00)\n"
                                + "({30, 7839, 1981-01-05},2,2850.00,2850.00)\n"
                                + "({20, 7839, 1981-02-04},2,2975.00,2975.00)\n"
                                + "({20, 7566, 1981-12-03},2,3000.00,3000.00)\n"
                                + "({20, 7566, 1987-04-19},2,3000.00,3000.00)\n"
                                + "({10, null, 1981-11-17},2,5000.00,5000.00)\n";
    testRunPigScript(script, expectedResult);
    String expectedSql = ""
                             + "SELECT ROW(DEPTNO, MGR, HIREDATE) AS group, CAST(COUNT(*) AS "
                             + "BIGINT) + 1 AS $f1, CAST(SUM(SAL) AS DECIMAL(19, 0)) AS salSum, "
                             + "CAST(SUM(SAL) AS DECIMAL(19, 0)) / CAST(CAST(COUNT(*) AS BIGINT) "
                             + "AS DECIMAL(19, 0)) AS salAvg\n"
                             + "FROM scott.EMP\n"
                             + "GROUP BY DEPTNO, MGR, HIREDATE\n"
                             + "ORDER BY CAST(SUM(SAL) AS DECIMAL(19, 0))";
    testSQLTranslation(script, expectedSql);

    script = ""
                 + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, MGR:int, "
                 + "HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                 + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
                 + "C = FOREACH B GENERATE group, COUNT(A) + 1, BigDecimalSum(A.SAL) as salSum, "
                 + "BigDecimalSum(A.SAL) / COUNT(A) as salAvg, A;\n"
                 + "D = ORDER C BY salSum;\n";
    expectedSql = ""
                      + "SELECT ROW(DEPTNO, MGR, HIREDATE) AS group, CAST(COUNT(*) AS BIGINT) + 1"
                      + " AS $f1, CAST(SUM(SAL) AS DECIMAL(19, 0)) AS salSum, CAST(SUM(SAL) AS "
                      + "DECIMAL(19, 0)) / CAST(CAST(COUNT(*) AS BIGINT) AS DECIMAL(19, 0)) AS "
                      + "salAvg, COLLECT(ROW(EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO)"
                      + ") AS A\n"
                      + "FROM scott.EMP\n"
                      + "GROUP BY DEPTNO, MGR, HIREDATE\n"
                      + "ORDER BY CAST(SUM(SAL) AS DECIMAL(19, 0))";
    testSQLTranslation(script, expectedSql);

    script = ""
                 + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, MGR:int, "
                 + "HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
                 + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
                 + "C = FOREACH B GENERATE group, A, COUNT(A);\n";
    expectedSql = ""
                      + "SELECT ROW(DEPTNO, MGR, HIREDATE) AS group, COLLECT(ROW(EMPNO, ENAME, "
                      + "JOB, MGR, HIREDATE, SAL, COMM, DEPTNO)) AS A, CAST(COUNT(*) AS BIGINT) "
                      + "AS $f2\n"
                      + "FROM scott.EMP\n"
                      + "GROUP BY DEPTNO, MGR, HIREDATE";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testAggregate4() throws IOException {
    String script =
        ""
            + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray, JOB:chararray, "
            + "MGR:int, HIREDATE:datetime, SAL:bigdecimal, COMM:bigdecimal, DEPTNO:int);\n"
            + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
            + "C = FOREACH B GENERATE FLATTEN(group) as (DEPTNO, MGR, HIREDATE), "
            + "COUNT(A), 1L as newCol, A.COMM as comArray, SUM(A.SAL) as salSum;\n"
            + "D = ORDER C BY salSum;\n";
    String expectedPlan =
        ""
            + "LogicalSort(sort0=[$6], dir0=[ASC])\n"
            + "  LogicalProject(DEPTNO=[$0.DEPTNO], MGR=[$0.MGR], HIREDATE=[$0.HIREDATE], "
            + "$f3=[COUNT(PIG_BAG($1))], newCol=[1:BIGINT], comArray=[MULTISET_PROJECTION($1, 6)], "
            + "salSum=[BigDecimalSum(PIG_BAG(MULTISET_PROJECTION($1, 5)))])\n"
            + "    LogicalProject(group=[ROW($0, $1, $2)], A=[$3])\n"
            + "      LogicalAggregate(group=[{0, 1, 2}], A=[COLLECT($3)])\n"
            + "        LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], "
            + "$f3=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedOptimizedPlan =
        ""
            + "LogicalSort(sort0=[$6], dir0=[ASC])\n"
            + "  LogicalProject(DEPTNO=[$0], MGR=[$1], HIREDATE=[$2], $f3=[CAST($3):BIGINT], "
            + "newCol=[1:BIGINT], comArray=[$4], salSum=[CAST($5):DECIMAL(19, 0)])\n"
            + "    LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT()], agg#1=[COLLECT($3)], "
            + "agg#2=[SUM($4)])\n"
            + "      LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], COMM=[$6], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    testPigRelOpTranslation(script, expectedPlan, expectedOptimizedPlan);

    String expectedSql =
        ""
            + "SELECT DEPTNO, MGR, HIREDATE, CAST(COUNT(*) AS BIGINT) AS $f3, 1 AS newCol, "
            + "COLLECT(COMM) AS comArray, CAST(SUM(SAL) AS DECIMAL(19, 0)) AS salSum\n"
            + "FROM scott.EMP\n"
            + "GROUP BY DEPTNO, MGR, HIREDATE\n"
            + "ORDER BY CAST(SUM(SAL) AS DECIMAL(19, 0))";
    testSQLTranslation(script, expectedSql);
  }

  @Test
  public void testCoGroup() throws IOException {
    String script = ""
                        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
                        + "B = FILTER A BY DEPTNO <= 30;\n"
                        + "C = FILTER A BY DEPTNO >= 20;\n"
                        + "D = GROUP A BY DEPTNO + 10, B BY (int) DEPTNO, C BY (int) DEPTNO;\n"
                        + "E = ORDER D BY $0;\n";
    String expectedPlan =
        ""
            + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  LogicalProject(group=[$0], A=[$1], B=[$2], C=[$3])\n"
            + "    LogicalProject(DEPTNO=[CASE(IS NOT NULL($0), $0, $3)], A=[$1], B=[$2], C=[$4])\n"
            + "      LogicalJoin(condition=[=($0, $3)], joinType=[full])\n"
            + "        LogicalProject(DEPTNO=[CASE(IS NOT NULL($0), $0, $2)], A=[$1], B=[$3])\n"
            + "          LogicalJoin(condition=[=($0, $2)], joinType=[full])\n"
            + "            LogicalAggregate(group=[{0}], A=[COLLECT($1)])\n"
            + "              LogicalProject($f0=[+($0, 10)], $f1=[ROW($0, $1, $2)])\n"
            + "                LogicalTableScan(table=[[scott, DEPT]])\n"
            + "            LogicalAggregate(group=[{0}], B=[COLLECT($1)])\n"
            + "              LogicalProject(DEPTNO=[CAST($0):INTEGER], $f1=[ROW($0, $1, $2)])\n"
            + "                LogicalFilter(condition=[<=($0, 30)])\n"
            + "                  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "        LogicalAggregate(group=[{0}], C=[COLLECT($1)])\n"
            + "          LogicalProject(DEPTNO=[CAST($0):INTEGER], $f1=[ROW($0, $1, $2)])\n"
            + "            LogicalFilter(condition=[>=($0, 20)])\n"
            + "              LogicalTableScan(table=[[scott, DEPT]])\n";

    String expectedResult =
        ""
            + "(10,{},{(10,ACCOUNTING,NEW YORK)},{})\n"
            + "(20,{(10,ACCOUNTING,NEW YORK)},{(20,RESEARCH,DALLAS)},{(20,RESEARCH,DALLAS)})\n"
            + "(30,{(20,RESEARCH,DALLAS)},{(30,SALES,CHICAGO)},{(30,SALES,CHICAGO)})\n"
            + "(40,{(30,SALES,CHICAGO)},{},{(40,OPERATIONS,BOSTON)})\n"
            + "(50,{(40,OPERATIONS,BOSTON)},{},{})\n";
    testPigRelOpTranslation(script, expectedPlan);
    testRunPigScript(script, expectedResult);
    String sql =
        ""
            + "SELECT CASE WHEN t4.DEPTNO IS NOT NULL THEN t4.DEPTNO ELSE t7.DEPTNO END "
            + "AS DEPTNO, t4.A, t4.B, t7.C\n"
            + "FROM (SELECT CASE WHEN t0.$f0 IS NOT NULL THEN t0.$f0 ELSE t3.DEPTNO END "
            + "AS DEPTNO, t0.A, t3.B\n"
            + "      FROM (SELECT DEPTNO + 10 AS $f0, COLLECT(ROW(DEPTNO, DNAME, LOC)) AS A\n"
            + "            FROM scott.DEPT\n"
            + "            GROUP BY DEPTNO + 10) AS t0\n"
            + "        FULL JOIN (SELECT CAST(DEPTNO AS INTEGER) AS DEPTNO, COLLECT(ROW"
            + "(DEPTNO, DNAME, LOC)) AS B\n"
            + "            FROM scott.DEPT\n"
            + "            WHERE DEPTNO <= 30\n"
            + "            GROUP BY CAST(DEPTNO AS INTEGER)) AS t3 ON t0.$f0 = t3.DEPTNO) AS t4\n"
            + "  FULL JOIN (SELECT CAST(DEPTNO AS INTEGER) AS DEPTNO, COLLECT(ROW(DEPTNO, DNAME, "
            + "LOC)) AS C\n"
            + "      FROM scott.DEPT\n"
            + "      WHERE DEPTNO >= 20\n"
            + "      GROUP BY CAST(DEPTNO AS INTEGER)) AS t7 ON t4.DEPTNO = t7.DEPTNO\n"
            + "ORDER BY CASE WHEN t4.DEPTNO IS NOT NULL THEN t4.DEPTNO ELSE t7.DEPTNO END";
    testSQLTranslation(script, sql);
  }
}

// End PigRelOpTest.java
