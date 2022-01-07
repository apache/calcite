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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.calcite.test.Matchers.hasTree;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@code PigRelOpVisitor}.
 */
class PigRelOpTest extends PigRelTestBase {
  /**
   * SQL dialect for the tests.
   */
  private static class PigRelSqlDialect extends SqlDialect {
    static final SqlDialect DEFAULT =
        new CalciteSqlDialect(SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(DatabaseProduct.CALCITE));

    private PigRelSqlDialect(Context context) {
      super(context);
    }
  }

  /** Contains a Pig script and has various methods to translate and
   * run that script and check the results. Each method returns
   * this, so that method calls for the same script can be
   * chained. */
  class Fluent {
    private final String script;

    Fluent(String script) {
      this.script = script;
    }

    private Fluent assertRel(String pigAlias, boolean optimized,
        Matcher<RelNode> relMatcher) {
      try {
        final RelNode rel;
        final List<RelNode> relNodes =
            converter.pigQuery2Rel(script, optimized, true, optimized);
        if (pigAlias == null) {
          rel = relNodes.get(0);
        } else {
          rel = converter.getBuilder().getRel(pigAlias);
        }
        assertThat(rel, relMatcher);
      } catch (IOException e) {
        throw TestUtil.rethrow(e);
      }
      return this;
    }

    private Fluent assertRel(Matcher<RelNode> relMatcher) {
      return assertRel(null, false, relMatcher);
    }

    private Fluent assertOptimizedRel(Matcher<RelNode> relMatcher) {
      return assertRel(null, true, relMatcher);
    }

    private Fluent assertSql(Matcher<String> sqlMatcher) {
      try {
        final String sql =
            converter.pigToSql(script, PigRelSqlDialect.DEFAULT).get(0);
        assertThat(sql, sqlMatcher);
        return this;
      } catch (IOException e) {
        throw TestUtil.rethrow(e);
      }
    }

    private Fluent assertSql(Matcher<String> sqlMatcher, int pos) {
      try {
        final String sql =
            converter.pigToSql(script, PigRelSqlDialect.DEFAULT).get(pos);
        assertThat(sql, sqlMatcher);
        return this;
      } catch (IOException e) {
        throw TestUtil.rethrow(e);
      }
    }

    private Fluent assertResult(Matcher<String> resultMatcher) {
      final RelNode rel;
      try {
        rel = converter.pigQuery2Rel(script, false, true, false).get(0);
      } catch (IOException e) {
        throw TestUtil.rethrow(e);
      }
      final StringWriter sw = new StringWriter();
      CalciteHandler.dump(rel, new PrintWriter(sw));
      assertThat(Util.toLinux(sw.toString()), resultMatcher);
      return this;
    }
  }

  private static void writeToFile(File f, String[] inputData) {
    try (PrintWriter pw =
             new PrintWriter(
                 new OutputStreamWriter(new FileOutputStream(f),
                     StandardCharsets.UTF_8))) {
      for (String input : inputData) {
        pw.print(input);
        pw.print("\n");
      }
    } catch (FileNotFoundException e) {
      throw TestUtil.rethrow(e);
    }
  }

  /** Creates a {@link Fluent} containing a script, that can then be used to
   * translate and execute that script. */
  private Fluent pig(String script) {
    return new Fluent(script);
  }

  @Test void testLoadFromFile() {
    final String datadir = "/tmp/pigdata";
    final String schema = "{\"fields\":["
        + "{\"name\":\"x\",\"type\":55,\"schema\":null},"
        + "{\"name\":\"y\",\"type\":10,\"schema\":null},"
        + "{\"name\":\"z\",\"type\":25,\"schema\":null}],"
        + "\"version\":0,\"sortKeys\":[],\"sortKeyOrders\":[]}";
    final File inputDir = new File(datadir, "testTable");
    inputDir.mkdirs();
    final File inputSchemaFile = new File(inputDir, ".pig_schema");
    writeToFile(inputSchemaFile, new String[]{schema});

    final String script = ""
        + "A = LOAD '" + inputDir.getAbsolutePath() + "' using PigStorage();\n"
        + "B = FILTER A BY z > 5.5;\n"
        + "C = GROUP B BY x;\n";
    final String plan = ""
        + "LogicalProject(group=[$0], B=[$1])\n"
        + "  LogicalAggregate(group=[{0}], B=[COLLECT($1)])\n"
        + "    LogicalProject(x=[$0], $f1=[ROW($0, $1, $2)])\n"
        + "      LogicalFilter(condition=[>($2, 5.5E0)])\n"
        + "        LogicalTableScan(table=[[/tmp/pigdata/testTable]])\n";
    pig(script).assertRel(hasTree(plan));
  }

  @Test void testLoadWithoutSchema() {
    final String script = "A = LOAD 'scott.DEPT';";
    final String plan = "LogicalTableScan(table=[[scott, DEPT]])\n";
    final String result = ""
        + "(10,ACCOUNTING,NEW YORK)\n"
        + "(20,RESEARCH,DALLAS)\n"
        + "(30,SALES,CHICAGO)\n"
        + "(40,OPERATIONS,BOSTON)\n";
    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result));
  }

  @Test void testLoadWithSchema() {
    final String script = ""
        + "A = LOAD 'testSchema.testTable' as (a:int, b:long, c:float, "
        + "d:double, e:chararray, "
        + "f:bytearray, g:boolean, "
        + "h:datetime, i:biginteger, j:bigdecimal, k1:tuple(), k2:tuple"
        + "(k21:int, k22:float), "
        + "l1:bag{}, "
        + "l2:bag{l21:(l22:int, l23:float)}, m1:map[], m2:map[int], m3:map["
        + "(m3:float)])\n;";
    final String plan = "LogicalTableScan(table=[[testSchema, testTable]])\n";
    pig(script).assertRel(hasTree(plan));

    final String script1 =
        "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);";
    pig(script1)
        .assertRel(hasTree("LogicalTableScan(table=[[scott, DEPT]])\n"));
  }

  @Test void testFilter() {
    final String script = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
        + "B = FILTER A BY DEPTNO == 10;\n";
    final String plan = ""
        + "LogicalFilter(condition=[=($0, 10)])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    final String result = "(10,ACCOUNTING,NEW YORK)\n";
    final String sql = "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "WHERE DEPTNO = 10";
    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testSample() {
    final String script = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
        + "B = SAMPLE A 0.5;\n";
    final String plan = ""
        + "LogicalFilter(condition=[<(RAND(), 5E-1)])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    final String sql = ""
        + "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "WHERE RAND() < 0.5";
    pig(script).assertRel(hasTree(plan))
        .assertSql(is(sql));
  }

  @Test void testSplit() {
    String script = ""
        + "A = LOAD 'scott.EMP'as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "SPLIT A INTO B1 IF DEPTNO == 10, B2 IF  DEPTNO == 20;\n"
        + "B = UNION B1, B2;\n";
    final String scan = "  LogicalTableScan(table=[[scott, EMP]])\n";
    final String plan = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalFilter(condition=[=($7, 10)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($7, 20)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";

    final String result = ""
        + "(7782,CLARK,MANAGER,7839,1981-06-09,2450.00,null,10)\n"
        + "(7839,KING,PRESIDENT,null,1981-11-17,5000.00,null,10)\n"
        + "(7934,MILLER,CLERK,7782,1982-01-23,1300.00,null,10)\n"
        + "(7369,SMITH,CLERK,7902,1980-12-17,800.00,null,20)\n"
        + "(7566,JONES,MANAGER,7839,1981-02-04,2975.00,null,20)\n"
        + "(7788,SCOTT,ANALYST,7566,1987-04-19,3000.00,null,20)\n"
        + "(7876,ADAMS,CLERK,7788,1987-05-23,1100.00,null,20)\n"
        + "(7902,FORD,ANALYST,7566,1981-12-03,3000.00,null,20)\n";

    final String sql = ""
        + "SELECT *\n"
        + "FROM scott.EMP\n"
        + "WHERE DEPTNO = 10\n"
        + "UNION ALL\n"
        + "SELECT *\n"
        + "FROM scott.EMP\n"
        + "WHERE DEPTNO = 20";
    pig(script)
        .assertRel("B1", false,
            hasTree("LogicalFilter(condition=[=($7, 10)])\n"
              + scan))
        .assertRel("B2", false,
            hasTree("LogicalFilter(condition=[=($7, 20)])\n"
              + scan))
        .assertRel(hasTree(plan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testUdf() {
    final String script = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
        + "B = FILTER A BY ENDSWITH(DNAME, 'LES');\n";
    final String plan = ""
        + "LogicalFilter(condition=[ENDSWITH(PIG_TUPLE($1, 'LES'))])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    final String result = "(30,SALES,CHICAGO)\n";
    final String sql = ""
        + "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "WHERE ENDSWITH(PIG_TUPLE(DNAME, 'LES'))";
    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testSimpleForEach1() {
    String script = ""
        + "A = LOAD 'testSchema.testTable' as (a:int, b:long, c:float, "
        + "d:double, e:chararray, f:bytearray, g:boolean, "
        + "h:datetime, i:biginteger, j:bigdecimal, k1:tuple(), "
        + "k2:tuple(k21:int, k22:float), l1:bag{}, "
        + "l2:bag{l21:(l22:int, l23:float)}, "
        + "m1:map[], m2:map[int], m3:map[(m3:float)]);\n"
        + "B = FOREACH A GENERATE a, a as a2, b, c, d, e, f, g, h, i, j, k2, "
        + "l2, m2, null as n:chararray;\n";
    final String plan = ""
        + "LogicalProject(a=[$0], a2=[$0], b=[$1], c=[$2], d=[$3], e=[$4], "
        + "f=[$5], g=[$6], h=[$7], i=[$8], j=[$9], k2=[$11], l2=[$13], "
        + "m2=[$15], n=[null:VARCHAR])\n"
        + "  LogicalTableScan(table=[[testSchema, testTable]])\n";
    final String sql = ""
        + "SELECT a, a AS a2, b, c, d, e, f, g, h, i, j, k2, l2, m2, "
        + "CAST(NULL AS VARCHAR CHARACTER SET ISO-8859-1) AS n\n"
        + "FROM testSchema.testTable";
    pig(script).assertRel(hasTree(plan))
        .assertSql(is(sql));
  }

  @Test void testSimpleForEach2() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = FOREACH A GENERATE DEPTNO + 10 as dept, MGR;\n";
    final String plan = ""
        + "LogicalProject(dept=[+($7, 10)], MGR=[$3])\n"
        + "  LogicalTableScan(table=[[scott, EMP]])\n";

    final String result = ""
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
    final String sql = ""
        + "SELECT DEPTNO + 10 AS dept, MGR\n"
        + "FROM scott.EMP";
    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testSimpleForEach3() {
    String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = FILTER A BY JOB != 'CLERK';\n"
        + "C = GROUP B BY (DEPTNO, JOB);\n"
        + "D = FOREACH C GENERATE flatten(group) as (dept, job), flatten(B);\n"
        + "E = ORDER D BY dept, job;\n";
    final String plan = ""
        + "LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
        + "  LogicalProject(dept=[$0], job=[$1], EMPNO=[$3], ENAME=[$4], "
        + "JOB=[$5], MGR=[$6], HIREDATE=[$7], SAL=[$8], COMM=[$9], "
        + "DEPTNO=[$10])\n"
        + "    LogicalCorrelate(correlation=[$cor0], joinType=[inner], "
        + "requiredColumns=[{2}])\n"
        + "      LogicalProject(dept=[$0.DEPTNO], job=[$0.JOB], B=[$1])\n"
        + "        LogicalProject(group=[ROW($0, $1)], B=[$2])\n"
        + "          LogicalAggregate(group=[{0, 1}], B=[COLLECT($2)])\n"
        + "            LogicalProject(DEPTNO=[$7], JOB=[$2], $f2=[ROW($0, "
        + "$1, $2, $3, $4, $5, $6, $7)])\n"
        + "              LogicalFilter(condition=[<>($2, 'CLERK')])\n"
        + "                LogicalTableScan(table=[[scott, EMP]])\n"
        + "      Uncollect\n"
        + "        LogicalProject($f0=[$cor0.B])\n"
        + "          LogicalValues(tuples=[[{ 0 }]])\n";

    final String sql = ""
        + "SELECT $cor1.DEPTNO AS dept, $cor1.JOB AS job, $cor1.EMPNO,"
        + " $cor1.ENAME, $cor1.JOB0 AS JOB, $cor1.MGR, $cor1.HIREDATE,"
        + " $cor1.SAL, $cor1.COMM, $cor1.DEPTNO0 AS DEPTNO\n"
        + "FROM (SELECT DEPTNO, JOB, COLLECT(ROW(EMPNO, ENAME, JOB, MGR, "
        + "HIREDATE, SAL, COMM, DEPTNO)) AS $f2\n"
        + "    FROM scott.EMP\n"
        + "    WHERE JOB <> 'CLERK'\n"
        + "    GROUP BY DEPTNO, JOB) AS $cor1,\n"
        + "  LATERAL UNNEST (SELECT $cor1.$f2 AS $f0\n"
        + "    FROM (VALUES (0)) AS t (ZERO)) AS t3 (EMPNO, ENAME, JOB,"
        + " MGR, HIREDATE, SAL, COMM, DEPTNO) AS t30\n"
        + "ORDER BY $cor1.DEPTNO, $cor1.JOB";
    pig(script).assertRel(hasTree(plan))
        .assertSql(is(sql));

    // TODO fix Calcite execution
    final String result = ""
        + "(10,7782,CLARK,MANAGER,7839,1981-06-09,2450.00,null,10)\n"
        + "(10,7839,KING,PRESIDENT,null,1981-11-17,5000.00,null,10)\n"
        + "(20,7566,JONES,MANAGER,7839,1981-02-04,2975.00,null,20)\n"
        + "(20,7788,SCOTT,ANALYST,7566,1987-04-19,3000.00,null,20)\n"
        + "(20,7902,FORD,ANALYST,7566,1981-12-03,3000.00,null,20)\n"
        + "(30,7499,ALLEN,SALESMAN,7698,1981-02-20,1600.00,300.00,30)\n"
        + "(30,7521,WARD,SALESMAN,7698,1981-02-22,1250.00,500.00,30)\n"
        + "(30,7654,MARTIN,SALESMAN,7698,1981-09-28,1250.00,1400.00,30)\n"
        + "(30,7698,BLAKE,MANAGER,7839,1981-01-05,2850.00,null,30)\n"
        + "(30,7844,TURNER,SALESMAN,7698,1981-09-08,1500.00,0.00,30)\n";
    if (false) {
      pig(script).assertResult(is(result));
    }
  }

  @Test void testForEachNested() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = GROUP A BY DEPTNO;\n"
        + "C = FOREACH B {\n"
        + "      S = FILTER A BY JOB != 'CLERK';\n"
        + "      Y = FOREACH S GENERATE ENAME, JOB, DEPTNO, SAL;\n"
        + "      X = ORDER Y BY SAL;\n"
        + "      GENERATE group, COUNT(X) as cnt, flatten(X), BigDecimalMax(X.SAL);\n"
        + "}\n"
        + "D = ORDER C BY $0;\n";
    final String plan = ""
        + "LogicalSort(sort0=[$0], dir0=[ASC])\n"
        + "  LogicalProject(group=[$0], cnt=[$1], ENAME=[$4], JOB=[$5], "
        + "DEPTNO=[$6], SAL=[$7], $f3=[$3])\n"
        + "    LogicalCorrelate(correlation=[$cor1], joinType=[inner], "
        + "requiredColumns=[{2}])\n"
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

    final String result = ""
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

    final String sql = ""
        + "SELECT $cor5.group, $cor5.cnt, $cor5.ENAME, $cor5.JOB, "
        + "$cor5.DEPTNO, $cor5.SAL, $cor5.$f3\n"
        + "FROM (SELECT $cor4.DEPTNO AS group, "
        + "COUNT(PIG_BAG($cor4.X)) AS cnt, $cor4.X, "
        + "BigDecimalMax(PIG_BAG(MULTISET_PROJECTION($cor4.X, 3))) AS $f3\n"
        + "    FROM (SELECT DEPTNO, COLLECT(ROW(EMPNO, ENAME, JOB, MGR, "
        + "HIREDATE, SAL, COMM, DEPTNO)) AS A\n"
        + "        FROM scott.EMP\n"
        + "        GROUP BY DEPTNO) AS $cor4,\n"
        + "      LATERAL (SELECT COLLECT(ROW(ENAME, JOB, DEPTNO, SAL)) AS X\n"
        + "        FROM (SELECT ENAME, JOB, DEPTNO, SAL\n"
        + "            FROM UNNEST (SELECT $cor4.A AS $f0\n"
        + "                FROM (VALUES (0)) AS t (ZERO)) "
        + "AS t2 (EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO)\n"
        + "            WHERE JOB <> 'CLERK'\n"
        + "            ORDER BY SAL) AS t5\n"
        + "        GROUP BY 'all') AS t8) AS $cor5,\n"
        + "  LATERAL UNNEST (SELECT $cor5.X AS $f0\n"
        + "    FROM (VALUES (0)) AS t (ZERO)) "
        + "AS t11 (ENAME, JOB, DEPTNO, SAL) AS t110\n"
        + "ORDER BY $cor5.group";
    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testUnionSameSchema() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = FILTER A BY DEPTNO == 10;\n"
        + "C = FILTER A BY DEPTNO == 20;\n"
        + "D = UNION B, C;\n";
    final String plan = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalFilter(condition=[=($7, 10)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalFilter(condition=[=($7, 20)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    final String result = ""
        + "(7782,CLARK,MANAGER,7839,1981-06-09,2450.00,null,10)\n"
        + "(7839,KING,PRESIDENT,null,1981-11-17,5000.00,null,10)\n"
        + "(7934,MILLER,CLERK,7782,1982-01-23,1300.00,null,10)\n"
        + "(7369,SMITH,CLERK,7902,1980-12-17,800.00,null,20)\n"
        + "(7566,JONES,MANAGER,7839,1981-02-04,2975.00,null,20)\n"
        + "(7788,SCOTT,ANALYST,7566,1987-04-19,3000.00,null,20)\n"
        + "(7876,ADAMS,CLERK,7788,1987-05-23,1100.00,null,20)\n"
        + "(7902,FORD,ANALYST,7566,1981-12-03,3000.00,null,20)\n";
    final String sql = ""
        + "SELECT *\n"
        + "FROM scott.EMP\n"
        + "WHERE DEPTNO = 10\n"
        + "UNION ALL\n"
        + "SELECT *\n"
        + "FROM scott.EMP\n"
        + "WHERE DEPTNO = 20";
    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testUnionDifferentSchemas1() {
    final String script = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
        + "B = FOREACH A GENERATE DEPTNO, DNAME;\n"
        + "C = UNION ONSCHEMA A, B;\n";
    final String plan = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[null:VARCHAR])\n"
        + "    LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    final String optimizedPlan = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[null:VARCHAR])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    final String result = ""
        + "(10,ACCOUNTING,NEW YORK)\n"
        + "(20,RESEARCH,DALLAS)\n"
        + "(30,SALES,CHICAGO)\n"
        + "(40,OPERATIONS,BOSTON)\n"
        + "(10,ACCOUNTING,null)\n"
        + "(20,RESEARCH,null)\n"
        + "(30,SALES,null)\n"
        + "(40,OPERATIONS,null)\n";
    final String sql = ""
        + "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "UNION ALL\n"
        + "SELECT DEPTNO, DNAME, "
        + "CAST(NULL AS VARCHAR CHARACTER SET ISO-8859-1) AS LOC\n"
        + "FROM scott.DEPT";
    pig(script).assertRel(hasTree(plan))
        .assertOptimizedRel(hasTree(optimizedPlan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testUnionDifferentSchemas2() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = FILTER A BY DEPTNO == 10;\n"
        + "C = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
        + "D = UNION ONSCHEMA B, C;\n";
    final String plan = ""
        + "LogicalUnion(all=[true])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], "
        + "HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], "
        + "DNAME=[null:VARCHAR], LOC=[null:VARCHAR])\n"
        + "    LogicalFilter(condition=[=($7, 10)])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalProject(EMPNO=[null:INTEGER], ENAME=[null:VARCHAR], "
        + "JOB=[null:VARCHAR], MGR=[null:INTEGER], HIREDATE=[null:DATE], "
        + "SAL=[null:DECIMAL(19, 0)], COMM=[null:DECIMAL(19, 0)], DEPTNO=[$0], "
        + "DNAME=[$1], LOC=[$2])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    final String result = ""
        + "(7782,CLARK,MANAGER,7839,1981-06-09,2450.00,null,10,null,null)\n"
        + "(7839,KING,PRESIDENT,null,1981-11-17,5000.00,null,10,null,"
        + "null)\n"
        + "(7934,MILLER,CLERK,7782,1982-01-23,1300.00,null,10,null,null)\n"
        + "(null,null,null,null,null,null,null,10,ACCOUNTING,NEW YORK)\n"
        + "(null,null,null,null,null,null,null,20,RESEARCH,DALLAS)\n"
        + "(null,null,null,null,null,null,null,30,SALES,CHICAGO)\n"
        + "(null,null,null,null,null,null,null,40,OPERATIONS,BOSTON)\n";
    final String sql = ""
        + "SELECT EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO, "
        + "CAST(NULL AS VARCHAR CHARACTER SET ISO-8859-1) AS DNAME, "
        + "CAST(NULL AS VARCHAR CHARACTER SET ISO-8859-1) AS LOC\n"
        + "FROM scott.EMP\n"
        + "WHERE DEPTNO = 10\n"
        + "UNION ALL\n"
        + "SELECT CAST(NULL AS INTEGER) AS EMPNO, "
        + "CAST(NULL AS VARCHAR CHARACTER SET ISO-8859-1) AS ENAME, "
        + "CAST(NULL AS VARCHAR CHARACTER SET ISO-8859-1) AS JOB, "
        + "CAST(NULL AS INTEGER) AS MGR, "
        + "CAST(NULL AS DATE) AS HIREDATE, CAST(NULL AS DECIMAL(19, 0)) AS SAL, "
        + "CAST(NULL AS DECIMAL(19, 0)) AS COMM, DEPTNO, DNAME, LOC\n"
        + "FROM scott.DEPT";
    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testJoin2Rels() {
    final String scanScript = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n";
    final String scanPlan = ""
        + "  LogicalTableScan(table=[[scott, EMP]])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";

    final String innerScript = scanScript
        + "C = JOIN A BY DEPTNO, B BY DEPTNO;\n";
    final String plan = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
        + scanPlan;
    final String innerSql = ""
        + "SELECT *\n"
        + "FROM scott.EMP\n"
        + "  INNER JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO";
    pig(innerScript).assertRel(hasTree(plan))
        .assertSql(is(innerSql));

    final String leftScript =
        scanScript + "C = JOIN A BY DEPTNO LEFT OUTER, B BY DEPTNO;\n";
    final String leftSql = ""
        + "SELECT *\n"
        + "FROM scott.EMP\n"
        + "  LEFT JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO";
    final String leftPlan = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
        + scanPlan;
    pig(leftScript).assertRel(hasTree(leftPlan))
        .assertSql(is(leftSql));

    final String rightScript = scanScript
        + "C = JOIN A BY DEPTNO RIGHT OUTER, B BY DEPTNO;\n";
    final String rightSql = ""
        + "SELECT *\n"
        + "FROM scott.EMP\n"
        + "  RIGHT JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO";
    final String rightPlan =
        "LogicalJoin(condition=[=($7, $8)], joinType=[right])\n"
            + scanPlan;
    pig(rightScript)
        .assertRel(hasTree(rightPlan))
        .assertSql(is(rightSql));

    final String fullScript = scanScript
        + "C = JOIN A BY DEPTNO FULL, B BY DEPTNO;\n";
    final String fullPlan = ""
        + "LogicalJoin(condition=[=($7, $8)], joinType=[full])\n"
        + scanPlan;
    final String fullSql = ""
        + "SELECT *\n"
        + "FROM scott.EMP\n"
        + "  FULL JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO";
    final String fullResult = ""
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
    pig(fullScript)
        .assertRel(hasTree(fullPlan))
        .assertSql(is(fullSql))
        .assertResult(is(fullResult));
  }

  @Test void testJoin3Rels() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
        + "C = FILTER B BY LOC == 'CHICAGO';\n"
        + "D = JOIN A BY DEPTNO, B BY DEPTNO, C BY DEPTNO;\n";
    final String plan = ""
        + "LogicalJoin(condition=[=($7, $11)], joinType=[inner])\n"
        + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalFilter(condition=[=($2, 'CHICAGO')])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    final String sql = ""
        + "SELECT *\n"
        + "FROM scott.EMP\n"
        + "  INNER JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO\n"
        + "  INNER JOIN (SELECT *\n"
        + "    FROM scott.DEPT\n"
        + "    WHERE LOC = 'CHICAGO') AS t ON EMP.DEPTNO = t.DEPTNO";
    final String result = ""
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
    pig(script).assertRel(hasTree(plan))
        .assertSql(is(sql))
        .assertResult(is(result));

    final String script2 = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray,\n"
        + "    LOC:CHARARRAY);\n"
        + "C = FILTER B BY LOC == 'CHICAGO';\n"
        + "D = JOIN A BY (DEPTNO, ENAME), B BY (DEPTNO, DNAME),\n"
        + "    C BY (DEPTNO, DNAME);\n";
    final String plan2 = ""
        + "LogicalJoin(condition=[AND(=($7, $11), =($9, $12))], "
        + "joinType=[inner])\n"
        + "  LogicalJoin(condition=[AND(=($7, $8), =($1, $9))], "
        + "joinType=[inner])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalFilter(condition=[=($2, 'CHICAGO')])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    final String sql2 = ""
        + "SELECT *\n"
        + "FROM scott.EMP\n"
        + "  INNER JOIN scott.DEPT ON EMP.DEPTNO = DEPT.DEPTNO "
        + "AND EMP.ENAME = DEPT.DNAME\n"
        + "  INNER JOIN (SELECT *\n"
        + "    FROM scott.DEPT\n"
        + "    WHERE LOC = 'CHICAGO') AS t ON EMP.DEPTNO = t.DEPTNO "
        + "AND DEPT.DNAME = t.DNAME";
    pig(script2).assertRel(hasTree(plan2))
        .assertSql(is(sql2));
  }

  @Test void testCross() {
    final String script = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray,\n"
        + "    LOC:CHARARRAY);\n"
        + "B = FOREACH A GENERATE DEPTNO;\n"
        + "C = FILTER B BY DEPTNO <= 20;\n"
        + "D = CROSS B, C;\n";
    final String plan = ""
        + "LogicalJoin(condition=[true], joinType=[inner])\n"
        + "  LogicalProject(DEPTNO=[$0])\n"
        + "    LogicalTableScan(table=[[scott, DEPT]])\n"
        + "  LogicalFilter(condition=[<=($0, 20)])\n"
        + "    LogicalProject(DEPTNO=[$0])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    final String sql = ""
        + "SELECT *\n"
        + "FROM (SELECT DEPTNO\n"
        + "    FROM scott.DEPT) AS t,\n"
        + "    (SELECT DEPTNO\n"
        + "    FROM scott.DEPT\n"
        + "    WHERE DEPTNO <= 20) AS t1";
    final String result = ""
        + "(10,10)\n"
        + "(10,20)\n"
        + "(20,10)\n"
        + "(20,20)\n"
        + "(30,10)\n"
        + "(30,20)\n"
        + "(40,10)\n"
        + "(40,20)\n";
    pig(script).assertRel(hasTree(plan))
        .assertSql(is(sql))
        .assertResult(is(result));

    final String script2 = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray,"
        + "    LOC:CHARARRAY);\n"
        + "B = FOREACH A GENERATE DEPTNO;\n"
        + "C = FILTER B BY DEPTNO <= 20;\n"
        + "D = FILTER B BY DEPTNO > 20;\n"
        + "E = CROSS B, C, D;\n";

    final String plan2 = ""
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

    final String result2 = ""
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
    final String sql2 = ""
        + "SELECT *\n"
        + "FROM (SELECT DEPTNO\n"
        + "    FROM scott.DEPT) AS t,\n"
        + "    (SELECT DEPTNO\n"
        + "    FROM scott.DEPT\n"
        + "    WHERE DEPTNO <= 20) AS t1,\n"
        + "    (SELECT DEPTNO\n"
        + "    FROM scott.DEPT\n"
        + "    WHERE DEPTNO > 20) AS t3";
    pig(script2).assertRel(hasTree(plan2))
        .assertResult(is(result2))
        .assertSql(is(sql2));
  }

  @Test void testGroupby() {
    final String baseScript =
        "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n";
    final String basePlan = "      LogicalTableScan(table=[[scott, DEPT]])\n";
    final String script = baseScript + "B = GROUP A BY DEPTNO;\n";
    final String plan = ""
        + "LogicalProject(group=[$0], A=[$1])\n"
        + "  LogicalAggregate(group=[{0}], A=[COLLECT($1)])\n"
        + "    LogicalProject(DEPTNO=[$0], $f1=[ROW($0, $1, $2)])\n"
        + basePlan;
    final String result = ""
        + "(20,{(20,RESEARCH,DALLAS)})\n"
        + "(40,{(40,OPERATIONS,BOSTON)})\n"
        + "(10,{(10,ACCOUNTING,NEW YORK)})\n"
        + "(30,{(30,SALES,CHICAGO)})\n";

    final String sql = ""
        + "SELECT DEPTNO, COLLECT(ROW(DEPTNO, DNAME, LOC)) AS A\n"
        + "FROM scott.DEPT\n"
        + "GROUP BY DEPTNO";
    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result))
        .assertSql(is(sql));

    final String script1 = baseScript + "B = GROUP A ALL;\n";
    final String plan1 = ""
        + "LogicalProject(group=[$0], A=[$1])\n"
        + "  LogicalAggregate(group=[{0}], A=[COLLECT($1)])\n"
        + "    LogicalProject($f0=['all'], $f1=[ROW($0, $1, $2)])\n"
        + basePlan;
    final String result1 = ""
        + "(all,{(10,ACCOUNTING,NEW YORK),(20,RESEARCH,DALLAS),"
        + "(30,SALES,CHICAGO),(40,OPERATIONS,BOSTON)})\n";
    pig(script1).assertResult(is(result1))
        .assertRel(hasTree(plan1));
  }

  @Test void testGroupby2() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = FOREACH A GENERATE EMPNO, ENAME, JOB, MGR, SAL, COMM, DEPTNO;\n"
        + "C = GROUP B BY (DEPTNO, JOB);\n";
    final String plan = ""
        + "LogicalProject(group=[ROW($0, $1)], B=[$2])\n"
        + "  LogicalAggregate(group=[{0, 1}], B=[COLLECT($2)])\n"
        + "    LogicalProject(DEPTNO=[$6], JOB=[$2], $f2=[ROW($0, $1, $2, "
        + "$3, $4, $5, $6)])\n"
        + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],"
        + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    final String result = ""
        + "({10, MANAGER},{(7782,CLARK,MANAGER,7839,2450.00,null,10)})\n"
        + "({10, PRESIDENT},{(7839,KING,PRESIDENT,null,5000.00,null,10)})\n"
        + "({20, CLERK},{(7369,SMITH,CLERK,7902,800.00,null,20),"
        + "(7876,ADAMS,CLERK,7788,1100.00,null,20)})\n"
        + "({30, MANAGER},{(7698,BLAKE,MANAGER,7839,2850.00,null,30)})\n"
        + "({20, ANALYST},{(7788,SCOTT,ANALYST,7566,3000.00,null,20),"
        + "(7902,FORD,ANALYST,7566,3000.00,null,20)})\n"
        + "({30, SALESMAN},{(7499,ALLEN,SALESMAN,7698,1600.00,300.00,30),"
        + "(7521,WARD,SALESMAN,7698,1250.00,500.00,30),"
        + "(7654,MARTIN,SALESMAN,7698,1250.00,1400.00,30),"
        + "(7844,TURNER,SALESMAN,7698,1500.00,0.00,30)})\n"
        + "({30, CLERK},{(7900,JAMES,CLERK,7698,950.00,null,30)})\n"
        + "({20, MANAGER},{(7566,JONES,MANAGER,7839,2975.00,null,20)})\n"
        + "({10, CLERK},{(7934,MILLER,CLERK,7782,1300.00,null,10)})\n";

    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result));
  }

  @Test void testCubeCube() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = CUBE A BY CUBE(DEPTNO, JOB);\n"
        + "C = FOREACH B GENERATE group, COUNT(cube.ENAME);\n";
    final String plan = ""
        + "LogicalProject(group=[$0], $f1=[COUNT(PIG_BAG"
        + "(MULTISET_PROJECTION($1, 3)))])\n"
        + "  LogicalProject(group=[ROW($0, $1)], cube=[$2])\n"
        + "    LogicalAggregate(group=[{0, 1}], "
        + "groups=[[{0, 1}, {0}, {1}, {}]], cube=[COLLECT($2)])\n"
        + "      LogicalProject(DEPTNO=[$7], JOB=[$2], "
        + "$f2=[ROW($7, $2, $0, $1, $3, $4, $5, $6)])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    final String optimizedPlan = ""
        + "LogicalProject(group=[ROW($0, $1)], $f1=[CAST($2):BIGINT])\n"
        + "  LogicalAggregate(group=[{0, 1}], "
        + "groups=[[{0, 1}, {0}, {1}, {}]], agg#0=[COUNT($2)])\n"
        + "    LogicalProject(DEPTNO=[$7], JOB=[$2], ENAME=[$1])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    final String result = ""
        + "({30, SALESMAN},4)\n"
        + "({30, null},6)\n"
        + "({10, null},3)\n"
        + "({null, PRESIDENT},1)\n"
        + "({30, MANAGER},1)\n"
        + "({20, MANAGER},1)\n"
        + "({20, ANALYST},2)\n"
        + "({10, MANAGER},1)\n"
        + "({null, CLERK},4)\n"
        + "(null,14)\n"
        + "({20, null},5)\n"
        + "({10, PRESIDENT},1)\n"
        + "({null, ANALYST},2)\n"
        + "({null, SALESMAN},4)\n"
        + "({30, CLERK},1)\n"
        + "({10, CLERK},1)\n"
        + "({20, CLERK},2)\n"
        + "({null, MANAGER},3)\n";

    final String sql = ""
        + "SELECT ROW(DEPTNO, JOB) AS group,"
        + " CAST(COUNT(ENAME) AS BIGINT) AS $f1\n"
        + "FROM scott.EMP\n"
        + "GROUP BY CUBE(DEPTNO, JOB)";
    pig(script).assertRel(hasTree(plan))
        .assertOptimizedRel(hasTree(optimizedPlan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testCubeRollup() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = CUBE A BY ROLLUP(DEPTNO, JOB);\n"
        + "C = FOREACH B GENERATE group, COUNT(cube.ENAME);\n";
    final String plan = ""
        + "LogicalProject(group=[$0], $f1=[COUNT(PIG_BAG"
        + "(MULTISET_PROJECTION($1, 3)))])\n"
        + "  LogicalProject(group=[ROW($0, $1)], cube=[$2])\n"
        + "    LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, {1}, {}]],"
        + " cube=[COLLECT($2)])\n"
        + "      LogicalProject(DEPTNO=[$7], JOB=[$2], $f2=[ROW($7, $2, $0,"
        + " $1, $3, $4, $5, $6)])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    final String optimizedPlan = ""
        + "LogicalProject(group=[ROW($0, $1)], $f1=[CAST($2):BIGINT])\n"
        + "  LogicalAggregate(group=[{0, 1}], groups=[[{0, 1}, {1}, {}]], "
        + "agg#0=[COUNT($2)])\n"
        + "    LogicalProject(DEPTNO=[$7], JOB=[$2], ENAME=[$1])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    final String result = ""
        + "({30, SALESMAN},4)\n"
        + "({null, PRESIDENT},1)\n"
        + "({30, MANAGER},1)\n"
        + "({20, MANAGER},1)\n"
        + "({20, ANALYST},2)\n"
        + "({10, MANAGER},1)\n"
        + "({null, CLERK},4)\n"
        + "(null,14)\n"
        + "({10, PRESIDENT},1)\n"
        + "({null, ANALYST},2)\n"
        + "({null, SALESMAN},4)\n"
        + "({30, CLERK},1)\n"
        + "({10, CLERK},1)\n"
        + "({20, CLERK},2)\n"
        + "({null, MANAGER},3)\n";
    final String sql = ""
        + "SELECT ROW(DEPTNO, JOB) AS group, "
        + "CAST(COUNT(ENAME) AS BIGINT) AS $f1\n"
        + "FROM scott.EMP\n"
        + "GROUP BY ROLLUP(DEPTNO, JOB)";
    pig(script).assertRel(hasTree(plan))
        .assertOptimizedRel(hasTree(optimizedPlan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testMultisetProjection() {
    final String script = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray,\n"
        + "    LOC:CHARARRAY);\n"
        + "B = GROUP A BY DEPTNO;\n"
        + "C = FOREACH B GENERATE A.(DEPTNO, DNAME);\n";
    final String plan = ""
        + "LogicalProject($f0=[MULTISET_PROJECTION($1, 0, 1)])\n"
        + "  LogicalProject(group=[$0], A=[$1])\n"
        + "    LogicalAggregate(group=[{0}], A=[COLLECT($1)])\n"
        + "      LogicalProject(DEPTNO=[$0], $f1=[ROW($0, $1, $2)])\n"
        + "        LogicalTableScan(table=[[scott, DEPT]])\n";
    final String optimizedPlan = ""
        + "LogicalProject($f0=[$1])\n"
        + "  LogicalAggregate(group=[{0}], agg#0=[COLLECT($1)])\n"
        + "    LogicalProject(DEPTNO=[$0], $f2=[ROW($0, $1)])\n"
        + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    final String result = ""
        + "({(20,RESEARCH)})\n"
        + "({(40,OPERATIONS)})\n"
        + "({(10,ACCOUNTING)})\n"
        + "({(30,SALES)})\n";
    final String sql = ""
        + "SELECT COLLECT(ROW(DEPTNO, DNAME)) AS $f0\n"
        + "FROM scott.DEPT\n"
        + "GROUP BY DEPTNO";
    pig(script).assertRel(hasTree(plan))
        .assertOptimizedRel(hasTree(optimizedPlan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testOrderBy() {
    final String scan = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray,\n"
        + "    LOC:CHARARRAY);\n";
    final String scanPlan =
        "  LogicalTableScan(table=[[scott, DEPT]])\n";

    final String plan0 = "LogicalSort(sort0=[$1], dir0=[ASC])\n"
        + scanPlan;
    final String script0 = scan + "B = ORDER A BY DNAME;\n";
    final String sql0 = "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "ORDER BY DNAME";
    final String result0 = ""
        + "(10,ACCOUNTING,NEW YORK)\n"
        + "(40,OPERATIONS,BOSTON)\n"
        + "(20,RESEARCH,DALLAS)\n"
        + "(30,SALES,CHICAGO)\n";
    pig(script0).assertRel(hasTree(plan0))
        .assertSql(is(sql0))
        .assertResult(is(result0));

    final String plan1 = "LogicalSort(sort0=[$1], dir0=[DESC])\n"
        + scanPlan;
    final String script1 = scan + "B = ORDER A BY DNAME DESC;\n";
    final String sql1 = "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "ORDER BY DNAME DESC";
    pig(script1).assertRel(hasTree(plan1))
        .assertSql(is(sql1));

    final String plan2 = ""
        + "LogicalSort(sort0=[$2], sort1=[$0], dir0=[DESC], dir1=[ASC])\n"
        + scanPlan;
    final String script2 = scan + "B = ORDER A BY LOC DESC, DEPTNO;\n";
    final String sql2 = "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "ORDER BY LOC DESC, DEPTNO";
    pig(script2).assertRel(hasTree(plan2))
        .assertSql(is(sql2));

    final String plan3 = ""
        + "LogicalSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC])\n"
        + scanPlan;
    final String script3 = scan + "B = ORDER A BY *;\n";
    final String sql3 = "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "ORDER BY DEPTNO, DNAME, LOC";
    pig(script3).assertRel(hasTree(plan3))
        .assertSql(is(sql3));

    final String plan4 = ""
        + "LogicalSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[DESC], dir1=[DESC], dir2=[DESC])\n"
        + scanPlan;
    final String script4 = scan + "B = ORDER A BY * DESC;\n";
    final String result4 = ""
        + "(40,OPERATIONS,BOSTON)\n"
        + "(30,SALES,CHICAGO)\n"
        + "(20,RESEARCH,DALLAS)\n"
        + "(10,ACCOUNTING,NEW YORK)\n";
    pig(script4).assertRel(hasTree(plan4))
        .assertResult(is(result4));
  }

  @Test void testRank() {
    final String base = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = FOREACH A GENERATE EMPNO, JOB, DEPTNO;\n";
    final String basePlan = ""
        + "  LogicalProject(EMPNO=[$0], JOB=[$2], DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    final String optimizedPlan = ""
        + "LogicalProject(rank_B=[$3], EMPNO=[$0], JOB=[$1], DEPTNO=[$2])\n"
        + "  LogicalWindow(window#0=[window(order by [2, 1 DESC] "
        + "aggs [RANK()])])\n"
        + "    LogicalProject(EMPNO=[$0], JOB=[$2], DEPTNO=[$7])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";

    final String script = base + "C = RANK B BY DEPTNO ASC, JOB DESC;\n";
    final String plan = ""
        + "LogicalProject(rank_B=[RANK() OVER (ORDER BY $2, $1 DESC)], "
        + "EMPNO=[$0], JOB=[$1], DEPTNO=[$2])\n"
        + basePlan;
    final String result = ""
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
    final String sql = ""
        + "SELECT RANK() OVER (ORDER BY DEPTNO, JOB DESC RANGE BETWEEN "
        + "UNBOUNDED PRECEDING AND CURRENT ROW) AS rank_B, EMPNO, JOB, DEPTNO\n"
        + "FROM scott.EMP";
    pig(script).assertRel(hasTree(plan))
        .assertOptimizedRel(hasTree(optimizedPlan))
        .assertResult(is(result))
        .assertSql(is(sql));

    final String script2 = base + "C = RANK B BY DEPTNO ASC, JOB DESC DENSE;\n";
    final String optimizedPlan2 = ""
        + "LogicalProject(rank_B=[$3], EMPNO=[$0], JOB=[$1], DEPTNO=[$2])\n"
        + "  LogicalWindow(window#0=[window(order by [2, 1 DESC] "
        + "aggs [DENSE_RANK()])"
        + "])\n"
        + "    LogicalProject(EMPNO=[$0], JOB=[$2], DEPTNO=[$7])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    final String plan2 = ""
        + "LogicalProject(rank_B=[DENSE_RANK() OVER (ORDER BY $2, $1 DESC)], "
        + "EMPNO=[$0], JOB=[$1], DEPTNO=[$2])\n"
        + basePlan;
    final String result2 = ""
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
    final String sql2 = ""
        + "SELECT DENSE_RANK() OVER (ORDER BY DEPTNO, JOB DESC RANGE BETWEEN "
        + "UNBOUNDED PRECEDING AND CURRENT ROW) AS rank_B, EMPNO, JOB, DEPTNO\n"
        + "FROM scott.EMP";
    pig(script2).assertRel(hasTree(plan2))
        .assertOptimizedRel(hasTree(optimizedPlan2))
        .assertResult(is(result2))
        .assertSql(is(sql2));
  }

  @Test void testLimit() {
    final String scan = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray,\n"
        + "    LOC:CHARARRAY);\n";
    final String scanPlan = "  LogicalTableScan(table=[[scott, DEPT]])\n";

    final String plan1 = "LogicalSort(sort0=[$1], dir0=[ASC], fetch=[2])\n"
        + scanPlan;
    final String script1 = scan
        + "B = ORDER A BY DNAME;\n"
        + "C = LIMIT B 2;\n";
    final String sql1 = "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "ORDER BY DNAME\n"
        + "FETCH NEXT 2 ROWS ONLY";
    final String result1 = ""
        + "(10,ACCOUNTING,NEW YORK)\n"
        + "(40,OPERATIONS,BOSTON)\n";
    pig(script1).assertRel(hasTree(plan1))
        .assertSql(is(sql1))
        .assertResult(is(result1));

    final String plan2 = "LogicalSort(fetch=[2])\n"
        + scanPlan;
    final String script2 = scan + "B = LIMIT A 2;\n";
    final String sql2 = "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "FETCH NEXT 2 ROWS ONLY";
    pig(script2).assertRel(hasTree(plan2))
        .assertSql(is(sql2));
  }

  @Test void testDistinct() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = FOREACH A GENERATE DEPTNO;\n"
        + "C = DISTINCT B;\n";
    final String plan = ""
        + "LogicalAggregate(group=[{0}])\n"
        + "  LogicalProject(DEPTNO=[$7])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";

    final String result = ""
        + "(20)\n"
        + "(10)\n"
        + "(30)\n";
    final String sql = "SELECT DEPTNO\n"
        + "FROM scott.EMP\n"
        + "GROUP BY DEPTNO";
    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testAggregate() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = GROUP A BY DEPTNO;\n"
        + "C = FOREACH B GENERATE group, COUNT(A), BigDecimalSum(A.SAL);\n";
    final String plan = ""
        + "LogicalProject(group=[$0], $f1=[COUNT(PIG_BAG($1))], "
        + "$f2=[BigDecimalSum(PIG_BAG(MULTISET_PROJECTION($1, 5)))])\n"
        + "  LogicalProject(group=[$0], A=[$1])\n"
        + "    LogicalAggregate(group=[{0}], A=[COLLECT($1)])\n"
        + "      LogicalProject(DEPTNO=[$7], $f1=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    final String optimizedPlan = ""
        + "LogicalProject(group=[$0], $f1=[CAST($1):BIGINT], $f2=[CAST($2):DECIMAL(19, 0)])\n"
        + "  LogicalAggregate(group=[{0}], agg#0=[COUNT()], agg#1=[SUM($1)])\n"
        + "    LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
        + "      LogicalTableScan(table=[[scott, EMP]])\n";
    final String result = ""
        + "(20,5,10875.00)\n"
        + "(10,3,8750.00)\n"
        + "(30,6,9400.00)\n";
    final String sql = ""
        + "SELECT DEPTNO AS group, CAST(COUNT(*) AS BIGINT) AS $f1, CAST(SUM(SAL) AS "
        + "DECIMAL(19, 0)) AS $f2\n"
        + "FROM scott.EMP\n"
        + "GROUP BY DEPTNO";
    pig(script).assertRel(hasTree(plan))
        .assertOptimizedRel(hasTree(optimizedPlan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testAggregate2() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
        + "C = FOREACH B GENERATE group, COUNT(A), SUM(A.SAL) as salSum;\n"
        + "D = ORDER C BY salSum;\n";
    final String plan = ""
        + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
        + "  LogicalProject(group=[$0], $f1=[COUNT(PIG_BAG($1))], "
        + "salSum=[BigDecimalSum(PIG_BAG(MULTISET_PROJECTION($1, 5)))])\n"
        + "    LogicalProject(group=[ROW($0, $1, $2)], A=[$3])\n"
        + "      LogicalAggregate(group=[{0, 1, 2}], A=[COLLECT($3)])\n"
        + "        LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], "
        + "$f3=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
        + "          LogicalTableScan(table=[[scott, EMP]])\n";
    final String optimizedPlan = ""
        + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
        + "  LogicalProject(group=[ROW($0, $1, $2)], $f1=[CAST($3):BIGINT], "
        + "salSum=[CAST($4):DECIMAL(19, 0)])\n"
        + "    LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT()], agg#1=[SUM($3)])\n"
        + "      LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    pig(script).assertRel(hasTree(plan))
        .assertOptimizedRel(hasTree(optimizedPlan));

    final String result = ""
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
    final String sql = ""
        + "SELECT ROW(DEPTNO, MGR, HIREDATE) AS group, CAST(COUNT(*) AS "
        + "BIGINT) AS $f1, CAST(SUM(SAL) AS DECIMAL(19, 0)) AS salSum\n"
        + "FROM scott.EMP\n"
        + "GROUP BY DEPTNO, MGR, HIREDATE\n"
        + "ORDER BY CAST(SUM(SAL) AS DECIMAL(19, 0))";
    pig(script).assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testAggregate2half() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
        + "C = FOREACH B GENERATE flatten(group) as (DEPTNO, MGR, HIREDATE),\n"
        + "    COUNT(A), SUM(A.SAL) as salSum, MAX(A.DEPTNO) as maxDep,\n"
        + "    MIN(A.HIREDATE) as minHire;\n"
        + "D = ORDER C BY salSum;\n";
    final String plan = ""
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

    final String optimizedPlan = ""
        + "LogicalSort(sort0=[$4], dir0=[ASC])\n"
        + "  LogicalProject(DEPTNO=[$0], MGR=[$1], HIREDATE=[$2], $f3=[CAST($3):BIGINT], "
        + "salSum=[CAST($4):DECIMAL(19, 0)], maxDep=[CAST($5):INTEGER], minHire=[$6])\n"
        + "    LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT()"
        + "], agg#1=[SUM($3)], agg#2=[MAX($0)], agg#3=[MIN($2)])\n"
        + "      LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    pig(script).assertRel(hasTree(plan))
        .assertOptimizedRel(hasTree(optimizedPlan));

    final String script2 = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
        + "C = FOREACH B GENERATE group.DEPTNO, COUNT(A), SUM(A.SAL) as salSum, "
        + "group.MGR, MAX(A.DEPTNO) as maxDep, MIN(A.HIREDATE) as minHire;\n"
        + "D = ORDER C BY salSum;\n";
    final String plan2 = ""
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

    final String optimizedPlan2 = ""
        + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
        + "  LogicalProject(DEPTNO=[$0], $f1=[CAST($3):BIGINT], salSum=[CAST($4):DECIMAL(19, 0)]"
        + ", MGR=[$1], maxDep=[CAST($5):INTEGER], minHire=[$6])\n"
        + "    LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT()], agg#1=[SUM($3)], "
        + "agg#2=[MAX($0)], agg#3=[MIN($2)])\n"
        + "      LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    pig(script2).assertRel(hasTree(plan2))
        .assertOptimizedRel(hasTree(optimizedPlan2));
  }

  @Test void testAggregate3() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
        + "C = FOREACH B GENERATE group, COUNT(A) + 1, BigDecimalSum(A.SAL) as "
        + "salSum, BigDecimalSum(A.SAL) / COUNT(A) as salAvg;\n"
        + "D = ORDER C BY salSum;\n";
    final String plan = ""
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
    final String optimizedPlan = ""
        + "LogicalSort(sort0=[$2], dir0=[ASC])\n"
        + "  LogicalProject(group=[ROW($0, $1, $2)], $f1=[+($3, 1)], salSum=[CAST($4):DECIMAL(19,"
        + " 0)], salAvg=[/(CAST($4):DECIMAL(19, 0), CAST($3):DECIMAL(19, 0))])\n"
        + "    LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT()], agg#1=[SUM($3)])\n"
        + "      LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    final String result = ""
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
    final String sql = ""
        + "SELECT ROW(DEPTNO, MGR, HIREDATE) AS group, COUNT(*) + 1 AS $f1, CAST(SUM(SAL) AS "
        + "DECIMAL(19, 0)) AS salSum, CAST(SUM(SAL) AS DECIMAL(19, 0)) / CAST(COUNT(*) AS DECIMAL"
        + "(19, 0)) AS salAvg\n"
        + "FROM scott.EMP\n"
        + "GROUP BY DEPTNO, MGR, HIREDATE\n"
        + "ORDER BY CAST(SUM(SAL) AS DECIMAL(19, 0))";
    pig(script).assertRel(hasTree(plan))
        .assertOptimizedRel(hasTree(optimizedPlan))
        .assertResult(is(result))
        .assertSql(is(sql));

    final String script2 = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
        + "C = FOREACH B GENERATE group, COUNT(A) + 1, BigDecimalSum(A.SAL) as salSum, "
        + "BigDecimalSum(A.SAL) / COUNT(A) as salAvg, A;\n"
        + "D = ORDER C BY salSum;\n";
    final String sql2 = ""
        + "SELECT ROW(DEPTNO, MGR, HIREDATE) AS group, COUNT(*) + 1 AS $f1, CAST(SUM(SAL) AS "
        + "DECIMAL(19, 0)) AS salSum, CAST(SUM(SAL) AS DECIMAL(19, 0)) / CAST(COUNT(*) AS DECIMAL"
        + "(19, 0)) AS salAvg, COLLECT(ROW(EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO)) "
        + "AS A\n"
        + "FROM scott.EMP\n"
        + "GROUP BY DEPTNO, MGR, HIREDATE\n"
        + "ORDER BY CAST(SUM(SAL) AS DECIMAL(19, 0))";
    pig(script2).assertSql(is(sql2));

    final String script3 = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
        + "C = FOREACH B GENERATE group, A, COUNT(A);\n";
    final String sql3 = ""
        + "SELECT ROW(DEPTNO, MGR, HIREDATE) AS group, COLLECT(ROW(EMPNO, ENAME, "
        + "JOB, MGR, HIREDATE, SAL, COMM, DEPTNO)) AS A, CAST(COUNT(*) AS BIGINT) "
        + "AS $f2\n"
        + "FROM scott.EMP\n"
        + "GROUP BY DEPTNO, MGR, HIREDATE";
    pig(script3).assertSql(is(sql3));
  }

  @Test void testAggregate4() {
    final String script = ""
        + "A = LOAD 'scott.EMP' as (EMPNO:int, ENAME:chararray,\n"
        + "    JOB:chararray, MGR:int, HIREDATE:datetime, SAL:bigdecimal,\n"
        + "    COMM:bigdecimal, DEPTNO:int);\n"
        + "B = GROUP A BY (DEPTNO, MGR, HIREDATE);\n"
        + "C = FOREACH B GENERATE FLATTEN(group) as (DEPTNO, MGR, HIREDATE), "
        + "COUNT(A), 1L as newCol, A.COMM as comArray, SUM(A.SAL) as salSum;\n"
        + "D = ORDER C BY salSum;\n";
    final String plan = ""
        + "LogicalSort(sort0=[$6], dir0=[ASC])\n"
        + "  LogicalProject(DEPTNO=[$0.DEPTNO], MGR=[$0.MGR], HIREDATE=[$0.HIREDATE], "
        + "$f3=[COUNT(PIG_BAG($1))], newCol=[1:BIGINT], comArray=[MULTISET_PROJECTION($1, 6)], "
        + "salSum=[BigDecimalSum(PIG_BAG(MULTISET_PROJECTION($1, 5)))])\n"
        + "    LogicalProject(group=[ROW($0, $1, $2)], A=[$3])\n"
        + "      LogicalAggregate(group=[{0, 1, 2}], A=[COLLECT($3)])\n"
        + "        LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], "
        + "$f3=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
        + "          LogicalTableScan(table=[[scott, EMP]])\n";
    final String optimizedPlan = ""
        + "LogicalSort(sort0=[$6], dir0=[ASC])\n"
        + "  LogicalProject(DEPTNO=[$0], MGR=[$1], HIREDATE=[$2], $f3=[CAST($3):BIGINT], "
        + "newCol=[1:BIGINT], comArray=[$4], salSum=[CAST($5):DECIMAL(19, 0)])\n"
        + "    LogicalAggregate(group=[{0, 1, 2}], agg#0=[COUNT()], agg#1=[COLLECT($3)], "
        + "agg#2=[SUM($4)])\n"
        + "      LogicalProject(DEPTNO=[$7], MGR=[$3], HIREDATE=[$4], COMM=[$6], SAL=[$5])\n"
        + "        LogicalTableScan(table=[[scott, EMP]])\n";
    final String sql = ""
        + "SELECT DEPTNO, MGR, HIREDATE, CAST(COUNT(*) AS BIGINT) AS $f3, 1 AS newCol, "
        + "COLLECT(COMM) AS comArray, CAST(SUM(SAL) AS DECIMAL(19, 0)) AS salSum\n"
        + "FROM scott.EMP\n"
        + "GROUP BY DEPTNO, MGR, HIREDATE\n"
        + "ORDER BY CAST(SUM(SAL) AS DECIMAL(19, 0))";
    pig(script).assertRel(hasTree(plan))
        .assertOptimizedRel(hasTree(optimizedPlan))
        .assertSql(is(sql));
  }

  @Test void testCoGroup() {
    final String script = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
        + "B = FILTER A BY DEPTNO <= 30;\n"
        + "C = FILTER A BY DEPTNO >= 20;\n"
        + "D = GROUP A BY DEPTNO + 10, B BY (int) DEPTNO, C BY (int) DEPTNO;\n"
        + "E = ORDER D BY $0;\n";
    final String plan = ""
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

    final String result = ""
        + "(10,{},{(10,ACCOUNTING,NEW YORK)},{})\n"
        + "(20,{(10,ACCOUNTING,NEW YORK)},{(20,RESEARCH,DALLAS)},{(20,RESEARCH,DALLAS)})\n"
        + "(30,{(20,RESEARCH,DALLAS)},{(30,SALES,CHICAGO)},{(30,SALES,CHICAGO)})\n"
        + "(40,{(30,SALES,CHICAGO)},{},{(40,OPERATIONS,BOSTON)})\n"
        + "(50,{(40,OPERATIONS,BOSTON)},{},{})\n";

    final String sql = ""
        + "SELECT CASE WHEN t4.DEPTNO IS NOT NULL THEN t4.DEPTNO ELSE t7.DEPTNO END "
        + "AS DEPTNO, t4.A, t4.B, t7.C\n"
        + "FROM (SELECT CASE WHEN t0.$f0 IS NOT NULL THEN t0.$f0 ELSE t3.DEPTNO END "
        + "AS DEPTNO, t0.A, t3.B\n"
        + "    FROM (SELECT DEPTNO + 10 AS $f0, "
        + "COLLECT(ROW(DEPTNO, DNAME, LOC)) AS A\n"
        + "        FROM scott.DEPT\n"
        + "        GROUP BY DEPTNO + 10) AS t0\n"
        + "      FULL JOIN (SELECT CAST(DEPTNO AS INTEGER) AS DEPTNO, "
        + "COLLECT(ROW(DEPTNO, DNAME, LOC)) AS B\n"
        + "        FROM scott.DEPT\n"
        + "        WHERE DEPTNO <= 30\n"
        + "        GROUP BY CAST(DEPTNO AS INTEGER)) AS t3 "
        + "ON t0.$f0 = t3.DEPTNO) AS t4\n"
        + "  FULL JOIN (SELECT CAST(DEPTNO AS INTEGER) AS DEPTNO, COLLECT(ROW(DEPTNO, DNAME, "
        + "LOC)) AS C\n"
        + "    FROM scott.DEPT\n"
        + "    WHERE DEPTNO >= 20\n"
        + "    GROUP BY CAST(DEPTNO AS INTEGER)) AS t7 ON t4.DEPTNO = t7.DEPTNO\n"
        + "ORDER BY CASE WHEN t4.DEPTNO IS NOT NULL THEN t4.DEPTNO ELSE t7.DEPTNO END";
    pig(script).assertRel(hasTree(plan))
        .assertResult(is(result))
        .assertSql(is(sql));
  }

  @Test void testFlattenStrSplit() {
    final String script = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
        + "B = FOREACH A GENERATE FLATTEN(STRSPLIT(DNAME, ',')) as NAMES;\n";
    final String plan = ""
        + "LogicalProject(NAMES=[CAST(ITEM(STRSPLIT(PIG_TUPLE($1, ',')), 1)):BINARY(1)])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    final String sql = ""
        + "SELECT CAST(STRSPLIT(PIG_TUPLE(DNAME, ','))[1] AS BINARY(1)) AS NAMES\n"
        + "FROM scott.DEPT";
    pig(script).assertRel(hasTree(plan))
        .assertSql(is(sql));
  }

  @Test void testMultipleStores() {
    final String script = ""
        + "A = LOAD 'scott.DEPT' as (DEPTNO:int, DNAME:chararray, LOC:CHARARRAY);\n"
        + "B = FILTER A BY DEPTNO <= 30;\n"
        + "STORE B into 'output.csv';\n"
        + "C = FILTER A BY DEPTNO >= 20;\n"
        + "STORE C into 'output1.csv';\n";
    final String plan = ""
        + "LogicalFilter(condition=[<=($0, 30)])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    final String sql0 = ""
        + "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "WHERE DEPTNO <= 30";
    final String sql1 = ""
        + "SELECT *\n"
        + "FROM scott.DEPT\n"
        + "WHERE DEPTNO >= 20";
    pig(script).assertRel(hasTree(plan))
        .assertSql(is(sql0), 0)
        .assertSql(is(sql1), 1);
  }

  @Test void testRankAndFilter() {
    final String script = ""
        + "A = LOAD 'emp1' USING PigStorage(',')  as ("
        + "    id:int, name:chararray, age:int, city:chararray);\n"
        + "B = rank A;\n"
        + "C = FILTER B by ($0 > 1);";

    final String plan = ""
        + "LogicalFilter(condition=[>($0, 1)])\n"
        + "  LogicalProject(rank_A=[RANK() OVER ()], id=[$0],"
        + " name=[$1], age=[$2], city=[$3])\n"
        + "    LogicalTableScan(table=[[emp1]])\n";

    final String sql = "SELECT w0$o0 AS rank_A, id, name, age, city\n"
        + "FROM (SELECT id, name, age, city, RANK() OVER (RANGE BETWEEN "
        + "UNBOUNDED PRECEDING AND CURRENT ROW)\n"
        + "    FROM emp1) AS t\n"
        + "WHERE w0$o0 > 1";
    pig(script).assertRel(hasTree(plan))
        .assertSql(is(sql));
  }

}
