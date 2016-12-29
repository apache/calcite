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

import org.apache.calcite.piglet.parser.ParseException;

import org.junit.Ignore;
import org.junit.Test;

/** Unit tests for Piglet. */
public class PigletTest {
  private static Fluent pig(String pig) {
    return new Fluent(pig);
  }

  @Test public void testParseLoad() throws ParseException {
    final String s = "A = LOAD 'Emp';";
    final String expected = "{op: PROGRAM, stmts: [\n"
        + "  {op: LOAD, target: A, name: Emp}]}";
    pig(s).parseContains(expected);
  }

  /** Tests parsing and un-parsing all kinds of operators. */
  @Test public void testParse2() throws ParseException {
    final String s = "A = LOAD 'Emp';\n"
        + "DESCRIBE A;\n"
        + "DUMP A;\n"
        + "B = FOREACH A GENERATE 1, name;\n"
        + "B1 = FOREACH A {\n"
        + "  X = DISTINCT A;\n"
        + "  Y = FILTER X BY foo;\n"
        + "  Z = LIMIT Z 3;\n"
        + "  GENERATE 1, name;\n"
        + "}\n"
        + "C = FILTER B BY name;\n"
        + "D = DISTINCT C;\n"
        + "E = ORDER D BY $1 DESC, $2 ASC, $3;\n"
        + "F = ORDER E BY * DESC;\n"
        + "G = LIMIT F -10;\n"
        + "H = GROUP G ALL;\n"
        + "I = GROUP H BY e;\n"
        + "J = GROUP I BY (e1, e2);\n";
    final String expected = "{op: PROGRAM, stmts: [\n"
        + "  {op: LOAD, target: A, name: Emp},\n"
        + "  {op: DESCRIBE, relation: A},\n"
        + "  {op: DUMP, relation: A},\n"
        + "  {op: FOREACH, target: B, source: A, expList: [\n"
        + "    1,\n"
        + "    name]},\n"
        + "  {op: FOREACH, target: B1, source: A, nestedOps: [\n"
        + "    {op: DISTINCT, target: X, source: A},\n"
        + "    {op: FILTER, target: Y, source: X, condition: foo},\n"
        + "    {op: LIMIT, target: Z, source: Z, count: 3}], expList: [\n"
        + "    1,\n"
        + "    name]},\n"
        + "  {op: FILTER, target: C, source: B, condition: name},\n"
        + "  {op: DISTINCT, target: D, source: C},\n"
        + "  {op: ORDER, target: E, source: D},\n"
        + "  {op: ORDER, target: F, source: E},\n"
        + "  {op: LIMIT, target: G, source: F, count: -10},\n"
        + "  {op: GROUP, target: H, source: G},\n"
        + "  {op: GROUP, target: I, source: H, keys: [\n"
        + "    e]},\n"
        + "  {op: GROUP, target: J, source: I, keys: [\n"
        + "    e1,\n"
        + "    e2]}]}";
    pig(s).parseContains(expected);
  }

  @Test public void testScan() throws ParseException {
    final String s = "A = LOAD 'EMP';";
    final String expected = "LogicalTableScan(table=[[scott, EMP]])\n";
    pig(s).explainContains(expected);
  }

  @Test public void testDump() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "DUMP A;";
    final String expected = "LogicalTableScan(table=[[scott, DEPT]])\n";
    final String out = "(10,ACCOUNTING,NEW YORK)\n"
        + "(20,RESEARCH,DALLAS)\n"
        + "(30,SALES,CHICAGO)\n"
        + "(40,OPERATIONS,BOSTON)\n";
    pig(s).explainContains(expected).returns(out);
  }

  /** VALUES is an extension to Pig. You can achieve the same effect in standard
   * Pig by creating a text file. */
  @Test public void testDumpValues() throws ParseException {
    final String s = "A = VALUES (1, 'a'), (2, 'b') AS (x: int, y: string);\n"
        + "DUMP A;";
    final String expected =
        "LogicalValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n";
    final String out = "(1,a)\n(2,b)\n";
    pig(s).explainContains(expected).returns(out);
  }

  @Test public void testForeach() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "B = FOREACH A GENERATE DNAME, $2;";
    final String expected = "LogicalProject(DNAME=[$1], LOC=[$2])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    pig(s).explainContains(expected);
  }

  @Ignore // foreach nested not implemented yet
  @Test public void testForeachNested() throws ParseException {
    final String s = "A = LOAD 'EMP';\n"
        + "B = GROUP A BY DEPTNO;\n"
        + "C = FOREACH B {\n"
        + "  D = ORDER A BY SAL DESC;\n"
        + "  E = LIMIT D 3;\n"
        + "  GENERATE E.DEPTNO, E.EMPNO;\n"
        + "}";
    final String expected = "LogicalProject(DNAME=[$1], LOC=[$2])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    pig(s).explainContains(expected);
  }

  @Test public void testGroup() throws ParseException {
    final String s = "A = LOAD 'EMP';\n"
        + "B = GROUP A BY DEPTNO;";
    final String expected = ""
        + "LogicalAggregate(group=[{7}], A=[COLLECT($8)])\n"
        + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[ROW($0, $1, $2, $3, $4, $5, $6, $7)])\n"
        + "    LogicalTableScan(table=[[scott, EMP]])\n";
    pig(s).explainContains(expected);
  }

  @Test public void testGroupExample() throws ParseException {
    final String pre = "A = VALUES ('John',18,4.0F),\n"
        + "('Mary',19,3.8F),\n"
        + "('Bill',20,3.9F),\n"
        + "('Joe',18,3.8F) AS (name:chararray,age:int,gpa:float);\n";
    final String b = pre
        + "B = GROUP A BY age;\n"
        + "DUMP B;\n";
    pig(b).returnsUnordered(
        "(18,{(John,18,4.0F),(Joe,18,3.8F)})",
        "(19,{(Mary,19,3.8F)})",
        "(20,{(Bill,20,3.9F)})");
  }

  @Test public void testDistinctExample() throws ParseException {
    final String pre = "A = VALUES (8,3,4),\n"
        + "(1,2,3),\n"
        + "(4,3,3),\n"
        + "(4,3,3),\n"
        + "(1,2,3) AS (a1:int,a2:int,a3:int);\n";
    final String x = pre
        + "X = DISTINCT A;\n"
        + "DUMP X;\n";
    pig(x).returnsUnordered("(1,2,3)",
        "(4,3,3)",
        "(8,3,4)");
  }

  @Test public void testFilter() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "B = FILTER A BY DEPTNO;";
    final String expected = "LogicalFilter(condition=[$0])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    pig(s).explainContains(expected);
  }

  @Test public void testFilterExample() throws ParseException {
    final String pre = "A = VALUES (1,2,3),\n"
        + "(4,2,1),\n"
        + "(8,3,4),\n"
        + "(4,3,3),\n"
        + "(7,2,5),\n"
        + "(8,4,3) AS (f1:int,f2:int,f3:int);\n";

    final String x = pre
        + "X = FILTER A BY f3 == 3;\n"
        + "DUMP X;\n";
    final String expected = "(1,2,3)\n"
        + "(4,3,3)\n"
        + "(8,4,3)\n";
    pig(x).returns(expected);

    final String x2 = pre
        + "X2 = FILTER A BY (f1 == 8) OR (NOT (f2+f3 > f1));\n"
        + "DUMP X2;\n";
    final String expected2 = "(4,2,1)\n"
        + "(8,3,4)\n"
        + "(7,2,5)\n"
        + "(8,4,3)\n";
    pig(x2).returns(expected2);
  }

  @Test public void testLimit() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "B = LIMIT A 3;";
    final String expected = "LogicalSort(fetch=[3])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    pig(s).explainContains(expected);
  }

  @Test public void testLimitExample() throws ParseException {
    final String pre = "A = VALUES (1,2,3),\n"
        + "(4,2,1),\n"
        + "(8,3,4),\n"
        + "(4,3,3),\n"
        + "(7,2,5),\n"
        + "(8,4,3) AS (f1:int,f2:int,f3:int);\n";

    final String x = pre
        + "X = LIMIT A 3;\n"
        + "DUMP X;\n";
    final String expected = "(1,2,3)\n"
        + "(4,2,1)\n"
        + "(8,3,4)\n";
    pig(x).returns(expected);

    final String x2 = pre
        + "B = ORDER A BY f1 DESC, f2 ASC;\n"
        + "X2 = LIMIT B 3;\n"
        + "DUMP X2;\n";
    final String expected2 = "(8,3,4)\n"
        + "(8,4,3)\n"
        + "(7,2,5)\n";
    pig(x2).returns(expected2);
  }

  @Test public void testOrder() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "B = ORDER A BY DEPTNO DESC, DNAME;";
    final String expected = ""
        + "LogicalSort(sort0=[$0], sort1=[$1], dir0=[DESC], dir1=[ASC])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    pig(s).explainContains(expected);
  }

  @Test public void testOrderStar() throws ParseException {
    final String s = "A = LOAD 'DEPT';\n"
        + "B = ORDER A BY * DESC;";
    final String expected = ""
        + "LogicalSort(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[DESC], dir1=[DESC], dir2=[DESC])\n"
        + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    pig(s).explainContains(expected);
  }

  @Test public void testOrderExample() throws ParseException {
    final String pre = "A = VALUES (1,2,3),\n"
        + "(4,2,1),\n"
        + "(8,3,4),\n"
        + "(4,3,3),\n"
        + "(7,2,5),\n"
        + "(8,4,3) AS (a1:int,a2:int,a3:int);\n";

    final String x = pre
        + "X = ORDER A BY a3 DESC;\n"
        + "DUMP X;\n";
    final String expected = "(7,2,5)\n"
        + "(8,3,4)\n"
        + "(1,2,3)\n"
        + "(4,3,3)\n"
        + "(8,4,3)\n"
        + "(4,2,1)\n";
    pig(x).returns(expected);
  }

  /** VALUES is an extension to Pig. You can achieve the same effect in standard
   * Pig by creating a text file. */
  @Test public void testValues() throws ParseException {
    final String s = "A = VALUES (1, 'a'), (2, 'b') AS (x: int, y: string);\n"
        + "DUMP A;";
    final String expected =
        "LogicalValues(tuples=[[{ 1, 'a' }, { 2, 'b' }]])\n";
    pig(s).explainContains(expected);
  }

  @Test public void testValuesNested() throws ParseException {
    final String s = "A = VALUES (1, {('a', true), ('b', false)}),\n"
        + " (2, {})\n"
        + "AS (x: int, y: bag {tuple(a: string, b: boolean)});\n"
        + "DUMP A;";
    final String expected =
        "LogicalValues(tuples=[[{ 1, [['a', true], ['b', false]] }, { 2, [] }]])\n";
    pig(s).explainContains(expected);
  }
}

// End PigletTest.java
