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
import org.apache.calcite.util.Util;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link org.apache.calcite.piglet.PigRelExVisitor}.
 */
public class PigRelExTest extends PigRelTestBase {
  private void testPigRelOpTranslation(String pigExpr, String expectedRelExpr) throws IOException {
    String pigScript = ""
        + "A = LOAD 'test' as (a:int, b:long, c:float, d:double, e:chararray, f:bytearray, g:boolean, h:datetime, i:biginteger, "
        +                     "j:bigdecimal, k1:tuple(), k2:tuple(k21:int, k22:(k221:long, k222:chararray)), l1:bag{}, l2:bag{(l21:int, l22:float)}, "
        +                     "m1:map[], m2:map[int], m3:map[(m31:float)]);\n"
        + "B = FILTER A BY " + pigExpr + ";\n";
    RelNode rel = converter.pigQuery2Rel(pigScript, false, false, false).get(0);
    assertThat(Util.toLinux(RelOptUtil.toString(rel)), containsString(expectedRelExpr));
  }

  private void testPigConstantType(String pigExpr, String expectedRelExpr) throws IOException {
    String pigScript = ""
        + "A = LOAD 'test' as (a:int);\n"
        + "B = FOREACH A GENERATE a, " + pigExpr + ";\n";
    RelNode rel = converter.pigQuery2Rel(pigScript, false, false, false).get(0);
    assertThat(rel.getRowType().toString(), containsString(expectedRelExpr));
  }

  @Test
  public void testConstantBolean() throws IOException {
    testPigRelOpTranslation("g == false", "NOT($6)");
  }

  @Test
  public void testConstantType() throws IOException {
    testPigConstantType("0L as longCol", "BIGINT longCol");
    testPigConstantType("0 as intCol", "INTEGER intCol");
    testPigConstantType("0.0 as doubleCol", "DOUBLE doubleCol");
    testPigConstantType("'0.0' as charCol", "CHAR(3) charCol");
    testPigConstantType("true as boolCol", "BOOLEAN boolCol");
  }

  @Test
  public void testConstantFloat() throws IOException {
    testPigRelOpTranslation(".1E6 == -2.3", "=(1E5:DOUBLE, -2.3:DECIMAL(2, 1))");
  }

  @Test
  public void testConstantString() throws IOException {
    testPigRelOpTranslation("'test' == 'passed'", "=('test', 'passed')");
  }

  @Test
  public void testProjection() throws IOException {
    testPigRelOpTranslation("g", "=[$6]");
  }

  @Test
  public void testNegation() throws IOException {
    testPigRelOpTranslation("-b == -6", "=(-($1), -6)");
  }

  @Test
  public void testEqual() throws IOException {
    testPigRelOpTranslation("a == 10", "=($0, 10)");
  }

  @Test
  public void testNotEqual() throws IOException {
    testPigRelOpTranslation("b != 10", "<>($1, 10)");
  }

  @Test
  public void testLessThan() throws IOException {
    testPigRelOpTranslation("b < 10", "<($1, 10)");
  }

  @Test
  public void testLessThanEqual() throws IOException {
    testPigRelOpTranslation("b <= 10", "<=($1, 10)");
  }

  @Test
  public void testGreaterThan() throws IOException {
    testPigRelOpTranslation("b > 10", ">($1, 10)");
  }

  @Test
  public void testGreaterThanEqual() throws IOException {
    testPigRelOpTranslation("b >= 10", ">=($1, 10)");
  }

  @Test
  @Ignore
  public void testMatch() throws IOException {
    testPigRelOpTranslation("e matches 'A*BC.D'", "LIKE($4, 'A%BC_D')");
  }

  @Test// End PigRelExTest.java
  public void testIsNull() throws IOException {
    testPigRelOpTranslation("e is null", "IS NULL($4)");
  }

  @Test
  public void testIsNotNull() throws IOException {
    testPigRelOpTranslation("c is not null", "IS NOT NULL($2)");
  }

  @Test
  public void testNot() throws IOException {
    testPigRelOpTranslation("NOT(a is null)", "IS NOT NULL($0)");
    testPigRelOpTranslation("NOT(g)", "NOT($6)");
  }

  @Test
  public void testAnd() throws IOException {
    testPigRelOpTranslation("a > 10 and g", "AND(>($0, 10), $6)");
  }

  @Test
  public void testOr() throws IOException {
    testPigRelOpTranslation("a > 10 or g", "OR(>($0, 10), $6)");
  }

  @Test
  public void testAdd() throws IOException {
    testPigRelOpTranslation("b + 3", "+($1, 3)");
  }

  @Test
  public void testSubtract() throws IOException {
    testPigRelOpTranslation("b - 3", "-($1, 3)");
  }

  @Test
  public void testMultiply() throws IOException {
    testPigRelOpTranslation("b * 3", "*($1, 3)");
  }

  @Test
  public void testMod() throws IOException {
    testPigRelOpTranslation("b % 3", "MOD($1, 3)");
  }

  @Test
  public void testDivide() throws IOException {
    testPigRelOpTranslation("b / 3", "/($1, 3)");
    testPigRelOpTranslation("c / 3.1", "/($2, 3.1E0:DOUBLE)");
  }

  @Test
  public void testBinCond() throws IOException {
    testPigRelOpTranslation("(b == 1 ? 2 : 3)", "CASE(=($1, 1), 2, 3)");
  }

  @Test
  public void testTupleDereference() throws IOException {
    testPigRelOpTranslation("k2.k21", "[$11.k21]");
    testPigRelOpTranslation("k2.(k21, k22)", "[ROW($11.k21, $11.k22)]");
    testPigRelOpTranslation("k2.k22.(k221,k222)", "[ROW($11.k22.k221, $11.k22.k222)]");
  }

  @Test
  public void testBagDereference() throws IOException {
    testPigRelOpTranslation("l2.l22", "[MULTISET_PROJECTION($13, 1)]");
    testPigRelOpTranslation("l2.(l21, l22)", "[MULTISET_PROJECTION($13, 0, 1)]");
  }

  @Test
  public void testMapLookup() throws IOException, NoSuchMethodException {
    testPigRelOpTranslation("m2#'testKey'", "ITEM($15, 'testKey')");
  }

  @Test
  public void testCast() throws IOException, NoSuchMethodException {
    testPigRelOpTranslation("(int) b", "CAST($1):INTEGER");
    testPigRelOpTranslation("(long) a", "CAST($0):BIGINT");
    testPigRelOpTranslation("(float) b", "CAST($1):FLOAT");
    testPigRelOpTranslation("(double) b", "CAST($1):DOUBLE");
    testPigRelOpTranslation("(chararray) b", "CAST($1):VARCHAR");
    testPigRelOpTranslation("(bytearray) b", "CAST($1):BINARY");
    testPigRelOpTranslation("(boolean) c", "CAST($2):BOOLEAN");
    testPigRelOpTranslation("(biginteger) b", "CAST($1):DECIMAL(19, 0)");
    testPigRelOpTranslation("(bigdecimal) b", "CAST($1):DECIMAL(19, 0)");
    testPigRelOpTranslation("(tuple()) b", "CAST($1):(DynamicRecordRow[])");
    testPigRelOpTranslation("(tuple(int, float)) b", "CAST($1):RecordType(INTEGER $0, FLOAT $1)");
    testPigRelOpTranslation("(bag{}) b", "CAST($1):(DynamicRecordRow[]) NOT NULL MULTISET");
    testPigRelOpTranslation("(bag{tuple(int)}) b", "CAST($1):RecordType(INTEGER $0) MULTISET");
    testPigRelOpTranslation("(bag{tuple(int, float)}) b",
        "CAST($1):RecordType(INTEGER $0, FLOAT $1) MULTISET");
    testPigRelOpTranslation("(map[]) b", "CAST($1):(VARCHAR NOT NULL, BINARY(1) NOT NULL) MAP");
    testPigRelOpTranslation("(map[int]) b", "CAST($1):(VARCHAR NOT NULL, INTEGER");
    testPigRelOpTranslation("(map[tuple(int, float)]) b",
        "CAST($1):(VARCHAR NOT NULL, RecordType(INTEGER val_0, FLOAT val_1)) MAP");
  }

  @Test
  public void testPigBuiltinFunctions() throws IOException, NoSuchMethodException {
    testPigRelOpTranslation("ABS(-5)", "ABS(-5)");
    testPigRelOpTranslation("AddDuration(h, 'P1D')", "AddDuration(PIG_TUPLE($7, 'P1D'))");
    testPigRelOpTranslation("CEIL(1.2)", "CEIL(1.2E0:DOUBLE)");
  }
}

// End PigRelExTest.java
