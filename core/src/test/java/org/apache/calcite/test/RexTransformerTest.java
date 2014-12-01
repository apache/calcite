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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTransformer;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests transformations on rex nodes.
 */
public class RexTransformerTest {
  //~ Instance fields --------------------------------------------------------

  RexBuilder rexBuilder = null;
  RexNode x;
  RexNode y;
  RexNode z;
  RexNode trueRex;
  RexNode falseRex;
  RelDataType boolRelDataType;
  RelDataTypeFactory typeFactory;

  //~ Methods ----------------------------------------------------------------

  @Before
  public void setUp() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    boolRelDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

    x = new RexInputRef(
        0,
        typeFactory.createTypeWithNullability(boolRelDataType, true));
    y = new RexInputRef(
        1,
        typeFactory.createTypeWithNullability(boolRelDataType, true));
    z = new RexInputRef(
        2,
        typeFactory.createTypeWithNullability(boolRelDataType, true));
    trueRex = rexBuilder.makeLiteral(true);
    falseRex = rexBuilder.makeLiteral(false);
  }

  void check(
      Boolean encapsulateType,
      RexNode node,
      String expected) {
    RexNode root;
    if (null == encapsulateType) {
      root = node;
    } else if (encapsulateType.equals(Boolean.TRUE)) {
      root =
          rexBuilder.makeCall(
              SqlStdOperatorTable.IS_TRUE,
              node);
    } else {
      // encapsulateType.equals(Boolean.FALSE)
      root =
          rexBuilder.makeCall(
              SqlStdOperatorTable.IS_FALSE,
              node);
    }

    RexTransformer transformer = new RexTransformer(root, rexBuilder);
    RexNode result = transformer.transformNullSemantics();
    String actual = result.toString();
    if (!actual.equals(expected)) {
      String msg =
          "\nExpected=<" + expected + ">\n  Actual=<" + actual + ">";
      fail(msg);
    }
  }

  @Test public void testPreTests() {
    // can make variable nullable?
    RexNode node =
        new RexInputRef(
            0,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BOOLEAN),
                true));
    assertTrue(node.getType().isNullable());

    // can make variable not nullable?
    node =
        new RexInputRef(
            0,
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BOOLEAN),
                false));
    assertFalse(node.getType().isNullable());
  }

  @Test public void testNonBooleans() {
    RexNode node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            x,
            y);
    String expected = node.toString();
    check(Boolean.TRUE, node, expected);
    check(Boolean.FALSE, node, expected);
    check(null, node, expected);
  }

  /**
   * the or operator should pass through unchanged since e.g. x OR y should
   * return true if x=null and y=true if it was transformed into something
   * like (x ISNOTNULL) AND (y ISNOTNULL) AND (x OR y) an incorrect result
   * could be produced
   */
  @Test public void testOrUnchanged() {
    RexNode node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            x,
            y);
    String expected = node.toString();
    check(Boolean.TRUE, node, expected);
    check(Boolean.FALSE, node, expected);
    check(null, node, expected);
  }

  @Test public void testSimpleAnd() {
    RexNode node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            x,
            y);
    check(
        Boolean.FALSE,
        node,
        "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), AND($0, $1))");
  }

  @Test public void testSimpleEquals() {
    RexNode node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            x,
            y);
    check(
        Boolean.TRUE,
        node,
        "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), =($0, $1))");
  }

  @Test public void testSimpleNotEquals() {
    RexNode node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            x,
            y);
    check(
        Boolean.FALSE,
        node,
        "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <>($0, $1))");
  }

  @Test public void testSimpleGreaterThan() {
    RexNode node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            x,
            y);
    check(
        Boolean.TRUE,
        node,
        "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), >($0, $1))");
  }

  @Test public void testSimpleGreaterEquals() {
    RexNode node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            x,
            y);
    check(
        Boolean.FALSE,
        node,
        "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), >=($0, $1))");
  }

  @Test public void testSimpleLessThan() {
    RexNode node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            x,
            y);
    check(
        Boolean.TRUE,
        node,
        "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <($0, $1))");
  }

  @Test public void testSimpleLessEqual() {
    RexNode node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            x,
            y);
    check(
        Boolean.FALSE,
        node,
        "AND(AND(IS NOT NULL($0), IS NOT NULL($1)), <=($0, $1))");
  }

  @Test public void testOptimizeNonNullLiterals() {
    RexNode node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            x,
            trueRex);
    check(Boolean.TRUE, node, "AND(IS NOT NULL($0), <=($0, true))");
    node =
        rexBuilder.makeCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            trueRex,
            x);
    check(Boolean.FALSE, node, "AND(IS NOT NULL($0), <=(true, $0))");
  }

  @Test public void testSimpleIdentifier() {
    RexNode node = rexBuilder.makeInputRef(boolRelDataType, 0);
    check(Boolean.TRUE, node, "=(IS TRUE($0), true)");
  }

  @Test public void testMixed1() {
    // x=true AND y
    RexNode op1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            x,
            trueRex);
    RexNode and =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            op1,
            y);
    check(
        Boolean.FALSE,
        and,
        "AND(IS NOT NULL($1), AND(AND(IS NOT NULL($0), =($0, true)), $1))");
  }

  @Test public void testMixed2() {
    // x!=true AND y>z
    RexNode op1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            x,
            trueRex);
    RexNode op2 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            y,
            z);
    RexNode and =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            op1,
            op2);
    check(
        Boolean.FALSE,
        and,
        "AND(AND(IS NOT NULL($0), <>($0, true)), AND(AND(IS NOT NULL($1), IS NOT NULL($2)), >($1, $2)))");
  }

  @Test public void testMixed3() {
    // x=y AND false>z
    RexNode op1 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            x,
            y);
    RexNode op2 =
        rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            falseRex,
            z);
    RexNode and =
        rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            op1,
            op2);
    check(
        Boolean.TRUE,
        and,
        "AND(AND(AND(IS NOT NULL($0), IS NOT NULL($1)), =($0, $1)), AND(IS NOT NULL($2), >(false, $2)))");
  }
}

// End RexTransformerTest.java
