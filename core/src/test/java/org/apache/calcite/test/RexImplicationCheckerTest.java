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

import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import org.apache.calcite.tools.Frameworks;

import org.junit.BeforeClass;
import org.junit.Test;

import plan.RexImplicationChecker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
/**
 * Tests the RexImplication checker
 */
public class RexImplicationCheckerTest {
  //~ Instance fields --------------------------------------------------------

  static RexBuilder rexBuilder = null;
  static RexNode a;
  static RexNode b;
  static RexNode c;
  static RexNode trueRex;
  static RexNode falseRex;
  static RelDataType boolRelDataType;
  static RelDataType intRelDataType;
  static RelDataType decRelDataType;
  static RelDataTypeFactory typeFactory;
  static RexImplicationChecker checker;
  static RelDataType rowType;
  static RexExecutorImpl executor;

  //~ Methods ----------------------------------------------------------------

  @BeforeClass
  public static void setUp() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    boolRelDataType = typeFactory.createJavaType(Boolean.class);
    intRelDataType = typeFactory.createJavaType(Integer.class);
    decRelDataType = typeFactory.createJavaType(Double.class);

    a = new RexInputRef(
            0,
            typeFactory.createTypeWithNullability(boolRelDataType, true));
    b = new RexInputRef(
            1,
            typeFactory.createTypeWithNullability(intRelDataType, true));
    c = new RexInputRef(
            2,
            typeFactory.createTypeWithNullability(decRelDataType, true));
    trueRex = rexBuilder.makeLiteral(true);
    falseRex = rexBuilder.makeLiteral(false);
    rowType =  typeFactory.builder()
            .add("bool", boolRelDataType)
            .add("int", intRelDataType)
            .add("dec", decRelDataType)
            .build();

    Frameworks.withPrepare(
        new Frameworks.PrepareAction<Void>() {
          public Void apply(RelOptCluster cluster,
                            RelOptSchema relOptSchema,
                            SchemaPlus rootSchema,
                            CalciteServerStatement statement) {
            DataContext dataContext =
                    Schemas.createDataContext(statement.getConnection());
            executor = new RexExecutorImpl(dataContext);
            return null;
          }
        });

    checker = new RexImplicationChecker(rexBuilder, executor, rowType);
  }

  private void checkImplies(RexNode node1, RexNode node2) {
    assertTrue(node1.toString() + " doesnot imply " + node2.toString() + " when it should.",
            checker.implies(node1, node2));
  }

  private void checkNotImplies(RexNode node1, RexNode node2) {
    assertFalse(node1.toString() + " implies " + node2.toString() + " when it should not",
            checker.implies(node1, node2));
  }


  @Test public void testSimpleGreaterCond() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node3 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node4 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    RexNode node5 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node6 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT_EQUALS,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    checkImplies(node2, node1);
    checkNotImplies(node1, node2);
    checkNotImplies(node1, node3);
    checkImplies(node3, node1);
    checkImplies(node5, node1);
    checkNotImplies(node1, node5);
    checkNotImplies(node1, node6);
    checkNotImplies(node4, node6);
    // TODO: Need to support Identity
    //checkImplies(node1, node1);
    //checkImplies(node3, node3);
  }

  @Test public void testSimpleLesserCond() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node3 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node4 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    RexNode node5 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    RexNode node6 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT_EQUALS,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    checkImplies(node1, node2);
    checkNotImplies(node2, node1);
    checkImplies(node1, node3);
    checkNotImplies(node3, node1);
    checkImplies(node5, node2);
    checkNotImplies(node2, node5);
    checkNotImplies(node1, node5);
    checkNotImplies(node1, node6);
    checkNotImplies(node4, node6);
    // TODO: Need to support Identity
    //checkImplies(node1, node1);
    //checkImplies(node3, node3);
  }

  @Test public void testSimpleEq() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT_EQUALS,
                    b,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    //Check Identity
    checkImplies(node1, node1);
    //TODO: Support Identity
    // checkImplies(node2, node2);
    checkImplies(node1, node2);
    checkNotImplies(node2, node1);
  }

}
