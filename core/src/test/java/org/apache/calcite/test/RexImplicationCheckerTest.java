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
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.NlsString;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import plan.RexImplicationChecker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Tests the RexImplication checker
 */
public class RexImplicationCheckerTest {
  //~ Instance fields --------------------------------------------------------

  static RexBuilder rexBuilder = null;
  static RexNode bl;
  static RexNode i;
  static RexNode dec;
  static RexNode lg;
  static RexNode sh;
  static RexNode by;
  static RexNode fl;
  static RexNode dt;
  static RexNode ch;
  static RexNode ts;
  static RexNode t;

  static RelDataType boolRelDataType;
  static RelDataType intRelDataType;
  static RelDataType decRelDataType;
  static RelDataType longRelDataType;
  static RelDataType shortDataType;
  static RelDataType byteDataType;
  static RelDataType floatDataType;
  static RelDataType charDataType;
  static RelDataType dateDataType;
  static RelDataType timeStampDataType;
  static RelDataType timeDataType;
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
    longRelDataType = typeFactory.createJavaType(Long.class);
    shortDataType = typeFactory.createJavaType(Short.class);
    byteDataType = typeFactory.createJavaType(Byte.class);
    floatDataType = typeFactory.createJavaType(Float.class);
    charDataType = typeFactory.createJavaType(Character.class);
    dateDataType = typeFactory.createJavaType(Date.class);
    timeStampDataType = typeFactory.createJavaType(Timestamp.class);
    timeDataType = typeFactory.createJavaType(Time.class);

    bl = new RexInputRef(
            0,
            typeFactory.createTypeWithNullability(boolRelDataType, true));
    i = new RexInputRef(
            1,
            typeFactory.createTypeWithNullability(intRelDataType, true));
    dec = new RexInputRef(
            2,
            typeFactory.createTypeWithNullability(decRelDataType, true));
    lg = new RexInputRef(
            3,
            typeFactory.createTypeWithNullability(longRelDataType, true));
    sh = new RexInputRef(
            4,
            typeFactory.createTypeWithNullability(shortDataType, true));
    by = new RexInputRef(
            5,
            typeFactory.createTypeWithNullability(byteDataType, true));
    fl = new RexInputRef(
            6,
            typeFactory.createTypeWithNullability(floatDataType, true));
    ch = new RexInputRef(
            7,
            typeFactory.createTypeWithNullability(charDataType, true));
    dt = new RexInputRef(
            8,
            typeFactory.createTypeWithNullability(dateDataType, true));
    ts = new RexInputRef(
            9,
            typeFactory.createTypeWithNullability(timeStampDataType, true));
    t = new RexInputRef(
            10,
            typeFactory.createTypeWithNullability(timeDataType, true));

    rowType =  typeFactory.builder()
            .add("bool", boolRelDataType)
            .add("int", intRelDataType)
            .add("dec", decRelDataType)
            .add("long", longRelDataType)
            .add("short", shortDataType)
            .add("byte", byteDataType)
            .add("float", floatDataType)
            .add("char", charDataType)
            .add("date", dateDataType)
            .add("timestamp", timeStampDataType)
            .add("time", timeDataType)
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

  // Simple Tests for Operators
  @Test public void testSimpleGreaterCond() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN,
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN,
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node3 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node4 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    RexNode node5 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node6 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT_EQUALS,
                    i,
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
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN,
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node3 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node4 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    RexNode node5 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    RexNode node6 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT_EQUALS,
                    i,
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
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(30)));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.NOT_EQUALS,
                    i,
                    rexBuilder.makeExactLiteral(new BigDecimal(10)));

    //Check Identity
    checkImplies(node1, node1);
    //TODO: Support Identity
    // checkImplies(node2, node2);
    checkImplies(node1, node2);
    checkNotImplies(node2, node1);
  }

  // Simple Tests for DataTypes
  @Test public void testSimpleDec() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN,
                    dec,
                    rexBuilder.makeApproxLiteral(new BigDecimal(30.9)));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN,
                    dec,
                    rexBuilder.makeApproxLiteral(new BigDecimal(40.33)));

    checkImplies(node1, node2);
    checkNotImplies(node2, node1);
  }

  @Test public void testSimpleBoolean() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    bl,
                    rexBuilder.makeLiteral(true));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    bl,
                    rexBuilder.makeLiteral(false));

    //TODO: Need to support false => true
    //checkImplies(node2, node1);
    checkNotImplies(node1, node2);
  }

  @Test public void testSimpleLong() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    lg,
                    rexBuilder.makeLiteral(new Long(324324L), longRelDataType, true));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN,
                    lg,
                    rexBuilder.makeLiteral(new Long(324325L), longRelDataType, true));

    checkImplies(node2, node1);
    checkNotImplies(node1, node2);
  }

  @Test public void testSimpleShort() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    sh,
                    rexBuilder.makeLiteral(new Short((short) 10), shortDataType, true));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    sh,
                    rexBuilder.makeLiteral(new Short((short) 11), shortDataType, true));

    checkImplies(node2, node1);
    checkNotImplies(node1, node2);
  }

  @Test public void testSimpleChar() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    ch,
                    rexBuilder.makeCharLiteral(new NlsString("b", null, SqlCollation.COERCIBLE)));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    ch,
                    rexBuilder.makeCharLiteral(new NlsString("a", null, SqlCollation.COERCIBLE)));

    checkImplies(node1, node2);
    checkNotImplies(node2, node1);
  }

  @Test public void testSimpleDate() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    dt,
                    rexBuilder.makeDateLiteral(Calendar.getInstance()));

    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.EQUALS,
                    dt,
                    rexBuilder.makeDateLiteral(Calendar.getInstance()));

    checkImplies(node2, node1);
    checkNotImplies(node1, node2);
  }

  @Ignore("work in progress")
  @Test public void testSimpleTimeStamp() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    ts,
                    rexBuilder.makeTimestampLiteral(Calendar.getInstance(),
                            timeStampDataType.getPrecision()));


    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    ts,
                    rexBuilder.makeTimestampLiteral(Calendar.getInstance(),
                            timeStampDataType.getPrecision()));

    checkImplies(node1, node2);
    checkNotImplies(node2, node1);
  }

  @Ignore("work in progress")
  @Test public void testSimpleTime() {
    RexNode node1 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    t,
                    rexBuilder.makeTimeLiteral(Calendar.getInstance(),
                            timeDataType.getPrecision()));


    RexNode node2 =
            rexBuilder.makeCall(
                    SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    t,
                    rexBuilder.makeTimestampLiteral(Calendar.getInstance(),
                            timeDataType.getPrecision()));

    checkImplies(node1, node2);
    checkNotImplies(node2, node1);
  }

}
