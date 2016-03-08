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
import org.apache.calcite.plan.RexImplicationChecker;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.NlsString;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * Unit tests for {@link RexImplicationChecker}.
 */
public class RexImplicationCheckerTest {
  //~ Instance fields --------------------------------------------------------

  //~ Methods ----------------------------------------------------------------

  // Simple Tests for Operators
  @Test public void testSimpleGreaterCond() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.gt(f.i, f.literal(10));
    final RexNode node2 = f.gt(f.i, f.literal(30));
    final RexNode node3 = f.ge(f.i, f.literal(30));
    final RexNode node4 = f.ge(f.i, f.literal(10));
    final RexNode node5 = f.eq(f.i, f.literal(30));
    final RexNode node6 = f.ne(f.i, f.literal(10));

    f.checkImplies(node2, node1);
    f.checkNotImplies(node1, node2);
    f.checkNotImplies(node1, node3);
    f.checkImplies(node3, node1);
    f.checkImplies(node5, node1);
    f.checkNotImplies(node1, node5);
    f.checkNotImplies(node1, node6);
    f.checkNotImplies(node4, node6);
    // TODO: Need to support Identity
    //f.checkImplies(node1, node1);
    //f.checkImplies(node3, node3);
  }

  @Test public void testSimpleLesserCond() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.lt(f.i, f.literal(10));
    final RexNode node2 = f.lt(f.i, f.literal(30));
    final RexNode node3 = f.le(f.i, f.literal(30));
    final RexNode node4 = f.le(f.i, f.literal(10));
    final RexNode node5 = f.eq(f.i, f.literal(10));
    final RexNode node6 = f.ne(f.i, f.literal(10));

    f.checkImplies(node1, node2);
    f.checkNotImplies(node2, node1);
    f.checkImplies(node1, node3);
    f.checkNotImplies(node3, node1);
    f.checkImplies(node5, node2);
    f.checkNotImplies(node2, node5);
    f.checkNotImplies(node1, node5);
    f.checkNotImplies(node1, node6);
    f.checkNotImplies(node4, node6);
    // TODO: Need to support Identity
    //f.checkImplies(node1, node1);
    //f.checkImplies(node3, node3);
  }

  @Test public void testSimpleEq() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.eq(f.i, f.literal(30));
    final RexNode node2 = f.ne(f.i, f.literal(10));

    //Check Identity
    f.checkImplies(node1, node1);
    //TODO: Support Identity
    // f.checkImplies(node2, node2);
    f.checkImplies(node1, node2);
    f.checkNotImplies(node2, node1);
  }

  // Simple Tests for DataTypes
  @Test public void testSimpleDec() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.lt(f.dec, f.floatLiteral(30.9));
    final RexNode node2 = f.lt(f.dec, f.floatLiteral(40.33));

    f.checkImplies(node1, node2);
    f.checkNotImplies(node2, node1);
  }

  @Test public void testSimpleBoolean() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.eq(f.bl, f.rexBuilder.makeLiteral(true));
    final RexNode node2 = f.eq(f.bl, f.rexBuilder.makeLiteral(false));

    //TODO: Need to support false => true
    //f.checkImplies(node2, node1);
    f.checkNotImplies(node1, node2);
  }

  @Test public void testSimpleLong() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.ge(f.lg, f.longLiteral(324324L));
    final RexNode node2 = f.gt(f.lg, f.longLiteral(324325L));

    f.checkImplies(node2, node1);
    f.checkNotImplies(node1, node2);
  }

  @Test public void testSimpleShort() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.ge(f.sh, f.shortLiteral((short) 10));
    final RexNode node2 = f.ge(f.sh, f.shortLiteral((short) 11));

    f.checkImplies(node2, node1);
    f.checkNotImplies(node1, node2);
  }

  @Test public void testSimpleChar() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.ge(f.ch, f.charLiteral("b"));
    final RexNode node2 = f.ge(f.ch, f.charLiteral("a"));

    f.checkImplies(node1, node2);
    f.checkNotImplies(node2, node1);
  }

  @Test public void testSimpleString() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.eq(f.str, f.rexBuilder.makeLiteral("en"));

    f.checkImplies(node1, node1);
  }

  @Ignore("work in progress")
  @Test public void testSimpleDate() {
    final Fixture f = new Fixture();
    final Calendar instance = Calendar.getInstance();
    final RexNode node1 = f.ge(f.dt, f.rexBuilder.makeDateLiteral(instance));
    final RexNode node2 = f.eq(f.dt, f.rexBuilder.makeDateLiteral(instance));

    f.checkImplies(node2, node1);
    f.checkNotImplies(node1, node2);
  }

  @Ignore("work in progress")
  @Test public void testSimpleTimeStamp() {
    final Fixture f = new Fixture();
    final Calendar calendar = Calendar.getInstance();
    final RexNode node1 = f.le(f.ts, f.timestampLiteral(calendar));
    final RexNode node2 = f.le(f.ts, f.timestampLiteral(calendar));

    f.checkImplies(node1, node2);
    f.checkNotImplies(node2, node1);
  }

  @Ignore("work in progress")
  @Test public void testSimpleTime() {
    final Fixture f = new Fixture();
    final Calendar calendar = Calendar.getInstance();
    final RexNode node1 = f.le(f.ts, f.timeLiteral(calendar));
    final RexNode node2 = f.le(f.ts, f.timeLiteral(calendar));

    f.checkImplies(node1, node2);
    f.checkNotImplies(node2, node1);
  }

  @Test public void testSimpleBetween() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.ge(f.i, f.literal(30));
    final RexNode node2 = f.lt(f.i, f.literal(70));
    final RexNode node3 = f.and(node1, node2);
    final RexNode node4 = f.ge(f.i, f.literal(50));
    final RexNode node5 = f.lt(f.i, f.literal(60));
    final RexNode node6 = f.and(node4, node5);

    f.checkNotImplies(node3, node4);
    f.checkNotImplies(node3, node5);
    f.checkNotImplies(node3, node6);
    f.checkNotImplies(node1, node6);
    f.checkNotImplies(node2, node6);
    f.checkImplies(node6, node3);
    f.checkImplies(node6, node2);
    f.checkImplies(node6, node1);
  }

  @Test public void testSimpleBetweenCornerCases() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.gt(f.i, f.literal(30));
    final RexNode node2 = f.gt(f.i, f.literal(50));
    final RexNode node3 = f.lt(f.i, f.literal(60));
    final RexNode node4 = f.lt(f.i, f.literal(80));
    final RexNode node5 = f.lt(f.i, f.literal(90));
    final RexNode node6 = f.lt(f.i, f.literal(100));

    f.checkNotImplies(f.and(node1, node2), f.and(node3, node4));
    f.checkNotImplies(f.and(node5, node6), f.and(node3, node4));
    f.checkNotImplies(f.and(node1, node2), node6);
    f.checkNotImplies(node6, f.and(node1, node2));
    f.checkImplies(f.and(node3, node4), f.and(node5, node6));
  }

  @Test public void testNotNull() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.eq(f.str, f.rexBuilder.makeLiteral("en"));
    final RexNode node2 = f.notNull(f.str);
    final RexNode node3 = f.gt(f.str, f.rexBuilder.makeLiteral("abc"));
    f.checkImplies(node1, node2);
    f.checkNotImplies(node2, node1);
    f.checkImplies(node3, node2);
    //TODO: Tough one
    //f.checkImplies(node2, node2);
  }

  @Test public void testIsNull() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.eq(f.str, f.rexBuilder.makeLiteral("en"));
    final RexNode node2 = f.notNull(f.str);
    final RexNode node3 = f.isNull(f.str);
    f.checkNotImplies(node2, node3);
    f.checkNotImplies(node3, node2);
    f.checkNotImplies(node1, node3);
    f.checkNotImplies(node3, node1);
    //TODO:
    //f.checkImplies(node3, node3);
  }

  /** Contains all the nourishment a test case could possibly need.
   *
   * <p>We put the data in here, rather than as fields in the test case, so that
   * the data can be garbage-collected as soon as the test has executed.
   */
  private static class Fixture {
    private final RexBuilder rexBuilder;
    private final RexNode bl;
    private final RexNode i;
    private final RexNode dec;
    private final RexNode lg;
    private final RexNode sh;
    private final RexNode by;
    private final RexNode fl;
    private final RexNode dt;
    private final RexNode ch;
    private final RexNode ts;
    private final RexNode t;
    private final RexNode str;

    private final RelDataType boolRelDataType;
    private final RelDataType intRelDataType;
    private final RelDataType decRelDataType;
    private final RelDataType longRelDataType;
    private final RelDataType shortDataType;
    private final RelDataType byteDataType;
    private final RelDataType floatDataType;
    private final RelDataType charDataType;
    private final RelDataType dateDataType;
    private final RelDataType timeStampDataType;
    private final RelDataType timeDataType;
    private final RelDataType stringDataType;
    private final RelDataTypeFactory typeFactory;
    private final RexImplicationChecker checker;
    private final RelDataType rowType;
    private final RexExecutorImpl executor;

    public Fixture() {
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
      stringDataType = typeFactory.createJavaType(String.class);

      bl = ref(0, this.boolRelDataType);
      i = ref(1, intRelDataType);
      dec = ref(2, decRelDataType);
      lg = ref(3, longRelDataType);
      sh = ref(4, shortDataType);
      by = ref(5, byteDataType);
      fl = ref(6, floatDataType);
      ch = ref(7, charDataType);
      dt = ref(8, dateDataType);
      ts = ref(9, timeStampDataType);
      t = ref(10, timeDataType);
      str = ref(11, stringDataType);

      rowType = typeFactory.builder()
          .add("bool", this.boolRelDataType)
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
          .add("string", stringDataType)
          .build();

      final Holder<RexExecutorImpl> holder = Holder.of(null);
      Frameworks.withPrepare(
          new Frameworks.PrepareAction<Void>() {
            public Void apply(RelOptCluster cluster,
                RelOptSchema relOptSchema,
                SchemaPlus rootSchema,
                CalciteServerStatement statement) {
              DataContext dataContext =
                  Schemas.createDataContext(statement.getConnection());
              holder.set(new RexExecutorImpl(dataContext));
              return null;
            }
          });

      executor = holder.get();
      checker = new RexImplicationChecker(rexBuilder, executor, rowType);
    }

    RexInputRef ref(int i, RelDataType type) {
      return new RexInputRef(i,
          typeFactory.createTypeWithNullability(type, true));
    }

    RexLiteral literal(int i) {
      return rexBuilder.makeExactLiteral(new BigDecimal(i));
    }

    RexNode gt(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, node1, node2);
    }

    RexNode ge(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(
          SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, node1, node2);
    }

    RexNode eq(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, node1, node2);
    }

    RexNode ne(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, node1, node2);
    }

    RexNode lt(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, node1, node2);
    }

    RexNode le(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, node1,
          node2);
    }

    RexNode notNull(RexNode node1) {
      return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, node1);
    }

    RexNode isNull(RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, node2);
    }

    RexNode and(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.AND, node1, node2);
    }

    RexNode longLiteral(long value) {
      return rexBuilder.makeLiteral(value, longRelDataType, true);
    }

    RexNode shortLiteral(short value) {
      return rexBuilder.makeLiteral(value, shortDataType, true);
    }

    RexLiteral floatLiteral(double value) {
      return rexBuilder.makeApproxLiteral(new BigDecimal(value));
    }

    RexLiteral charLiteral(String z) {
      return rexBuilder.makeCharLiteral(
          new NlsString(z, null, SqlCollation.COERCIBLE));
    }

    RexNode timestampLiteral(Calendar calendar) {
      return rexBuilder.makeTimestampLiteral(
          calendar, timeStampDataType.getPrecision());
    }

    RexNode timeLiteral(Calendar calendar) {
      return rexBuilder.makeTimestampLiteral(
          calendar, timeDataType.getPrecision());
    }

    void checkImplies(RexNode node1, RexNode node2) {
      final String message =
          node1 + " does not imply " + node2 + " when it should";
      assertTrue(message, checker.implies(node1, node2));
    }

    void checkNotImplies(RexNode node1, RexNode node2) {
      final String message =
          node1 + " does implies " + node2 + " when it should not";
      assertFalse(message, checker.implies(node1, node2));
    }
  }
}

// End RexImplicationCheckerTest.java
