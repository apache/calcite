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
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;

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
    final RexNode iGt10 = f.gt(f.i, f.literal(10));
    final RexNode iGt30 = f.gt(f.i, f.literal(30));
    final RexNode iGe30 = f.ge(f.i, f.literal(30));
    final RexNode iGe10 = f.ge(f.i, f.literal(10));
    final RexNode iEq30 = f.eq(f.i, f.literal(30));
    final RexNode iNe10 = f.ne(f.i, f.literal(10));

    f.checkImplies(iGt30, iGt10);
    f.checkNotImplies(iGt10, iGt30);
    f.checkNotImplies(iGt10, iGe30);
    f.checkImplies(iGe30, iGt10);
    f.checkImplies(iEq30, iGt10);
    f.checkNotImplies(iGt10, iEq30);
    f.checkNotImplies(iGt10, iNe10);
    f.checkNotImplies(iGe10, iNe10);
    // identity
    f.checkImplies(iGt10, iGt10);
    f.checkImplies(iGe30, iGe30);
  }

  @Test public void testSimpleLesserCond() {
    final Fixture f = new Fixture();
    final RexNode iLt10 = f.lt(f.i, f.literal(10));
    final RexNode iLt30 = f.lt(f.i, f.literal(30));
    final RexNode iLe30 = f.le(f.i, f.literal(30));
    final RexNode iLe10 = f.le(f.i, f.literal(10));
    final RexNode iEq10 = f.eq(f.i, f.literal(10));
    final RexNode iNe10 = f.ne(f.i, f.literal(10));

    f.checkImplies(iLt10, iLt30);
    f.checkNotImplies(iLt30, iLt10);
    f.checkImplies(iLt10, iLe30);
    f.checkNotImplies(iLe30, iLt10);
    f.checkImplies(iEq10, iLt30);
    f.checkNotImplies(iLt30, iEq10);
    f.checkNotImplies(iLt10, iEq10);
    f.checkNotImplies(iLt10, iNe10);
    f.checkNotImplies(iLe10, iNe10);
    // identity
    f.checkImplies(iLt10, iLt10);
    f.checkImplies(iLe30, iLe30);
  }

  @Test public void testSimpleEq() {
    final Fixture f = new Fixture();
    final RexNode iEq30 = f.eq(f.i, f.literal(30));
    final RexNode iNe10 = f.ne(f.i, f.literal(10));
    final RexNode iNe30 = f.ne(f.i, f.literal(30));

    f.checkImplies(iEq30, iEq30);
    f.checkImplies(iNe10, iNe10);
    f.checkImplies(iEq30, iNe10);
    f.checkNotImplies(iNe10, iEq30);
    f.checkNotImplies(iNe30, iEq30);
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
    final RexNode bEqTrue = f.eq(f.bl, f.rexBuilder.makeLiteral(true));
    final RexNode bEqFalse = f.eq(f.bl, f.rexBuilder.makeLiteral(false));

    //TODO: Need to support false => true
    //f.checkImplies(bEqFalse, bEqTrue);
    f.checkNotImplies(bEqTrue, bEqFalse);
  }

  @Test public void testSimpleLong() {
    final Fixture f = new Fixture();
    final RexNode xGeBig = f.ge(f.lg, f.longLiteral(324324L));
    final RexNode xGtBigger = f.gt(f.lg, f.longLiteral(324325L));
    final RexNode xGeBigger = f.ge(f.lg, f.longLiteral(324325L));

    f.checkImplies(xGtBigger, xGeBig);
    f.checkImplies(xGtBigger, xGeBigger);
    f.checkImplies(xGeBigger, xGeBig);
    f.checkNotImplies(xGeBig, xGtBigger);
  }

  @Test public void testSimpleShort() {
    final Fixture f = new Fixture();
    final RexNode xGe10 = f.ge(f.sh, f.shortLiteral((short) 10));
    final RexNode xGe11 = f.ge(f.sh, f.shortLiteral((short) 11));

    f.checkImplies(xGe11, xGe10);
    f.checkNotImplies(xGe10, xGe11);
  }

  @Test public void testSimpleChar() {
    final Fixture f = new Fixture();
    final RexNode xGeB = f.ge(f.ch, f.charLiteral("b"));
    final RexNode xGeA = f.ge(f.ch, f.charLiteral("a"));

    f.checkImplies(xGeB, xGeA);
    f.checkNotImplies(xGeA, xGeB);
  }

  @Test public void testSimpleString() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.eq(f.str, f.rexBuilder.makeLiteral("en"));

    f.checkImplies(node1, node1);
  }

  @Ignore("work in progress")
  @Test public void testSimpleDate() {
    final Fixture f = new Fixture();
    final Calendar instance = Util.calendar();
    final RexNode node1 = f.ge(f.dt, f.rexBuilder.makeDateLiteral(instance));
    final RexNode node2 = f.eq(f.dt, f.rexBuilder.makeDateLiteral(instance));

    f.checkImplies(node2, node1);
    f.checkNotImplies(node1, node2);
  }

  @Ignore("work in progress")
  @Test public void testSimpleTimeStamp() {
    final Fixture f = new Fixture();
    final Calendar calendar = Util.calendar();
    final RexNode node1 = f.le(f.ts, f.timestampLiteral(calendar));
    final RexNode node2 = f.le(f.ts, f.timestampLiteral(calendar));

    f.checkImplies(node1, node2);
    f.checkNotImplies(node2, node1);
  }

  @Ignore("work in progress")
  @Test public void testSimpleTime() {
    final Fixture f = new Fixture();
    final Calendar calendar = Util.calendar();
    final RexNode node1 = f.le(f.ts, f.timeLiteral(calendar));
    final RexNode node2 = f.le(f.ts, f.timeLiteral(calendar));

    f.checkImplies(node1, node2);
    f.checkNotImplies(node2, node1);
  }

  @Test public void testSimpleBetween() {
    final Fixture f = new Fixture();
    final RexNode iGe30 = f.ge(f.i, f.literal(30));
    final RexNode iLt70 = f.lt(f.i, f.literal(70));
    final RexNode iGe30AndLt70 = f.and(iGe30, iLt70);
    final RexNode iGe50 = f.ge(f.i, f.literal(50));
    final RexNode iLt60 = f.lt(f.i, f.literal(60));
    final RexNode iGe50AndLt60 = f.and(iGe50, iLt60);

    f.checkNotImplies(iGe30AndLt70, iGe50);
    f.checkNotImplies(iGe30AndLt70, iLt60);
    f.checkNotImplies(iGe30AndLt70, iGe50AndLt60);
    f.checkNotImplies(iGe30, iGe50AndLt60);
    f.checkNotImplies(iLt70, iGe50AndLt60);
    f.checkImplies(iGe50AndLt60, iGe30AndLt70);
    f.checkImplies(iGe50AndLt60, iLt70);
    f.checkImplies(iGe50AndLt60, iGe30);
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

  /** Similar to {@link MaterializationTest#testAlias()}:
   * {@code x > 1 OR (y > 2 AND z > 4)}
   * implies
   * {@code (y > 3 AND z > 5)}. */
  @Test public void testOr() {
    final Fixture f = new Fixture();
    final RexNode xGt1 = f.gt(f.i, f.literal(1));
    final RexNode yGt2 = f.gt(f.dec, f.literal(2));
    final RexNode yGt3 = f.gt(f.dec, f.literal(3));
    final RexNode zGt4 = f.gt(f.lg, f.literal(4));
    final RexNode zGt5 = f.gt(f.lg, f.literal(5));
    final RexNode yGt2AndZGt4 = f.and(yGt2, zGt4);
    final RexNode yGt3AndZGt5 = f.and(yGt3, zGt5);
    final RexNode or = f.or(xGt1, yGt2AndZGt4);
    //f.checkNotImplies(or, yGt3AndZGt5);
    f.checkImplies(yGt3AndZGt5, or);
  }

  @Test public void testNotNull() {
    final Fixture f = new Fixture();
    final RexNode node1 = f.eq(f.str, f.rexBuilder.makeLiteral("en"));
    final RexNode node2 = f.notNull(f.str);
    final RexNode node3 = f.gt(f.str, f.rexBuilder.makeLiteral("abc"));
    f.checkImplies(node1, node2);
    f.checkNotImplies(node2, node1);
    f.checkImplies(node3, node2);
    f.checkImplies(node2, node2);
  }

  @Test public void testIsNull() {
    final Fixture f = new Fixture();
    final RexNode sEqEn = f.eq(f.str, f.charLiteral("en"));
    final RexNode sIsNotNull = f.notNull(f.str);
    final RexNode sIsNull = f.isNull(f.str);
    final RexNode iEq5 = f.eq(f.i, f.literal(5));
    final RexNode iIsNull = f.isNull(f.i);
    final RexNode iIsNotNull = f.notNull(f.i);
    f.checkNotImplies(sIsNotNull, sIsNull);
    f.checkNotImplies(sIsNull, sIsNotNull);
    f.checkNotImplies(sEqEn, sIsNull);
    f.checkNotImplies(sIsNull, sEqEn);
    f.checkImplies(sEqEn, sIsNotNull); // "s = literal" implies "s is not null"
    f.checkImplies(sIsNotNull, sIsNotNull); // "s is not null" implies "s is not null"
    f.checkImplies(sIsNull, sIsNull); // "s is null" implies "s is null"

    // "s is not null and y = 5" implies "s is not null"
    f.checkImplies(f.and(sIsNotNull, iEq5), sIsNotNull);

    // "y = 5 and s is not null" implies "s is not null"
    f.checkImplies(f.and(iEq5, sIsNotNull), sIsNotNull);

    // "y is not null" does not imply "s is not null"
    f.checkNotImplies(iIsNull, sIsNotNull);

    // "s is not null or i = 5" does not imply "s is not null"
    f.checkNotImplies(f.or(sIsNotNull, iEq5), sIsNotNull);

    // "s is not null" implies "s is not null or i = 5"
    f.checkImplies(sIsNotNull, f.or(sIsNotNull, iEq5));

    // "s is not null" implies "i = 5 or s is not null"
    f.checkImplies(sIsNotNull, f.or(iEq5, sIsNotNull));

    // "i > 10" implies "x is not null"
    f.checkImplies(f.gt(f.i, f.literal(10)), iIsNotNull);

    // "-20 > i" implies "x is not null"
    f.checkImplies(f.gt(f.literal(-20), f.i), iIsNotNull);

    // "s is null and -20 > i" implies "x is not null"
    f.checkImplies(f.and(sIsNull, f.gt(f.literal(-20), f.i)), iIsNotNull);

    // "i > 10" does not imply "x is null"
    f.checkNotImplies(f.gt(f.i, f.literal(10)), iIsNull);
  }

  /** Contains all the nourishment a test case could possibly need.
   *
   * <p>We put the data in here, rather than as fields in the test case, so that
   * the data can be garbage-collected as soon as the test has executed.
   */
  @SuppressWarnings("WeakerAccess")
  public static class Fixture {
    public final RelDataTypeFactory typeFactory;
    public final RexBuilder rexBuilder;
    public final RelDataType boolRelDataType;
    public final RelDataType intRelDataType;
    public final RelDataType decRelDataType;
    public final RelDataType longRelDataType;
    public final RelDataType shortDataType;
    public final RelDataType byteDataType;
    public final RelDataType floatDataType;
    public final RelDataType charDataType;
    public final RelDataType dateDataType;
    public final RelDataType timeStampDataType;
    public final RelDataType timeDataType;
    public final RelDataType stringDataType;

    public final RexNode bl;
    public final RexNode i;
    public final RexNode dec;
    public final RexNode lg;
    public final RexNode sh;
    public final RexNode by;
    public final RexNode fl;
    public final RexNode dt;
    public final RexNode ch;
    public final RexNode ts;
    public final RexNode t;
    public final RexNode str;

    public final RexImplicationChecker checker;
    public final RelDataType rowType;
    public final RexExecutorImpl executor;
    public final RexSimplify simplify;

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
      simplify = new RexSimplify(rexBuilder, false, executor);
      checker = new RexImplicationChecker(rexBuilder, executor, rowType);
    }

    public RexInputRef ref(int i, RelDataType type) {
      return new RexInputRef(i,
          typeFactory.createTypeWithNullability(type, true));
    }

    public RexLiteral literal(int i) {
      return rexBuilder.makeExactLiteral(new BigDecimal(i));
    }

    public RexNode gt(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, node1, node2);
    }

    public RexNode ge(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(
          SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, node1, node2);
    }

    public RexNode eq(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, node1, node2);
    }

    public RexNode ne(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, node1, node2);
    }

    public RexNode lt(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, node1, node2);
    }

    public RexNode le(RexNode node1, RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, node1,
          node2);
    }

    public RexNode notNull(RexNode node1) {
      return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, node1);
    }

    public RexNode isNull(RexNode node2) {
      return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, node2);
    }

    public RexNode and(RexNode... nodes) {
      return rexBuilder.makeCall(SqlStdOperatorTable.AND, nodes);
    }

    public RexNode or(RexNode... nodes) {
      return rexBuilder.makeCall(SqlStdOperatorTable.OR, nodes);
    }

    public RexNode longLiteral(long value) {
      return rexBuilder.makeLiteral(value, longRelDataType, true);
    }

    public RexNode shortLiteral(short value) {
      return rexBuilder.makeLiteral(value, shortDataType, true);
    }

    public RexLiteral floatLiteral(double value) {
      return rexBuilder.makeApproxLiteral(new BigDecimal(value));
    }

    public RexLiteral charLiteral(String z) {
      return rexBuilder.makeCharLiteral(
          new NlsString(z, null, SqlCollation.COERCIBLE));
    }

    public RexNode timestampLiteral(Calendar calendar) {
      return rexBuilder.makeTimestampLiteral(
          calendar, timeStampDataType.getPrecision());
    }

    public RexNode timeLiteral(Calendar calendar) {
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
