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

import org.apache.calcite.DataContexts;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptPredicateList;
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
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fixtures for verifying {@link RexImplicationChecker}.
 */
public interface RexImplicationCheckerFixtures {
  /** Contains all the nourishment a test case could possibly need.
   *
   * <p>We put the data in here, rather than as fields in the test case, so that
   * the data can be garbage-collected as soon as the test has executed.
   */
  @SuppressWarnings("WeakerAccess")
  class Fixture {
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
    public final RelDataType timestampDataType;
    public final RelDataType timeDataType;
    public final RelDataType stringDataType;

    public final RexNode bl; // a field of Java type "Boolean"
    public final RexNode i; // a field of Java type "Integer"
    public final RexNode dec; // a field of Java type "Double"
    public final RexNode lg; // a field of Java type "Long"
    public final RexNode sh; // a  field of Java type "Short"
    public final RexNode by; // a field of Java type "Byte"
    public final RexNode fl; // a field of Java type "Float" (not a SQL FLOAT)
    public final RexNode d; // a field of Java type "Date"
    public final RexNode ch; // a field of Java type "Character"
    public final RexNode ts; // a field of Java type "Timestamp"
    public final RexNode t; // a field of Java type "Time"
    public final RexNode str; // a field of Java type "String"

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
      timestampDataType = typeFactory.createJavaType(Timestamp.class);
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
      d = ref(8, dateDataType);
      ts = ref(9, timestampDataType);
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
          .add("timestamp", timestampDataType)
          .add("time", timeDataType)
          .add("string", stringDataType)
          .build();

      executor =
          Frameworks.withPrepare((cluster, relOptSchema, rootSchema, statement) ->
              new RexExecutorImpl(
                  DataContexts.of(statement.getConnection(), rootSchema)));
      simplify =
          new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor)
              .withParanoid(true);
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

    public RexNode between(RexNode node, RexNode lower, RexNode upper) {
      return rexBuilder.makeBetween(node, lower, upper);
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

    public RexNode dateLiteral(DateString d) {
      return rexBuilder.makeDateLiteral(d);
    }

    public RexNode timestampLiteral(TimestampString ts) {
      return rexBuilder.makeTimestampLiteral(ts,
          timestampDataType.getPrecision());
    }

    public RexNode timestampLocalTzLiteral(TimestampString ts) {
      return rexBuilder.makeTimestampWithLocalTimeZoneLiteral(ts,
          timestampDataType.getPrecision());
    }

    public RexNode timeLiteral(TimeString t) {
      return rexBuilder.makeTimeLiteral(t, timeDataType.getPrecision());
    }

    public RexNode cast(RelDataType type, RexNode exp) {
      return rexBuilder.makeCast(type, exp, true, false);
    }

    void checkImplies(RexNode node1, RexNode node2) {
      assertTrue(checker.implies(node1, node2),
          () -> node1 + " does not imply " + node2 + " when it should");
    }

    void checkNotImplies(RexNode node1, RexNode node2) {
      assertFalse(checker.implies(node1, node2),
          () -> node1 + " does implies " + node2 + " when it should not");
    }
  }
}
