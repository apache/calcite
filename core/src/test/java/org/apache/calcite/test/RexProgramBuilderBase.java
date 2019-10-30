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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * This class provides helper methods to build rex expressions.
 */
public abstract class RexProgramBuilderBase {
  /**
   * Input variables for tests should come from a struct type, so
   * a struct is created where the first {@code MAX_FIELDS} are nullable,
   * and the next {@code MAX_FIELDS} are not nullable.
   */
  protected static final int MAX_FIELDS = 10;

  protected JavaTypeFactory typeFactory;
  protected RexBuilder rexBuilder;
  protected RexExecutor executor;
  protected RexSimplify simplify;

  protected RexLiteral trueLiteral;
  protected RexLiteral falseLiteral;
  protected RexLiteral nullBool;
  protected RexLiteral nullInt;
  protected RexLiteral nullVarchar;

  private RelDataType nullableBool;
  private RelDataType nonNullableBool;

  private RelDataType nullableInt;
  private RelDataType nonNullableInt;

  private RelDataType nullableVarchar;
  private RelDataType nonNullableVarchar;

  // Note: JUnit 4 creates new instance for each test method,
  // so we initialize these structures on demand
  // It maps non-nullable type to struct of (10 nullable, 10 non-nullable) fields
  private Map<RelDataType, RexDynamicParam> dynamicParams;

  /**
   * Dummy data context for test.
   */
  private static class DummyTestDataContext implements DataContext {
    private final ImmutableMap<String, Object> map;

    DummyTestDataContext() {
      this.map =
          ImmutableMap.of(
              Variable.TIME_ZONE.camelName, TimeZone.getTimeZone("America/Los_Angeles"),
              Variable.CURRENT_TIMESTAMP.camelName, 1311120000000L);
    }

    public SchemaPlus getRootSchema() {
      return null;
    }

    public JavaTypeFactory getTypeFactory() {
      return null;
    }

    public QueryProvider getQueryProvider() {
      return null;
    }

    public Object get(String name) {
      return map.get(name);
    }
  }

  public void setUp() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    executor =
        new RexExecutorImpl(new DummyTestDataContext());
    simplify =
        new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor)
            .withParanoid(true);
    trueLiteral = rexBuilder.makeLiteral(true);
    falseLiteral = rexBuilder.makeLiteral(false);

    nonNullableInt = typeFactory.createSqlType(SqlTypeName.INTEGER);
    nullableInt = typeFactory.createTypeWithNullability(nonNullableInt, true);
    nullInt = rexBuilder.makeNullLiteral(nullableInt);

    nonNullableBool = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    nullableBool = typeFactory.createTypeWithNullability(nonNullableBool, true);
    nullBool = rexBuilder.makeNullLiteral(nullableBool);

    nonNullableVarchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    nullableVarchar = typeFactory.createTypeWithNullability(nonNullableVarchar, true);
    nullVarchar = rexBuilder.makeNullLiteral(nullableVarchar);
  }

  private RexDynamicParam getDynamicParam(RelDataType type, String fieldNamePrefix) {
    if (dynamicParams == null) {
      dynamicParams = new HashMap<>();
    }
    return dynamicParams.computeIfAbsent(type, k -> {
      RelDataType nullableType = typeFactory.createTypeWithNullability(k, true);
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      for (int i = 0; i < MAX_FIELDS; i++) {
        builder.add(fieldNamePrefix + i, nullableType);
      }
      String notNullPrefix = "notNull"
          + Character.toUpperCase(fieldNamePrefix.charAt(0))
          + fieldNamePrefix.substring(1);

      for (int i = 0; i < MAX_FIELDS; i++) {
        builder.add(notNullPrefix + i, k);
      }
      return rexBuilder.makeDynamicParam(builder.build(), 0);
    });
  }

  protected RexNode isNull(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, node);
  }

  protected RexNode isUnknown(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_UNKNOWN, node);
  }

  protected RexNode isNotNull(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, node);
  }

  protected RexNode isFalse(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_FALSE, node);
  }

  protected RexNode isNotFalse(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_FALSE, node);
  }

  protected RexNode isTrue(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, node);
  }

  protected RexNode isNotTrue(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_TRUE, node);
  }

  protected RexNode isDistinctFrom(RexNode a, RexNode b) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_DISTINCT_FROM, a, b);
  }

  protected RexNode isNotDistinctFrom(RexNode a, RexNode b) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, a, b);
  }

  protected RexNode nullIf(RexNode node1, RexNode node2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.NULLIF, node1, node2);
  }

  protected RexNode not(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.NOT, node);
  }

  protected RexNode unaryMinus(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, node);
  }

  protected RexNode unaryPlus(RexNode node) {
    return rexBuilder.makeCall(SqlStdOperatorTable.UNARY_PLUS, node);
  }

  protected RexNode and(RexNode... nodes) {
    return and(ImmutableList.copyOf(nodes));
  }

  protected RexNode and(Iterable<? extends RexNode> nodes) {
    // Does not flatten nested ANDs. We want test input to contain nested ANDs.
    return rexBuilder.makeCall(SqlStdOperatorTable.AND,
        ImmutableList.copyOf(nodes));
  }

  protected RexNode or(RexNode... nodes) {
    return or(ImmutableList.copyOf(nodes));
  }

  protected RexNode or(Iterable<? extends RexNode> nodes) {
    // Does not flatten nested ORs. We want test input to contain nested ORs.
    return rexBuilder.makeCall(SqlStdOperatorTable.OR,
        ImmutableList.copyOf(nodes));
  }

  protected RexNode case_(RexNode... nodes) {
    return case_(ImmutableList.copyOf(nodes));
  }

  protected RexNode case_(Iterable<? extends RexNode> nodes) {
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, ImmutableList.copyOf(nodes));
  }

  /**
   * Creates a call to the CAST operator.
   *
   * <p>This method enables to create {@code CAST(42 nullable int)} expressions.</p>
   *
   * @param e input node
   * @param type type to cast to
   * @return call to CAST operator
   */
  protected RexNode abstractCast(RexNode e, RelDataType type) {
    return rexBuilder.makeAbstractCast(type, e);
  }

  /**
   * Creates a call to the CAST operator, expanding if possible, and not
   * preserving nullability.
   *
   * <p>Tries to expand the cast, and therefore the result may be something
   * other than a {@link RexCall} to the CAST operator, such as a
   * {@link RexLiteral}.</p>

   * @param e input node
   * @param type type to cast to
   * @return input node converted to given type
   */
  protected RexNode cast(RexNode e, RelDataType type) {
    return rexBuilder.makeCast(type, e);
  }

  protected RexNode eq(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, n1, n2);
  }

  protected RexNode ne(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, n1, n2);
  }

  protected RexNode le(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, n1, n2);
  }

  protected RexNode lt(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, n1, n2);
  }

  protected RexNode ge(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, n1, n2);
  }

  protected RexNode gt(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, n1, n2);
  }

  protected RexNode plus(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.PLUS, n1, n2);
  }

  protected RexNode mul(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, n1, n2);
  }

  protected RexNode coalesce(RexNode... nodes) {
    return rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, nodes);
  }

  protected RexNode divInt(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE_INTEGER, n1, n2);
  }

  protected RexNode div(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, n1, n2);
  }

  protected RexNode sub(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.MINUS, n1, n2);
  }

  protected RexNode add(RexNode n1, RexNode n2) {
    return rexBuilder.makeCall(SqlStdOperatorTable.PLUS, n1, n2);
  }

  protected RexNode item(RexInputRef inputRef, RexNode literal) {
    RexNode rexNode = rexBuilder.makeCall(
        SqlStdOperatorTable.ITEM,
        inputRef,
        literal);
    return rexNode;
  }

  /**
   * Generates {@code x IN (y, z)} expression when called as {@code in(x, y, z)}.
   * @param node left side of the IN expression
   * @param nodes nodes in the right side of IN expression
   * @return IN expression
   */
  protected RexNode in(RexNode node, RexNode... nodes) {
    return rexBuilder.makeCall(SqlStdOperatorTable.IN,
        ImmutableList.<RexNode>builder().add(node).add(nodes).build());
  }

  // Types
  protected RelDataType nullable(RelDataType type) {
    if (type.isNullable()) {
      return type;
    }
    return typeFactory.createTypeWithNullability(type, true);
  }

  protected RelDataType tVarchar() {
    return nonNullableVarchar;
  }

  protected RelDataType tVarchar(boolean nullable) {
    return nullable ? nullableVarchar : nonNullableVarchar;
  }

  protected RelDataType tBoolean() {
    return nonNullableBool;
  }

  protected RelDataType tBoolean(boolean nullable) {
    return nullable ? nullableBool : nonNullableBool;
  }

  protected RelDataType tInt() {
    return nonNullableInt;
  }

  protected RelDataType tInt(boolean nullable) {
    return nullable ? nullableInt : nonNullableInt;
  }

  // Literals

  /**
   * Creates null literal with given type.
   * For instance: {@code null_(tInt())}
   *
   * @param type type of required null
   * @return null literal of a given type
   */
  protected RexLiteral null_(RelDataType type) {
    return rexBuilder.makeNullLiteral(nullable(type));
  }

  protected RexNode literal(boolean value) {
    return rexBuilder.makeLiteral(value, nonNullableBool, false);
  }

  protected RexNode literal(Boolean value) {
    if (value == null) {
      return rexBuilder.makeNullLiteral(nullableBool);
    }
    return literal(value.booleanValue());
  }

  protected RexNode literal(int value) {
    return rexBuilder.makeLiteral(value, nonNullableInt, false);
  }

  protected RexNode literal(BigDecimal value) {
    return rexBuilder.makeExactLiteral(value);
  }

  protected RexNode literal(BigDecimal value, RelDataType type) {
    return rexBuilder.makeExactLiteral(value, type);
  }

  protected RexNode literal(Integer value) {
    if (value == null) {
      return rexBuilder.makeNullLiteral(nullableInt);
    }
    return literal(value.intValue());
  }

  protected RexNode literal(String value) {
    if (value == null) {
      return rexBuilder.makeNullLiteral(nullableVarchar);
    }
    return rexBuilder.makeLiteral(value, nonNullableVarchar, false);
  }

  // Variables

  /**
   * Generates input ref with given type and index.
   *
   * <p>Prefer {@link #vBool()}, {@link #vInt()} and so on.
   *
   * <p>The problem with "input refs" is {@code input(tInt(), 0).toString()}
   * yields {@code $0}, so the type of the expression is not printed, and it
   * makes it hard to analyze the expressions.
   *
   * @param type desired type of the node
   * @param arg argument index (0-based)
   * @return input ref with given type and index
   */
  protected RexNode input(RelDataType type, int arg) {
    return rexBuilder.makeInputRef(type, arg);
  }

  private void assertArgValue(int arg) {
    assert arg >= 0 && arg < MAX_FIELDS
        : "arg should be in 0.." + (MAX_FIELDS - 1) + " range. Actual value was " + arg;
  }

  /**
   * Creates {@code nullable boolean variable} with index of 0.
   * If you need several distinct variables, use {@link #vBool(int)}
   * @return nullable boolean variable with index of 0
   */
  protected RexNode vBool() {
    return vBool(0);
  }

  /**
   * Creates {@code nullable boolean variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.bool3} if {@code arg} is {@code 3}.
   *
   * @return nullable boolean variable with given index (0-based)
   */
  protected RexNode vBool(int arg) {
    assertArgValue(arg);
    return rexBuilder.makeFieldAccess(getDynamicParam(nonNullableBool, "bool"), arg);
  }

  /**
   * Creates {@code non-nullable boolean variable} with index of 0.
   * If you need several distinct variables, use {@link #vBoolNotNull(int)}.
   * The resulting node would look like {@code ?0.notNullBool0}
   *
   * @return non-nullable boolean variable with index of 0
   */
  protected RexNode vBoolNotNull() {
    return vBoolNotNull(0);
  }

  /**
   * Creates {@code non-nullable boolean variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.notNullBool3} if {@code arg} is {@code 3}.
   *
   * @return non-nullable boolean variable with given index (0-based)
   */
  protected RexNode vBoolNotNull(int arg) {
    assertArgValue(arg);
    return rexBuilder.makeFieldAccess(
        getDynamicParam(nonNullableBool, "bool"),
        arg + MAX_FIELDS);
  }

  /**
   * Creates {@code nullable int variable} with index of 0.
   * If you need several distinct variables, use {@link #vInt(int)}.
   * The resulting node would look like {@code ?0.notNullInt0}
   *
   * @return nullable int variable with index of 0
   */
  protected RexNode vInt() {
    return vInt(0);
  }

  /**
   * Creates {@code nullable int variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.int3} if {@code arg} is {@code 3}.
   *
   * @return nullable int variable with given index (0-based)
   */
  protected RexNode vInt(int arg) {
    assertArgValue(arg);
    return rexBuilder.makeFieldAccess(getDynamicParam(nonNullableInt, "int"), arg);
  }

  /**
   * Creates {@code non-nullable int variable} with index of 0.
   * If you need several distinct variables, use {@link #vIntNotNull(int)}.
   * The resulting node would look like {@code ?0.notNullInt0}
   *
   * @return non-nullable int variable with index of 0
   */
  protected RexNode vIntNotNull() {
    return vIntNotNull(0);
  }

  /**
   * Creates {@code non-nullable int variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.notNullInt3} if {@code arg} is {@code 3}.
   *
   * @return non-nullable int variable with given index (0-based)
   */
  protected RexNode vIntNotNull(int arg) {
    assertArgValue(arg);
    return rexBuilder.makeFieldAccess(
        getDynamicParam(nonNullableInt, "int"),
        arg + MAX_FIELDS);
  }

  /**
   * Creates {@code nullable varchar variable} with index of 0.
   * If you need several distinct variables, use {@link #vVarchar(int)}.
   * The resulting node would look like {@code ?0.notNullVarchar0}
   *
   * @return nullable varchar variable with index of 0
   */
  protected RexNode vVarchar() {
    return vVarchar(0);
  }

  /**
   * Creates {@code nullable varchar variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.varchar3} if {@code arg} is {@code 3}.
   *
   * @return nullable varchar variable with given index (0-based)
   */
  protected RexNode vVarchar(int arg) {
    assertArgValue(arg);
    return rexBuilder.makeFieldAccess(
        getDynamicParam(nonNullableVarchar, "varchar"), arg);
  }

  /**
   * Creates {@code non-nullable varchar variable} with index of 0.
   * If you need several distinct variables, use {@link #vVarcharNotNull(int)}.
   * The resulting node would look like {@code ?0.notNullVarchar0}
   *
   * @return non-nullable varchar variable with index of 0
   */
  protected RexNode vVarcharNotNull() {
    return vVarcharNotNull(0);
  }

  /**
   * Creates {@code non-nullable varchar variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.notNullVarchar3} if {@code arg} is {@code 3}.
   *
   * @return non-nullable varchar variable with given index (0-based)
   */
  protected RexNode vVarcharNotNull(int arg) {
    assertArgValue(arg);
    return rexBuilder.makeFieldAccess(
        getDynamicParam(nonNullableVarchar, "varchar"),
        arg + MAX_FIELDS);
  }
}

// End RexProgramBuilderBase.java
