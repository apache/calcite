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
package org.apache.calcite.rex;

import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;

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
  protected RexLiteral nullSmallInt;
  protected RexLiteral nullVarchar;
  protected RexLiteral nullDecimal;
  protected RexLiteral nullReal;
  protected RexLiteral nullDouble;
  protected RexLiteral nullVarbinary;

  private RelDataType nullableBool;
  private RelDataType nonNullableBool;

  private RelDataType nullableSmallInt;
  private RelDataType nonNullableSmallInt;

  private RelDataType nullableInt;
  private RelDataType nonNullableInt;

  private RelDataType nullableVarchar;
  private RelDataType nonNullableVarchar;

  private RelDataType nullableDecimal;
  private RelDataType nonNullableDecimal;

  private RelDataType nullableReal;
  private RelDataType nonNullableReal;

  private RelDataType nullableDouble;
  private RelDataType nonNullableDouble;

  private RelDataType nullableVarbinary;
  private RelDataType nonNullableVarbinary;

  // Note: JUnit 4 creates new instance for each test method,
  // so we initialize these structures on demand
  // It maps non-nullable type to struct of (10 nullable, 10 non-nullable) fields
  private Map<RelDataType, RexDynamicParam> dynamicParams;

  @BeforeEach public void setUp() {
    typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    final DataContext dataContext =
        DataContexts.of(
            ImmutableMap.of(DataContext.Variable.TIME_ZONE.camelName,
                TimeZone.getTimeZone("America/Los_Angeles"),
                DataContext.Variable.CURRENT_TIMESTAMP.camelName,
                1311120000000L));
    executor = new RexExecutorImpl(dataContext);
    simplify =
        new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor)
            .withParanoid(true);
    trueLiteral = rexBuilder.makeLiteral(true);
    falseLiteral = rexBuilder.makeLiteral(false);

    nonNullableInt = typeFactory.createSqlType(SqlTypeName.INTEGER);
    nullableInt = typeFactory.createTypeWithNullability(nonNullableInt, true);
    nullInt = rexBuilder.makeNullLiteral(nullableInt);

    nonNullableSmallInt = typeFactory.createSqlType(SqlTypeName.SMALLINT);
    nullableSmallInt = typeFactory.createTypeWithNullability(nonNullableSmallInt, true);
    nullSmallInt = rexBuilder.makeNullLiteral(nullableSmallInt);

    nonNullableBool = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    nullableBool = typeFactory.createTypeWithNullability(nonNullableBool, true);
    nullBool = rexBuilder.makeNullLiteral(nullableBool);

    nonNullableVarchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    nullableVarchar = typeFactory.createTypeWithNullability(nonNullableVarchar, true);
    nullVarchar = rexBuilder.makeNullLiteral(nullableVarchar);

    nonNullableDecimal = typeFactory.createSqlType(SqlTypeName.DECIMAL);
    nullableDecimal = typeFactory.createTypeWithNullability(nonNullableDecimal, true);
    nullDecimal = rexBuilder.makeNullLiteral(nullableDecimal);

    nonNullableReal = typeFactory.createSqlType(SqlTypeName.REAL);
    nullableReal = typeFactory.createTypeWithNullability(nonNullableReal, true);
    nullReal = rexBuilder.makeNullLiteral(nullableReal);

    nonNullableDouble = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    nullableDouble = typeFactory.createTypeWithNullability(nonNullableDouble, true);
    nullDouble = rexBuilder.makeNullLiteral(nullableDouble);

    nonNullableVarbinary = typeFactory.createSqlType(SqlTypeName.VARBINARY);
    nullableVarbinary = typeFactory.createTypeWithNullability(nonNullableVarbinary, true);
    nullVarbinary = rexBuilder.makeNullLiteral(nullableVarbinary);
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

  // Operators

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
    assert node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_FALSE, node);
  }

  protected RexNode isNotFalse(RexNode node) {
    assert node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_FALSE, node);
  }

  protected RexNode isTrue(RexNode node) {
    assert node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
    return rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, node);
  }

  protected RexNode isNotTrue(RexNode node) {
    assert node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
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
   * <p>This method enables to create {@code CAST(42 nullable int)} expressions.
   *
   * @param e input node
   * @param type type to cast to
   * @return call to CAST operator
   */
  protected RexNode abstractCast(RexNode e, RelDataType type) {
    return rexBuilder.makeAbstractCast(type, e, false);
  }

  /**
   * Creates a call to the CAST operator, expanding if possible, and not
   * preserving nullability.
   *
   * <p>Tries to expand the cast, and therefore the result may be something
   * other than a {@link RexCall} to the CAST operator, such as a
   * {@link RexLiteral}.
   *
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

  protected RexNode like(RexNode ref, RexNode pattern) {
    return rexBuilder.makeCall(SqlStdOperatorTable.LIKE, ref, pattern);
  }

  protected RexNode similar(RexNode ref, RexNode pattern) {
    return rexBuilder.makeCall(SqlStdOperatorTable.SIMILAR_TO, ref, pattern);
  }

  protected RexNode like(RexNode ref, RexNode pattern, RexNode escape) {
    return rexBuilder.makeCall(SqlStdOperatorTable.LIKE, ref, pattern, escape);
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
  protected RexNode greatest(RexNode... nodes) {
    return rexBuilder.makeCall(SqlLibraryOperators.GREATEST, nodes);
  }

  protected RexNode least(RexNode... nodes) {
    return rexBuilder.makeCall(SqlLibraryOperators.LEAST, nodes);
  }

  protected RexNode m2v(RexNode n) {
    return rexBuilder.makeCall(SqlInternalOperators.M2V, n);
  }

  protected RexNode v2m(RexNode n) {
    return rexBuilder.makeCall(SqlInternalOperators.V2M, n);
  }

  protected RexNode item(RexNode inputRef, RexNode literal) {
    return rexBuilder.makeCall(SqlStdOperatorTable.ITEM, inputRef, literal);
  }

  /**
   * Generates {@code x IN (y, z)} expression when called as
   * {@code in(x, y, z)}.
   *
   * @param node left side of the IN expression
   * @param nodes nodes in the right side of IN expression
   * @return IN expression
   */
  protected RexNode in(RexNode node, RexNode... nodes) {
    return rexBuilder.makeIn(node, ImmutableList.copyOf(nodes));
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

  protected RelDataType tVarchar(int precision) {
    return tVarchar(false, precision);
  }

  protected RelDataType tVarchar(boolean nullable, int precision) {
    RelDataType sqlType = typeFactory.createSqlType(SqlTypeName.VARCHAR, precision);
    if (nullable) {
      sqlType = typeFactory.createTypeWithNullability(sqlType, true);
    }
    return sqlType;
  }

  protected RelDataType tChar(int precision) {
    return tChar(false, precision);
  }

  protected RelDataType tChar(boolean nullable, int precision) {
    RelDataType sqlType = typeFactory.createSqlType(SqlTypeName.CHAR, precision);
    if (nullable) {
      sqlType = typeFactory.createTypeWithNullability(sqlType, true);
    }
    return sqlType;
  }

  protected RelDataType tBool() {
    return nonNullableBool;
  }

  protected RelDataType tBool(boolean nullable) {
    return nullable ? nullableBool : nonNullableBool;
  }

  protected RelDataType tInt() {
    return nonNullableInt;
  }

  protected RelDataType tInt(boolean nullable) {
    return nullable ? nullableInt : nonNullableInt;
  }

  protected RelDataType tSmallInt() {
    return nonNullableSmallInt;
  }

  protected RelDataType tSmallInt(boolean nullable) {
    return nullable ? nullableSmallInt : nonNullableSmallInt;
  }

  protected RelDataType tDecimal() {
    return tDecimal(false);
  }

  protected RelDataType tDecimal(boolean nullable) {
    return nullable ? nullableDecimal : nonNullableDecimal;
  }

  protected RelDataType tReal() {
    return tReal(false);
  }

  protected RelDataType tReal(boolean nullable) {
    return nullable ? nullableReal : nonNullableReal;
  }

  protected RelDataType tDouble() {
    return tDouble(false);
  }

  protected RelDataType tDouble(boolean nullable) {
    return nullable ? nullableDouble : nonNullableDouble;
  }

  protected RelDataType tBigInt() {
    return tBigInt(false);
  }

  protected RelDataType tBigInt(boolean nullable) {
    RelDataType type = typeFactory.createSqlType(SqlTypeName.BIGINT);
    if (nullable) {
      type = nullable(type);
    }
    return type;
  }

  protected RelDataType tVarbinary() {
    return nonNullableVarbinary;
  }

  protected RelDataType tVarbinary(boolean nullable) {
    return nullable ? nullableVarbinary : nonNullableVarbinary;
  }


  protected RelDataType tArray(RelDataType elemType) {
    return typeFactory.createArrayType(elemType, -1);
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

  protected RexLiteral literal(boolean value) {
    return rexBuilder.makeLiteral(value, nonNullableBool);
  }

  protected RexLiteral literal(Boolean value) {
    if (value == null) {
      return rexBuilder.makeNullLiteral(nullableBool);
    }
    return literal(value.booleanValue());
  }

  protected RexLiteral literal(int value) {
    return rexBuilder.makeLiteral(value, nonNullableInt);
  }

  protected RexLiteral literal(BigDecimal value) {
    return rexBuilder.makeExactLiteral(value);
  }

  protected RexLiteral literal(BigDecimal value, RelDataType type) {
    return rexBuilder.makeExactLiteral(value, type);
  }

  protected RexLiteral literal(Integer value) {
    if (value == null) {
      return rexBuilder.makeNullLiteral(nullableInt);
    }
    return literal(value.intValue());
  }

  protected RexLiteral literal(String value) {
    if (value == null) {
      return rexBuilder.makeNullLiteral(nullableVarchar);
    }
    return rexBuilder.makeLiteral(value, nonNullableVarchar);
  }

  protected RexLiteral literal(double value) {
    return rexBuilder.makeApproxLiteral(value, nonNullableDouble);
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
   *
   * @return nullable boolean variable with index of 0
   */
  protected RexNode vBool() {
    return vBool(0);
  }

  /**
   * Creates {@code nullable boolean variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.bool3} if {@code arg} is {@code 3}.
   *
   * @param arg argument index (0-based)
   * @return nullable boolean variable with given index (0-based)
   */
  protected RexNode vBool(int arg) {
    return vParam("bool", arg, nullableBool);
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
   * @param arg argument index (0-based)
   * @return non-nullable boolean variable with given index (0-based)
   */
  protected RexNode vBoolNotNull(int arg) {
    return vParamNotNull("bool", arg, nonNullableBool);
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
   * @param arg argument index (0-based)
   * @return nullable int variable with given index (0-based)
   */
  protected RexNode vInt(int arg) {
    return vParam("int", arg, nullableInt);
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
   * @param arg argument index (0-based)
   * @return non-nullable int variable with given index (0-based)
   */
  protected RexNode vIntNotNull(int arg) {
    return vParamNotNull("int", arg, nonNullableInt);
  }

  /**
   * Creates {@code nullable int variable} with index of 0.
   * If you need several distinct variables, use {@link #vSmallInt(int)}.
   * The resulting node would look like {@code ?0.notNullSmallInt0}
   *
   * @return nullable smallint variable with index of 0
   */
  protected RexNode vSmallInt() {
    return vSmallInt(0);
  }

  /**
   * Creates {@code nullable int variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.int3} if {@code arg} is {@code 3}.
   *
   * @param arg argument index (0-based)
   * @return nullable smallint variable with given index (0-based)
   */
  protected RexNode vSmallInt(int arg) {
    return vParam("smallint", arg, nullableSmallInt);
  }

  /**
   * Creates {@code non-nullable int variable} with index of 0.
   * If you need several distinct variables, use {@link #vSmallIntNotNull(int)}.
   * The resulting node would look like {@code ?0.notNullSmallInt0}
   *
   * @return non-nullable smallint variable with index of 0
   */
  protected RexNode vSmallIntNotNull() {
    return vSmallIntNotNull(0);
  }

  /**
   * Creates {@code non-nullable int variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.notNullSmallInt3} if {@code arg} is {@code 3}.
   *
   * @param arg argument index (0-based)
   * @return non-nullable smallint variable with given index (0-based)
   */
  protected RexNode vSmallIntNotNull(int arg) {
    return vParamNotNull("smallint", arg, nonNullableSmallInt);
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
   * @param arg argument index (0-based)
   * @return nullable varchar variable with given index (0-based)
   */
  protected RexNode vVarchar(int arg) {
    return vParam("varchar", arg, nullableVarchar);
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
   * @param arg argument index (0-based)
   * @return non-nullable varchar variable with given index (0-based)
   */
  protected RexNode vVarcharNotNull(int arg) {
    return vParamNotNull("varchar", arg, nonNullableVarchar);
  }

  /**
   * Creates {@code nullable decimal variable} with index of 0.
   * If you need several distinct variables, use {@link #vDecimal(int)}.
   * The resulting node would look like {@code ?0.notNullDecimal0}
   *
   * @return nullable decimal with index of 0
   */
  protected RexNode vDecimal() {
    return vDecimal(0);
  }

  /**
   * Creates {@code nullable decimal variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.decimal3} if {@code arg} is {@code 3}.
   *
   * @param arg argument index (0-based)
   * @return nullable decimal variable with given index (0-based)
   */
  protected RexNode vDecimal(int arg) {
    return vParam("decimal", arg, nullableDecimal);
  }

  /**
   * Creates {@code non-nullable decimal variable} with index of 0.
   * If you need several distinct variables, use {@link #vDecimalNotNull(int)}.
   * The resulting node would look like {@code ?0.notNullDecimal0}
   *
   * @return non-nullable decimal variable with index of 0
   */
  protected RexNode vDecimalNotNull() {
    return vDecimalNotNull(0);
  }

  /**
   * Creates {@code non-nullable decimal variable} with index of {@code arg} (0-based).
   * The resulting node would look like {@code ?0.notNullDecimal3} if {@code arg} is {@code 3}.
   *
   * @param arg argument index (0-based)
   * @return non-nullable decimal variable with given index (0-based)
   */
  protected RexNode vDecimalNotNull(int arg) {
    return vParamNotNull("decimal", arg, nonNullableDecimal);
  }

  /**
   * Creates {@code nullable variable} with given type and name of {@code arg} (0-based).
   * This enables cases when type is built dynamically.
   * For instance {@code vParam("char(2)_", tChar(2))} would generate a nullable
   * char(2) variable that would look like {@code ?0.char(2)_0}.
   * If you need multiple variables of that kind, use {@link #vParam(String, int, RelDataType)}.
   *
   * @param name variable name prefix
   * @return nullable variable of a given type
   */
  protected RexNode vParam(String name, RelDataType type) {
    return vParam(name, 0, type);
  }

  /**
   * Creates {@code nullable variable} with given type and name with index of {@code arg} (0-based).
   * This enables cases when type is built dynamically.
   * For instance {@code vParam("char(2)_", 3, tChar(2))} would generate a nullable
   * char(2) variable that would look like {@code ?0.char(2)_3}.
   *
   * @param name variable name prefix
   * @param arg argument index (0-based)
   * @return nullable varchar variable with given index (0-based)
   */
  protected RexNode vParam(String name, int arg, RelDataType type) {
    assertArgValue(arg);
    RelDataType nonNullableType = typeFactory.createTypeWithNullability(type, false);
    return rexBuilder.makeFieldAccess(getDynamicParam(nonNullableType, name), arg);
  }

  /**
   * Creates {@code non-nullable variable} with given type and name.
   * This enables cases when type is built dynamically.
   * For instance {@code vParam("char(2)_", tChar(2))} would generate a non-nullable
   * char(2) variable that would look like {@code ?0.char(2)_0}.
   * If you need multiple variables of that kind, use
   * {@link #vParamNotNull(String, int, RelDataType)}
   *
   * @param name variable name prefix
   * @return non-nullable variable of a given type
   */
  protected RexNode vParamNotNull(String name, RelDataType type) {
    return vParamNotNull(name, 0, type);
  }

  /**
   * Creates {@code non-nullable variable} with given type and name with index of
   * {@code arg} (0-based).
   * This enables cases when type is built dynamically.
   * For instance {@code vParam("char(2)_", 3, tChar(2))} would generate a non-nullable
   * char(2) variable that would look like {@code ?0.char(2)_3}.
   *
   * @param name variable name prefix
   * @param arg argument index (0-based)
   * @return non-nullable varchar variable with given index (0-based)
   */
  protected RexNode vParamNotNull(String name, int arg, RelDataType type) {
    assertArgValue(arg);
    RelDataType nonNullableType = typeFactory.createTypeWithNullability(type, false);
    return rexBuilder.makeFieldAccess(getDynamicParam(nonNullableType, name), arg + MAX_FIELDS);
  }
}
