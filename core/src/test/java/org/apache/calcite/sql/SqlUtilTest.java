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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import static java.util.Objects.requireNonNull;

/** Tests for {@link SqlUtil}. */
class SqlUtilTest {
  private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(DEFAULT);

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7845">[CALCITE-7845]
   * Fix UDF/UDTF overload resolution with generic parameter types</a>.*/
  @Test void testConcreteAndAnyOverloads() {
    final SqlFunction string = function(SqlTypeName.VARCHAR);
    final SqlFunction timestamp = function(SqlTypeName.TIMESTAMP);
    final SqlFunction any = function(SqlTypeName.ANY);
    final ImmutableList<SqlFunction> functions = ImmutableList.of(any, string, timestamp);
    for (List<SqlFunction> order : ImmutableList.of(functions, functions.reverse())) {
      assertThat(lookup(order, ImmutableList.of(SqlTypeName.CHAR), null), contains(string));
      assertThat(lookup(order, ImmutableList.of(SqlTypeName.VARCHAR), null), contains(string));
      assertThat(lookup(order, ImmutableList.of(SqlTypeName.TIMESTAMP), null), contains(timestamp));
      assertThat(lookup(order, ImmutableList.of(SqlTypeName.BOOLEAN), null), contains(any));
      assertThat(lookup(order, ImmutableList.of(SqlTypeName.ANY), null), contains(any));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7845">[CALCITE-7845]
   * Fix UDF/UDTF overload resolution with generic parameter types</a>. */
  @Test void testNumericPrecedenceWithAny() {
    final SqlFunction bigint = function(SqlTypeName.BIGINT);
    final SqlFunction decimal = function(SqlTypeName.DECIMAL);
    final SqlFunction any = function(SqlTypeName.ANY);
    final ImmutableList<SqlFunction> functions = ImmutableList.of(any, decimal, bigint);
    for (List<SqlFunction> order : ImmutableList.of(functions, functions.reverse())) {
      assertThat(lookup(order, ImmutableList.of(SqlTypeName.INTEGER), null), contains(bigint));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7845">[CALCITE-7845]
   * Fix UDF/UDTF overload resolution with generic parameter types</a>. */
  @Test void testNamedArgumentsWithAny() {
    final SqlFunction timestamp = function(SqlTypeName.VARCHAR, SqlTypeName.TIMESTAMP);
    final SqlFunction any = function(SqlTypeName.VARCHAR, SqlTypeName.ANY);
    final ImmutableList<SqlFunction> functions = ImmutableList.of(any, timestamp);
    for (List<SqlFunction> order : ImmutableList.of(functions, functions.reverse())) {
      assertThat(
          lookup(order, ImmutableList.of(SqlTypeName.TIMESTAMP, SqlTypeName.CHAR),
          ImmutableList.of("p1", "p0")), contains(timestamp));
      assertThat(lookup(order, ImmutableList.of(SqlTypeName.NULL, SqlTypeName.TIMESTAMP), null),
          contains(timestamp));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7845">[CALCITE-7845]
   * Fix UDF/UDTF overload resolution with generic parameter types</a>. */
  @Test void testNullArgumentHasNoTypePrecedence() {
    final SqlFunction string = function(SqlTypeName.VARCHAR);
    final SqlFunction timestamp = function(SqlTypeName.TIMESTAMP);
    final SqlFunction any = function(SqlTypeName.ANY);
    final ImmutableList<SqlFunction> functions = ImmutableList.of(any, string, timestamp);
    for (List<SqlFunction> order : ImmutableList.of(functions, functions.reverse())) {
      // An untyped NULL does not distinguish otherwise compatible overloads.
      assertThat(lookup(order, ImmutableList.of(SqlTypeName.NULL), null),
          contains(order.toArray()));
    }
  }

  private static SqlFunction function(SqlTypeName... types) {
    final List<SqlTypeName> paramTypes = ImmutableList.copyOf(types);
    final List<SqlTypeFamily> families = paramTypes.stream()
        .map(type -> requireNonNull(type.getFamily())).collect(Collectors.toList());
    final SqlOperandMetadata metadata =
        OperandTypes.operandMetadata(families,
            factory -> paramTypes.stream().map(factory::createSqlType).collect(Collectors.toList()),
            i -> "p" + i, i -> false);
    return new SqlFunction("F", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000,
        null, metadata, SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  private List<SqlOperator> lookup(List<SqlFunction> functions, List<SqlTypeName> types,
      @Nullable List<String> names) {
    final List<RelDataType> argTypes = types.stream().map(typeFactory::createSqlType)
        .collect(Collectors.toList());
    return ImmutableList.copyOf(
        SqlUtil.lookupSubjectRoutines(SqlOperatorTables.of(functions), typeFactory,
            new SqlIdentifier("F", SqlParserPos.ZERO), argTypes, names,
            SqlSyntax.FUNCTION, SqlKind.OTHER_FUNCTION, SqlFunctionCategory.USER_DEFINED_FUNCTION,
            SqlNameMatchers.withCaseSensitive(true), false));
  }
}
