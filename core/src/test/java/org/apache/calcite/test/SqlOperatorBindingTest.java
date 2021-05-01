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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Unit tests for {@link SqlOperatorBinding} and its sub-classes
 * {@link SqlCallBinding} and {@link RexCallBinding}.
 */
class SqlOperatorBindingTest {
  private RexBuilder rexBuilder;
  private RelDataType integerDataType;
  private SqlDataTypeSpec integerType;

  @BeforeEach
  void setUp() {
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    integerDataType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    integerType = SqlTypeUtil.convertTypeToSpec(integerDataType);
    rexBuilder = new RexBuilder(typeFactory);
  }

  /** Tests {@link org.apache.calcite.sql.SqlUtil#isLiteral(SqlNode, boolean)},
   * which was added to enhance Calcite's public API
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1219">[CALCITE-1219]
   * Add a method to SqlOperatorBinding to determine whether operand is a
   * literal</a>.
   */
  @Test void testSqlNodeLiteral() {
    final SqlParserPos pos = SqlParserPos.ZERO;
    final SqlNode zeroLiteral = SqlLiteral.createExactNumeric("0", pos);
    final SqlNode oneLiteral = SqlLiteral.createExactNumeric("1", pos);
    final SqlNode nullLiteral = SqlLiteral.createNull(pos);
    final SqlCharStringLiteral aLiteral = SqlLiteral.createCharString("a", pos);

    final SqlNode castLiteral =
        SqlStdOperatorTable.CAST.createCall(pos, zeroLiteral, integerType);
    final SqlNode castCastLiteral =
        SqlStdOperatorTable.CAST.createCall(pos, castLiteral, integerType);
    final SqlNode mapLiteral =
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR.createCall(pos,
            aLiteral, oneLiteral);
    final SqlNode map2Literal =
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR.createCall(pos,
            aLiteral, castLiteral);
    final SqlNode arrayLiteral =
        SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(pos,
            zeroLiteral, oneLiteral);
    final SqlNode defaultCall = SqlStdOperatorTable.DEFAULT.createCall(pos);

    // SqlLiteral is considered a literal
    assertThat(SqlUtil.isLiteral(zeroLiteral, false), is(true));
    assertThat(SqlUtil.isLiteral(zeroLiteral, true), is(true));
    // NULL literal is considered a literal
    assertThat(SqlUtil.isLiteral(nullLiteral, false), is(true));
    assertThat(SqlUtil.isLiteral(nullLiteral, true), is(true));
    // CAST(SqlLiteral as type) is considered a literal, iff allowCast
    assertThat(SqlUtil.isLiteral(castLiteral, false), is(false));
    assertThat(SqlUtil.isLiteral(castLiteral, true), is(true));
    // CAST(CAST(SqlLiteral as type) as type) is considered a literal,
    // iff allowCast
    assertThat(SqlUtil.isLiteral(castCastLiteral, false), is(false));
    assertThat(SqlUtil.isLiteral(castCastLiteral, true), is(true));
    // MAP['a', 1] and MAP['a', CAST(0 AS INTEGER)] are considered literals,
    // iff allowCast
    assertThat(SqlUtil.isLiteral(mapLiteral, false), is(false));
    assertThat(SqlUtil.isLiteral(mapLiteral, true), is(true));
    assertThat(SqlUtil.isLiteral(map2Literal, false), is(false));
    assertThat(SqlUtil.isLiteral(map2Literal, true), is(true));
    // ARRAY[0, 1] is considered a literal, iff allowCast
    assertThat(SqlUtil.isLiteral(arrayLiteral, false), is(false));
    assertThat(SqlUtil.isLiteral(arrayLiteral, true), is(true));
    // DEFAULT is considered a literal, iff allowCast
    assertThat(SqlUtil.isLiteral(defaultCall, false), is(false));
    assertThat(SqlUtil.isLiteral(defaultCall, true), is(true));
  }

  /** Tests {@link org.apache.calcite.rex.RexUtil#isLiteral(RexNode, boolean)},
   * which was added to enhance Calcite's public API
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1219">[CALCITE-1219]
   * Add a method to SqlOperatorBinding to determine whether operand is a
   * literal</a>.
   */
  @Test void testRexNodeLiteral() {
    final RexNode literal = rexBuilder.makeZeroLiteral(
        integerDataType);

    final RexNode castLiteral = rexBuilder.makeCall(
        integerDataType,
        SqlStdOperatorTable.CAST,
        Lists.newArrayList(literal));

    final RexNode castCastLiteral = rexBuilder.makeCall(
        integerDataType,
        SqlStdOperatorTable.CAST,
        Lists.newArrayList(castLiteral));

    // RexLiteral is considered a literal
    assertThat(RexUtil.isLiteral(literal, true), is(true));
    // CAST(RexLiteral as type) is considered a literal
    assertThat(RexUtil.isLiteral(castLiteral, true), is(true));
    // CAST(CAST(RexLiteral as type) as type) is NOT considered a literal
    assertThat(RexUtil.isLiteral(castCastLiteral, true), is(false));
  }
}
