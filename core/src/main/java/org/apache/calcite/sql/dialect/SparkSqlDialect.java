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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.RelToSqlConverterUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.util.RelToSqlConverterUtil.unparseHiveTrim;
import static org.apache.calcite.util.RelToSqlConverterUtil.unparseSparkArrayAndMap;

/**
 * A <code>SqlDialect</code> implementation for the APACHE SPARK database.
 */
public class SparkSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.SPARK)
      .withIdentifierQuoteString("`")
      .withNullCollation(NullCollation.LOW);

  public static final SqlDialect DEFAULT = new SparkSqlDialect(DEFAULT_CONTEXT);

  /**
   * Creates a SparkSqlDialect.
   */
  public SparkSqlDialect(SqlDialect.Context context) {
    super(context);
  }

  @Override protected boolean allowsAs() {
    return false;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public JoinType emulateJoinTypeForCrossJoin() {
    return JoinType.CROSS;
  }

  @Override public boolean supportsGroupByWithRollup() {
    return true;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case AVG:
    case COUNT:
    case CUBE:
    case SUM:
    case SUM0:
    case MIN:
    case MAX:
    case ROLLUP:
      return true;
    default:
      break;
    }
    return false;
  }

  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }

  @Override public boolean supportsGroupByWithCube() {
    return true;
  }

  @Override public boolean supportsTimestampPrecision() {
    return false;
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case ARRAY_VALUE_CONSTRUCTOR:
    case MAP_VALUE_CONSTRUCTOR:
      unparseSparkArrayAndMap(writer, call, leftPrec, rightPrec);
      break;
    case STARTS_WITH:
      SqlCall starsWithCall = SqlLibraryOperators.STARTSWITH
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, starsWithCall, leftPrec, rightPrec);
      break;
    case ENDS_WITH:
      SqlCall endsWithCall = SqlLibraryOperators.ENDSWITH
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, endsWithCall, leftPrec, rightPrec);
      break;
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }

      final SqlLiteral timeUnitNode = call.operand(1);
      final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

      SqlCall call2 =
          SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
              timeUnitNode.getParserPosition());
      SqlFloorFunction.unparseDatetimeFunction(writer, call2, "DATE_TRUNC", false);
      break;
    case TRIM:
      unparseHiveTrim(writer, call, leftPrec, rightPrec);
      break;
    case POSITION:
      SqlUtil.unparseFunctionSyntax(SqlStdOperatorTable.POSITION, writer, call, false);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case VARCHAR:
      return new SqlDataTypeSpec(
          new SqlAlienSystemTypeNameSpec("STRING", type.getSqlTypeName(),
              SqlParserPos.ZERO), SqlParserPos.ZERO);
    case ARRAY:
      return RelToSqlConverterUtil.getCastSpecAngleBracketArrayType(this, type,
          SqlParserPos.ZERO);
    case MULTISET:
      throw new UnsupportedOperationException("Spark dialect does not support cast to "
          + type.getSqlTypeName());
    default:
      break;
    }
    return super.getCastSpec(type);
  }

  /**
   * Rewrite SINGLE_VALUE(result).
   *
   * <blockquote><pre>
   * CASE COUNT(*)
   * WHEN 0 THEN NULL
   * WHEN 1 THEN MIN(&lt;result&gt;)
   * ELSE RAISE_ERROR("more than one value in agg SINGLE_VALUE")
   * </pre></blockquote>
   *
   * <pre>RAISE_ERROR("more than one value in agg SINGLE_VALUE") will throw exception
   * when result includes more than one value.</pre>
   */
  @Override public SqlNode rewriteSingleValueExpr(SqlNode aggCall, RelDataType relDataType) {
    final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
    final SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlNodeList sqlNodesList = new SqlNodeList(SqlParserPos.ZERO);
    sqlNodesList.add(
        SqlLiteral.createCharString("more than one value in agg SINGLE_VALUE", SqlParserPos.ZERO));
    final SqlNode caseExpr =
        new SqlCase(SqlParserPos.ZERO,
            SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO,
                ImmutableList.of(SqlIdentifier.STAR)),
            SqlNodeList.of(
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)),
            SqlNodeList.of(
                nullLiteral,
                SqlStdOperatorTable.MIN.createCall(SqlParserPos.ZERO, operand)),
            RelToSqlConverterUtil.specialOperatorByName("RAISE_ERROR").createCall(sqlNodesList));
    LOGGER.debug("SINGLE_VALUE rewritten into [{}]", caseExpr);
    return caseExpr;
  }

}
