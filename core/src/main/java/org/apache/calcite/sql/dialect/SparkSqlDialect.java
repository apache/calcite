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
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.util.RelToSqlConverterUtil.unparseHiveTrim;

/**
 * A <code>SqlDialect</code> implementation for the APACHE SPARK database.
 */
public class SparkSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.SPARK)
      .withNullCollation(NullCollation.LOW);

  public static final SqlDialect DEFAULT = new SparkSqlDialect(DEFAULT_CONTEXT);

  private static final SqlFunction SPARKSQL_SUBSTRING =
      SqlBasicFunction.create("SUBSTRING", ReturnTypes.ARG0_NULLABLE_VARYING,
          OperandTypes.VARIADIC, SqlFunctionCategory.STRING);

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
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      SqlUtil.unparseFunctionSyntax(SPARKSQL_SUBSTRING, writer, call, false);
    } else {
      switch (call.getKind()) {
      case ARRAY_VALUE_CONSTRUCTOR:
      case MAP_VALUE_CONSTRUCTOR:
        final String keyword =
            call.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR ? "array" : "map";

        writer.keyword(keyword);

        final SqlWriter.Frame frame = writer.startList("(", ")");
        for (SqlNode operand : call.getOperandList()) {
          writer.sep(",");
          operand.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
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
  }
}
