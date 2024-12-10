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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.RelToSqlConverterUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A <code>SqlDialect</code> implementation for the Presto database.
 */
public class PrestoSqlDialect extends SqlDialect {
  public static final Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(DatabaseProduct.PRESTO)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.UNCHANGED)
      .withNullCollation(NullCollation.LAST);

  public static final SqlDialect DEFAULT = new PrestoSqlDialect(DEFAULT_CONTEXT);

  /**
   * Creates a PrestoSqlDialect.
   */
  public PrestoSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean requiresAliasForFromItems() {
    return true;
  }

  @Override public boolean supportsTimestampPrecision() {
    return false;
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    unparseUsingLimit(writer, offset, fetch);
  }

  /** Unparses offset/fetch using "OFFSET offset LIMIT fetch " syntax. */
  private static void unparseUsingLimit(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    checkArgument(fetch != null || offset != null);
    unparseOffset(writer, offset);
    unparseLimit(writer, fetch);
  }

  @Override public @Nullable SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
  }

  @Override public boolean supportsAggregateFunction(SqlKind kind) {
    switch (kind) {
    case AVG:
    case COUNT:
    case CUBE:
    case SUM:
    case MIN:
    case MAX:
    case ROLLUP:
      return true;
    default:
      break;
    }
    return false;
  }

  @Override public boolean supportsGroupByWithCube() {
    return true;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public boolean supportsGroupByWithRollup() {
    return true;
  }

  @Override public CalendarPolicy getCalendarPolicy() {
    return CalendarPolicy.SHIFT;
  }

  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    return super.getCastSpec(type);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
      RelToSqlConverterUtil.specialOperatorByName("SUBSTR")
          .unparse(writer, call, 0, 0);
    } else if (call.getOperator() == SqlStdOperatorTable.APPROX_COUNT_DISTINCT) {
      RelToSqlConverterUtil.specialOperatorByName("APPROX_DISTINCT")
          .unparse(writer, call, 0, 0);
    } else {
      switch (call.getKind()) {
      case MAP_VALUE_CONSTRUCTOR:
        unparseMapValue(writer, call, leftPrec, rightPrec);
        break;
      case CHAR_LENGTH:
        SqlCall lengthCall = SqlLibraryOperators.LENGTH
            .createCall(SqlParserPos.ZERO, call.getOperandList());
        super.unparseCall(writer, lengthCall, leftPrec, rightPrec);
        break;
      default:
        // Current impl is same with Postgresql.
        PostgresqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
      }
    }
  }

  @Override public void unparseSqlIntervalQualifier(SqlWriter writer,
      SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {
    // Current impl is same with MySQL.
    MysqlSqlDialect.DEFAULT.unparseSqlIntervalQualifier(writer, qualifier, typeSystem);
  }

  /**
   * change map open/close symbol from default [] to ().
   */
  private static void unparseMapValue(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    call = convertMapValueCall(call);
    writer.keyword(call.getOperator().getName());
    final SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode operand : call.getOperandList()) {
      writer.sep(",");
      operand.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }

  /**
   * Converts a Presto MapValue call
   * from {@code MAP['k1', 'v1', 'k2', 'v2']}
   * to {@code MAP[ARRAY['k1', 'k2'], ARRAY['v1', 'v2']]}.
   */
  private static SqlCall convertMapValueCall(SqlCall call) {
    boolean unnestMap = call.operandCount() > 0
        && call.getOperandList().stream().allMatch(operand -> operand instanceof SqlLiteral);
    if (!unnestMap) {
      return call;
    }
    List<SqlNode> keys = new ArrayList<>();
    List<SqlNode> values = new ArrayList<>();
    for (int i = 0; i < call.operandCount(); i++) {
      if (i % 2 == 0) {
        keys.add(call.operand(i));
      } else {
        values.add(call.operand(i));
      }
    }
    SqlParserPos pos = call.getParserPosition();
    return new SqlBasicCall(
        new SqlMapValueConstructor(), ImmutableList.of(
        new SqlBasicCall(new SqlArrayValueConstructor(), keys, pos),
        new SqlBasicCall(new SqlArrayValueConstructor(), values, pos)), pos);
  }
}
