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
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMapTypeNameSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlArrayValueConstructor;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlMapValueConstructor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.RelToSqlConverterUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlDialect</code> implementation for the Presto database.
 */
public class PrestoSqlDialect extends SqlDialect {
  public static final RelDataTypeSystem TYPE_SYSTEM =
      new RelDataTypeSystemImpl() {

        // We can refer to document of Presto 0.29:
        // https://prestodb.io/docs/current/language/types.html#fixed-precision
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 38;
          default:
            return super.getMaxPrecision(typeName);
          }
        }

        @Override public int getMaxScale(SqlTypeName typeName) {
          switch (typeName) {
          case DECIMAL:
            return 38;
          default:
            return super.getMaxScale(typeName);
          }
        }

        @Override public int getMaxNumericScale() {
          return getMaxScale(SqlTypeName.DECIMAL);
        }
      };

  public static final Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(DatabaseProduct.PRESTO)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.UNCHANGED)
      .withNullCollation(NullCollation.LAST)
      .withDataTypeSystem(TYPE_SYSTEM);

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

  @Override public boolean supportsImplicitTypeCoercion(RexCall call) {
    RexNode rexNode = call.getOperands().get(0);
    return super.supportsImplicitTypeCoercion(call)
        && RexUtil.isLiteral(rexNode, false)
        && (rexNode.getType().getSqlTypeName() == SqlTypeName.VARCHAR
        || rexNode.getType().getSqlTypeName() == SqlTypeName.CHAR)
        && !SqlTypeUtil.isNumeric(call.type)
        && !SqlTypeUtil.isDate(call.type)
        && !SqlTypeUtil.isTimestamp(call.type);
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
    switch (type.getSqlTypeName()) {
    // PRESTO only supports REAL、DOUBLE for floating point types.
    case FLOAT:
      return new SqlDataTypeSpec(
          new SqlBasicTypeNameSpec(SqlTypeName.DOUBLE, SqlParserPos.ZERO), SqlParserPos.ZERO);
    // https://prestodb.io/docs/current/language/types.html#varbinary
    case BINARY:
      return new SqlDataTypeSpec(
          new SqlBasicTypeNameSpec(SqlTypeName.VARBINARY, SqlParserPos.ZERO), SqlParserPos.ZERO);
    case MAP:
      MapSqlType mapSqlType = (MapSqlType) type;
      SqlDataTypeSpec keySpec = (SqlDataTypeSpec) getCastSpec(mapSqlType.getKeyType());
      SqlDataTypeSpec valueSpec =
          (SqlDataTypeSpec) getCastSpec(mapSqlType.getValueType());
      SqlDataTypeSpec nonNullkeySpec = requireNonNull(keySpec, "keySpec");
      SqlDataTypeSpec nonNullvalueSpec = requireNonNull(valueSpec, "valueSpec");
      SqlMapTypeNameSpec sqlMapTypeNameSpec =
          new SqlMapTypeNameSpec(nonNullkeySpec, nonNullvalueSpec, SqlParserPos.ZERO);
      return new SqlDataTypeSpec(sqlMapTypeNameSpec,
          SqlParserPos.ZERO);
    case ARRAY:
      ArraySqlType arraySqlType = (ArraySqlType) type;
      SqlDataTypeSpec arrayValueSpec =
          (SqlDataTypeSpec) getCastSpec(arraySqlType.getComponentType());
      SqlDataTypeSpec nonNullarrayValueSpec =
          requireNonNull(arrayValueSpec, "arrayValueSpec");
      SqlCollectionTypeNameSpec sqlArrayTypeNameSpec =
          new SqlCollectionTypeNameSpec(nonNullarrayValueSpec.getTypeNameSpec(),
              SqlTypeName.ARRAY, SqlParserPos.ZERO);
      return new SqlDataTypeSpec(sqlArrayTypeNameSpec,
          SqlParserPos.ZERO);
    case MULTISET:
      throw new UnsupportedOperationException("Presto dialect does not support cast to "
          + type.getSqlTypeName());
    default:
      return super.getCastSpec(type);
    }
  }

  @Override public RexNode prepareUnparse(RexNode rexNode) {
    return RelToSqlConverterUtil.unparseIsTrueOrFalse(rexNode);
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
      case IS_NULL:
      case IS_NOT_NULL:
        if (call.operand(0) instanceof SqlBasicCall) {
          final SqlWriter.Frame frame = writer.startList("(", ")");
          call.operand(0).unparse(writer, leftPrec, rightPrec);
          writer.endList(frame);
          writer.print(call.getOperator().getName() + " ");
        } else {
          super.unparseCall(writer, call, leftPrec, rightPrec);
        }
        break;
      case CHAR_LENGTH:
        SqlCall lengthCall = SqlLibraryOperators.LENGTH
            .createCall(SqlParserPos.ZERO, call.getOperandList());
        super.unparseCall(writer, lengthCall, leftPrec, rightPrec);
        break;
      case TRIM:
        RelToSqlConverterUtil.unparseTrimLR(writer, call, leftPrec, rightPrec);
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
  public static void unparseMapValue(SqlWriter writer, SqlCall call,
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
  public static SqlCall convertMapValueCall(SqlCall call) {
    boolean unnestMap = call.operandCount() > 0
        && (call.getOperandList().get(0) instanceof SqlLiteral
        || call.getOperandList().get(1) instanceof SqlLiteral);
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
