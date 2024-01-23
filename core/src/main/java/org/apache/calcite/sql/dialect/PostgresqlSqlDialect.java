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
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * A <code>SqlDialect</code> implementation for the PostgreSQL database.
 */
public class PostgresqlSqlDialect extends SqlDialect {
  /** PostgreSQL type system. */
  public static final RelDataTypeSystem POSTGRESQL_TYPE_SYSTEM =
      new RelDataTypeSystemImpl() {
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case VARCHAR:
            // From htup_details.h in postgresql:
            // MaxAttrSize is a somewhat arbitrary upper limit on the declared size of
            // data fields of char(n) and similar types.  It need not have anything
            // directly to do with the *actual* upper limit of varlena values, which
            // is currently 1Gb (see TOAST structures in postgres.h).  I've set it
            // at 10Mb which seems like a reasonable number --- tgl 8/6/00. */
            return 10 * 1024 * 1024;
          default:
            return super.getMaxPrecision(typeName);
          }
        }
      };

  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.POSTGRESQL)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.TO_LOWER)
      .withDataTypeSystem(POSTGRESQL_TYPE_SYSTEM);

  public static final SqlDialect DEFAULT = new PostgresqlSqlDialect(DEFAULT_CONTEXT);

  /** Creates a PostgresqlSqlDialect. */
  public PostgresqlSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
    case TINYINT:
      // Postgres has no tinyint (1 byte), so instead cast to smallint (2 bytes)
      castSpec = "smallint";
      break;
    case DOUBLE:
      // Postgres has a double type but it is named differently
      castSpec = "double precision";
      break;
    default:
      return super.getCastSpec(type);
    }

    return new SqlDataTypeSpec(
        new SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName(), SqlParserPos.ZERO),
        SqlParserPos.ZERO);
  }

  @Override public SqlNode rewriteSingleValueExpr(SqlNode aggCall, RelDataType relDataType) {
    final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
    final SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
    final SqlNode unionOperand =
        new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY,
            SqlNodeList.of(
                SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, SqlNodeList.of(nullLiteral),
                    SqlNodeList.of(castNonNull(getCastSpec(relDataType))))), null, null, null, null,
            SqlNodeList.EMPTY, null, null, null, null, SqlNodeList.EMPTY);
    // For PostgreSQL, generate
    //   CASE COUNT(value)
    //   WHEN 0 THEN NULL
    //   WHEN 1 THEN min(value)
    //   ELSE (SELECT CAST(NULL AS valueDataType) UNION ALL SELECT CAST(NULL AS valueDataType))
    //   END
    final SqlNode caseExpr =
        new SqlCase(SqlParserPos.ZERO,
            SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO, operand),
            SqlNodeList.of(
                SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)),
            SqlNodeList.of(
                nullLiteral,
                SqlStdOperatorTable.MIN.createCall(SqlParserPos.ZERO, operand)),
            SqlStdOperatorTable.SCALAR_QUERY.createCall(SqlParserPos.ZERO,
                SqlStdOperatorTable.UNION_ALL
                    .createCall(SqlParserPos.ZERO, unionOperand, unionOperand)));

    LOGGER.debug("SINGLE_VALUE rewritten into [{}]", caseExpr);

    return caseExpr;
  }

  @Override public boolean supportsFunction(SqlOperator operator,
      RelDataType type, final List<RelDataType> paramTypes) {
    switch (operator.kind) {
    case LIKE:
      // introduces support for ILIKE as well
      return true;
    default:
      return super.supportsFunction(operator, type, paramTypes);
    }
  }

  @Override public boolean requiresAliasForFromItems() {
    return true;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    switch (call.getKind()) {
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
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  @Override public SqlNode rewriteMaxMinExpr(SqlNode aggCall, RelDataType relDataType) {
    return rewriteMaxMin(aggCall, relDataType);
  }

  @Override public boolean supportsGroupByLiteral() {
    return false;
  }
}
