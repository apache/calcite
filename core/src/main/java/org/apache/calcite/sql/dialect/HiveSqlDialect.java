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

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.util.RelToSqlConverterUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.util.RelToSqlConverterUtil.unparseSparkArrayAndMap;

/**
 * A <code>SqlDialect</code> implementation for the Apache Hive database.
 */
public class HiveSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.HIVE)
      .withIdentifierQuoteString("`")
      .withNullCollation(NullCollation.LOW);

  public static final SqlDialect DEFAULT = new HiveSqlDialect(DEFAULT_CONTEXT);

  private final boolean emulateNullDirection;

  /** Creates a HiveSqlDialect. */
  public HiveSqlDialect(Context context) {
    super(context);
    // Since 2.1.0, Hive natively supports "NULLS FIRST" and "NULLS LAST".
    // See https://issues.apache.org/jira/browse/HIVE-12994.
    emulateNullDirection = (context.databaseMajorVersion() < 2)
        || (context.databaseMajorVersion() == 2
            && context.databaseMinorVersion() < 1);
  }

  @Override protected boolean allowsAs() {
    return false;
  }

  @Override public boolean requiresAliasForFromItems() {
    return true;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override public @Nullable SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    if (emulateNullDirection) {
      return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
    }

    return null;
  }

  @Override public RexNode prepareUnparse(RexNode rexNode) {
    return RelToSqlConverterUtil.unparseIsTrueOrFalse(rexNode);
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call,
      final int leftPrec, final int rightPrec) {
    switch (call.getKind()) {
    case ARRAY_VALUE_CONSTRUCTOR:
    case MAP_VALUE_CONSTRUCTOR:
      unparseSparkArrayAndMap(writer, call, leftPrec, rightPrec);
      break;
    case POSITION:
      final SqlWriter.Frame frame = writer.startFunCall("INSTR");
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      if (3 == call.operandCount()) {
        writer.sep(",");
        call.operand(2).unparse(writer, leftPrec, rightPrec);
      }
      if (4 == call.operandCount()) {
        writer.sep(",");
        call.operand(2).unparse(writer, leftPrec, rightPrec);
        writer.sep(",");
        call.operand(3).unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(frame);
      break;
    case MOD:
      SqlOperator op = SqlStdOperatorTable.PERCENT_REMAINDER;
      SqlSyntax.BINARY.unparse(writer, op, call, leftPrec, rightPrec);
      break;
    case TRIM:
      RelToSqlConverterUtil.unparseHiveTrim(writer, call, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public boolean supportsGroupByWithRollup() {
    return true;
  }

  @Override public boolean supportsGroupByWithCube() {
    return true;
  }

  @Override public boolean supportsTimestampPrecision() {
    return false;
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public @Nullable SqlNode getCastSpec(final RelDataType type) {
    if (type instanceof BasicSqlType) {
      switch (type.getSqlTypeName()) {
      case REAL:
        return new SqlDataTypeSpec(
            new SqlAlienSystemTypeNameSpec("FLOAT", type.getSqlTypeName(),
                SqlParserPos.ZERO),
            SqlParserPos.ZERO);
      case INTEGER:
        SqlAlienSystemTypeNameSpec typeNameSpec =
            new SqlAlienSystemTypeNameSpec("INT", type.getSqlTypeName(),
                SqlParserPos.ZERO);
        return new SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO);
      case VARCHAR:
        if (type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED) {
          return new SqlDataTypeSpec(
              new SqlAlienSystemTypeNameSpec("STRING", type.getSqlTypeName(),
                  SqlParserPos.ZERO), SqlParserPos.ZERO);
        }
        break;
      default:
        break;
      }
    }

    if (type instanceof AbstractSqlType) {
      switch (type.getSqlTypeName()) {
      case ARRAY:
      case MAP:
      case MULTISET:
        throw new UnsupportedOperationException("Hive dialect does not support cast to "
            + type.getSqlTypeName());
      default:
        break;
      }
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
   * ELSE ASSERT_TRUE(false)
   * </pre></blockquote>
   *
   * <pre>ASSERT_TRUE(false) will throw assertion failed exception
   * when result includes more than one value.</pre>
   */
  @Override public SqlNode rewriteSingleValueExpr(SqlNode aggCall, RelDataType relDataType) {
    final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
    final SqlLiteral nullLiteral = SqlLiteral.createNull(SqlParserPos.ZERO);
    SqlNodeList sqlNodesList = new SqlNodeList(SqlParserPos.ZERO);
    sqlNodesList.add(SqlLiteral.createBoolean(false, SqlParserPos.ZERO));
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
            RelToSqlConverterUtil.specialOperatorByName("ASSERT_TRUE").createCall(sqlNodesList));
    LOGGER.debug("SINGLE_VALUE rewritten into [{}]", caseExpr);
    return caseExpr;
  }
}
