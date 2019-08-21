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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/**
 * A <code>SqlDialect</code> implementation for Google BigQuery's "Standard SQL"
 * dialect.
 */
public class BigQuerySqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new BigQuerySqlDialect(
          EMPTY_CONTEXT
              .withDatabaseProduct(SqlDialect.DatabaseProduct.BIG_QUERY)
              .withNullCollation(NullCollation.LOW)
              .withSqlConformance(SqlConformanceEnum.BIG_QUERY));

  /** Creates a BigQuerySqlDialect. */
  public BigQuerySqlDialect(SqlDialect.Context context) {
    super(context);
  }

  @Override public boolean supportsNestedAggregations() {
    return false;
  }

  @Override public boolean supportsColumnAliasInSort() {
    return true;
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public SqlOperator getTargetFunc(RexCall call) {
    switch (call.type.getSqlTypeName()) {
    case DATE:
      switch (call.getOperands().get(1).getType().getSqlTypeName()) {
      case INTERVAL_DAY:
      case INTERVAL_MONTH:
        return SqlLibraryOperators.DATE_ADD;
      }
    default:
      return super.getTargetFunc(call);
    }
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec,
      final int rightPrec) {

    switch (call.getKind()) {
    case POSITION:
      final SqlWriter.Frame frame = writer.startFunCall("STRPOS");
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      if (3 == call.operandCount()) {
        throw new RuntimeException("3rd operand Not Supported for Function STRPOS in Big Query");
      }
      writer.endFunCall(frame);
      break;
    case UNION:
      if (!((SqlSetOperator) call.getOperator()).isAll()) {
        SqlSyntax.BINARY.unparse(writer, UNION_DISTINCT, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case EXCEPT:
      if (!((SqlSetOperator) call.getOperator()).isAll()) {
        SqlSyntax.BINARY.unparse(writer, EXCEPT_DISTINCT, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case INTERSECT:
      if (!((SqlSetOperator) call.getOperator()).isAll()) {
        SqlSyntax.BINARY.unparse(writer, INTERSECT_DISTINCT, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    case CHAR_LENGTH:
    case CHARACTER_LENGTH:
      final SqlWriter.Frame lengthFrame = writer.startFunCall("LENGTH");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(lengthFrame);
      break;
    case SUBSTRING:
      final SqlWriter.Frame substringFrame = writer.startFunCall("SUBSTR");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(substringFrame);
      break;
    case TRUNCATE:
      final SqlWriter.Frame truncateFrame = writer.startFunCall("TRUNC");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(truncateFrame);
      break;
    case CONCAT:
      final SqlWriter.Frame concatFrame = writer.startFunCall("CONCAT");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(concatFrame);
      break;
    case DIVIDE_INTEGER:
      final SqlWriter.Frame castFrame = writer.startFunCall("CAST");
      unparseDivideInteger(writer, call, leftPrec, rightPrec);
      writer.sep("AS");
      writer.literal("INT64");
      writer.endFunCall(castFrame);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  @Override public SqlNode emulateNullDirection(SqlNode node,
      boolean nullsFirst, boolean desc) {
    return emulateNullDirectionWithIsNull(node, nullsFirst, desc);
  }

  /**
   * List of BigQuery Specific Operators needed to form Syntactically Correct SQL.
   */
  private static final SqlOperator UNION_DISTINCT = new SqlSetOperator(
      "UNION DISTINCT", SqlKind.UNION, 14, false);

  private static final SqlSetOperator EXCEPT_DISTINCT =
      new SqlSetOperator("EXCEPT DISTINCT", SqlKind.EXCEPT, 14, false);

  private static final SqlSetOperator INTERSECT_DISTINCT =
      new SqlSetOperator("INTERSECT DISTINCT", SqlKind.INTERSECT, 18, false);

  @Override public void unparseSqlDatetimeArithmetic(SqlWriter writer,
      SqlCall call, SqlKind sqlKind, int leftPrec, int rightPrec) {
    switch (sqlKind) {
    case MINUS:
      final SqlWriter.Frame dateDiffFrame = writer.startFunCall("DATE_DIFF");
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      writer.literal("DAY");
      writer.endFunCall(dateDiffFrame);
      break;
    }
  }

  @Override public void unparseIntervalOperandsBasedFunctions(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    super.unparseIntervalOperandsBasedFunctions(writer, call, leftPrec, rightPrec);
  }
}

// End BigQuerySqlDialect.java
