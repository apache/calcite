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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A <code>SqlDialect</code> implementation for the Snowflake database.
 */
public class SnowflakeSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.SNOWFLAKE)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.TO_UPPER);

  public static final SqlDialect DEFAULT =
      new SnowflakeSqlDialect(DEFAULT_CONTEXT);

  /** Creates a SnowflakeSqlDialect. */
  public SnowflakeSqlDialect(Context context) {
    super(context);
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec,
      final int rightPrec) {
    switch (call.getKind()) {
    case BIT_AND:
      SqlCall bitAndCall = SqlLibraryOperators.BITAND_AGG
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, bitAndCall, leftPrec, rightPrec);
      break;
    case BIT_OR:
      SqlCall bitOrCall = SqlLibraryOperators.BITOR_AGG
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, bitOrCall, leftPrec, rightPrec);
      break;
    case CHAR_LENGTH:
      SqlCall lengthCall = SqlLibraryOperators.LENGTH
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, lengthCall, leftPrec, rightPrec);
      break;
    case ENDS_WITH:
      SqlCall endsWithCall = SqlLibraryOperators.ENDSWITH
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, endsWithCall, leftPrec, rightPrec);
      break;
    case STARTS_WITH:
      SqlCall startsWithCall = SqlLibraryOperators.STARTSWITH
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, startsWithCall, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  @Override public SqlNode rewriteMaxMinExpr(SqlNode aggCall, RelDataType relDataType) {
    return rewriteMaxMin(aggCall, relDataType);
  }

  /**
   * Helper for rewrites of MAX/MIN.
   * Snowflake, rewrite as
   * BOOLOR_AGG/BOOLAND_AGG if the return type is BOOLEAN.
   */
  public static SqlNode rewriteMaxMin(SqlNode aggCall, RelDataType relDataType) {
    // The behavior of this method depends on the argument type,
    // and whether it is MIN/MAX
    final SqlTypeName type = relDataType.getSqlTypeName();
    final boolean isMax = aggCall.getKind() == SqlKind.MAX;
    // If the type is BOOLEAN, create a new call to the correct operator
    if (type == SqlTypeName.BOOLEAN) {
      final SqlOperator op =
          isMax ? SqlLibraryOperators.BOOLOR_AGG
              : SqlLibraryOperators.BOOLAND_AGG;
      final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
      return op.createCall(SqlParserPos.ZERO, operand);
    }
    // Otherwise, just return as it arrived
    return aggCall;
  }

  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }
}
