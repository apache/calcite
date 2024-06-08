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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableMap;

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

  /* Maps a SqlKind to its Snowflake-specific operator. */
  public static final ImmutableMap<SqlKind, SqlOperator> OPERATOR_MAP;

  static {
    final ImmutableMap.Builder<SqlKind, SqlOperator> builder =
        ImmutableMap.builder();
    builder.put(SqlKind.BIT_AND, SqlLibraryOperators.BITAND_AGG);
    builder.put(SqlKind.BIT_OR, SqlLibraryOperators.BITOR_AGG);
    builder.put(SqlKind.CHAR_LENGTH, SqlLibraryOperators.LENGTH);
    builder.put(SqlKind.ENDS_WITH, SqlLibraryOperators.ENDSWITH);
    builder.put(SqlKind.STARTS_WITH, SqlLibraryOperators.STARTSWITH);
    builder.put(SqlKind.MAX, SqlStdOperatorTable.MAX);
    builder.put(SqlKind.MIN, SqlStdOperatorTable.MIN);
    OPERATOR_MAP = builder.build();
  }

  /** Creates a SnowflakeSqlDialect. */
  public SnowflakeSqlDialect(Context context) {
    super(context);
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec,
      final int rightPrec) {
    SqlOperator op = OPERATOR_MAP.get(call.getKind());
    if (op != null) {
      SqlCall newCall = op.createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, newCall, leftPrec, rightPrec);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  @Override public boolean supportsApproxCountDistinct() {
    return true;
  }
}
