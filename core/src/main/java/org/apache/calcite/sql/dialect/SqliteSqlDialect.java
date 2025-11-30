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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.RelToSqlConverterUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A <code>SqliteSqlDialect</code> implementation for the SQLite database.
 */
public class SqliteSqlDialect extends SqlDialect {

  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.DUCKDB)
      .withIdentifierQuoteString("\"")
      // Refer to document: https://sqlite.org/datatype3.html
      .withNullCollation(NullCollation.LOW);

  public static final SqlDialect DEFAULT = new SqliteSqlDialect(DEFAULT_CONTEXT);

  private final int majorVersion;
  private final int minorVersion;

  /** Creates a SqliteSqlDialect. */
  public SqliteSqlDialect(SqlDialect.Context context) {
    super(context);
    this.majorVersion = context.databaseMajorVersion();
    this.minorVersion = context.databaseMinorVersion();
  }

  @Override public boolean supportsJoinType(JoinRelType joinType) {
    // Unknown version means we conservatively assume support for no join types
    if (majorVersion < 0) {
      return false;
    }

    // For non-RIGHT/FULL join types, SQLite supports them in any version
    // For RIGHT/FULL joins, SQLite added support in 3.39.0
    // See: https://www.sqlite.org/releaselog/3_39_0.html
    return (joinType != JoinRelType.RIGHT && joinType != JoinRelType.FULL)
        || (majorVersion > 3 || (majorVersion == 3 && minorVersion >= 39));
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
      @Nullable SqlNode fetch) {
    unparseFetchUsingLimit(writer, offset, fetch);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case CHAR_LENGTH:
      SqlCall lengthCall = SqlLibraryOperators.LENGTH
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, lengthCall, leftPrec, rightPrec);
      break;
    case TRIM:
      RelToSqlConverterUtil.unparseTrimLR(writer, call, leftPrec, rightPrec);
      break;
    case POSITION:
      final SqlWriter.Frame frame = writer.startFunCall("INSTR");
      writer.sep(",");
      call.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(",");
      call.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.endFunCall(frame);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

}
