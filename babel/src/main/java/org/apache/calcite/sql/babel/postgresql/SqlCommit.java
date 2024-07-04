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
package org.apache.calcite.sql.babel.postgresql;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Parse tree node representing a {@code COMMIT} clause.
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-commit.html">COMMIT specification</a>
 */
public class SqlCommit extends SqlCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("COMMIT", SqlKind.OTHER_FUNCTION, 32, false, ReturnTypes.BOOLEAN, null,
          null) {
        @Override public SqlCall createCall(@Nullable final SqlLiteral functionQualifier,
            final SqlParserPos pos,
            final @Nullable SqlNode... operands) {
          return new SqlCommit(pos, (SqlLiteral) operands[0]);
        }
      };
  private final SqlLiteral chain;

  protected SqlCommit(final SqlParserPos pos, final SqlLiteral chain) {
    super(pos);
    this.chain = chain;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(this.chain);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    writer.keyword("COMMIT");
    if (this.chain.symbolValue(AndChain.class) == AndChain.AND_CHAIN) {
      writer.literal("AND CHAIN");
    }
  }
}
