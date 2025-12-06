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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Represents {@code SELECT * EXCLUDE(...)}.
 */
public class SqlStarExclude extends SqlCall {
  public static final SqlOperator OPERATOR =
      new SqlSpecialOperator("SELECT_STAR_EXCLUDE", SqlKind.OTHER) {
        @SuppressWarnings("argument.type.incompatible")
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlStarExclude(
              pos,
              (SqlIdentifier) operands[0],
              (SqlNodeList) operands[1]);
        }
      };

  private final SqlIdentifier starIdentifier;
  private final SqlNodeList excludeList;

  public SqlStarExclude(SqlParserPos pos, SqlIdentifier starIdentifier,
      SqlNodeList excludeList) {
    super(pos);
    this.starIdentifier = requireNonNull(starIdentifier, "starIdentifier");
    this.excludeList = requireNonNull(excludeList, "excludeList");
  }

  public SqlIdentifier getStarIdentifier() {
    return starIdentifier;
  }

  public SqlNodeList getExcludeList() {
    return excludeList;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public SqlKind getKind() {
    return OPERATOR.getKind();
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(starIdentifier, excludeList);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    starIdentifier.unparse(writer, leftPrec, rightPrec);
    writer.sep("EXCLUDE");
    final SqlWriter.Frame frame = writer.startList("(", ")");
    excludeList.unparse(writer, 0, 0);
    writer.endList(frame);
  }
}
