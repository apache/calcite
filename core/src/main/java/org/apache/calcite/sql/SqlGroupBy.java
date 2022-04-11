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
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree node that represents a {@code GROUP BY} clause within a query.
 */
public class SqlGroupBy extends SqlCall {
  public @Nullable SqlLiteral qualifier;
  public @NonNull SqlNodeList groupList;

  //~ Constructors -----------------------------------------------------------

  public SqlGroupBy(SqlParserPos pos, @Nullable SqlLiteral qualifier, SqlNodeList groupList) {
    super(pos);
    this.qualifier = qualifier;
    this.groupList = requireNonNull(groupList, "groupList");
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      qualifier = (SqlLiteral) operand;
      break;
    case 1:
      groupList = requireNonNull((SqlNodeList) operand);
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public SqlKind getKind() {
    return SqlKind.GROUP_BY;
  }

  @Override public SqlOperator getOperator() {
    return SqlGroupByOperator.INSTANCE;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(qualifier, groupList);
  }

  public final boolean isDistinct() {
    if (qualifier == null) {
      return false;
    }
    return qualifier.symbolValue(SqlSelectKeyword.class) == SqlSelectKeyword.DISTINCT;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.GROUP_BY);
    if (qualifier != null) {
      qualifier.unparse(writer, 0, 0);
    }
    final SqlNodeList groupBy =
        groupList.size() == 0 ? SqlNodeList.SINGLETON_EMPTY
            : groupList;
    writer.list(SqlWriter.FrameTypeEnum.GROUP_BY_LIST, SqlWriter.COMMA,
        groupBy);
    writer.endList(frame);
  }
}
