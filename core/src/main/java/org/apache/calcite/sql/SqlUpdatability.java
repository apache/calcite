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

import java.util.List;
import java.util.Objects;

/**
 * Represents the updatability clause.
 * SELECT .. FROM ... FOR UPDATE [OF table,table...]
 */
public class SqlUpdatability extends SqlCall {

  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("UPDATABILITY", SqlKind.UPDATABILITY) {
      @Override public SqlCall createCall(SqlLiteral functionQualifier,
            SqlParserPos pos, SqlNode... operands) {
          return new SqlUpdatability((SqlNodeList) operands[0], pos);
        }
  };

  final SqlNodeList tables;

  public SqlUpdatability(SqlNodeList tables, SqlParserPos pos) {
    super(pos);
    this.tables = Objects.requireNonNull(tables != null
              ? tables : new SqlNodeList(pos));
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.sep("FOR UPDATE");
    if (this.tables.size() > 0) {
      writer.sep("OF");
      final SqlWriter.Frame tablesFrame =
          writer.startList(SqlWriter.FrameTypeEnum.SELECT_LIST);
      tables.commaList(writer);
      writer.endList(tablesFrame);
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public SqlKind getKind() {
    return SqlKind.UPDATABILITY;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tables);
  }

}

// End SqlUpdatability.java
