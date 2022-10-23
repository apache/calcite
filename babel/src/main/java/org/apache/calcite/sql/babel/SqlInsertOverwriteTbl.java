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
package org.apache.calcite.sql.babel;

import org.apache.calcite.sql.SqlInsertOverwrite;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlInsertOverwriteTable</code> is a node of a parse tree which represents an
 * INSERT OVERWRITE TABLE statement.
 */
public class SqlInsertOverwriteTbl extends SqlInsertOverwrite {
  protected static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("INSERT OVERWRITE TABLE", SqlKind.INSERT_OVERWRITE);
  private SqlNodeList partitionList;
  private boolean ifNotExists;
  private SqlNodeList columnList;

  public SqlInsertOverwriteTbl(SqlParserPos pos, SqlNode targetTable,
      SqlNodeList partitionList, boolean ifNotExists, SqlNodeList columnList, SqlNode source) {
    super(
        OPERATOR,
        targetTable,
        source,
        pos);
    this.partitionList = partitionList;
    this.ifNotExists = ifNotExists;
    this.columnList = columnList;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.keyword("INSERT OVERWRITE TABLE");
    getTarget().unparse(writer, leftPrec, rightPrec);
    writer.newlineAndIndent();

    if (!partitionList.isEmpty()) {
      writer.keyword("PARTITION");
      partitionList.unparse(writer, leftPrec, rightPrec);
      if (ifNotExists) {
        writer.keyword("IF NOT EXISTS");
      }
      writer.newlineAndIndent();
    }

    if (!columnList.isEmpty()) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      columnList.unparse(writer, leftPrec, rightPrec);
      writer.endList(frame);
      writer.newlineAndIndent();
    }

    getSource().unparse(writer, leftPrec, rightPrec);
  }
}
