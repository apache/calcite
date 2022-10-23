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
 * A <code>SqlInsertOverwriteDirectory</code> is a node of a parse tree which represents an
 * INSERT OVERWRITE DIRECTORY statement.
 */
public class SqlInsertOverwriteDir extends SqlInsertOverwrite {
  public static final SqlSpecialOperator OPERATOR =
        new SqlSpecialOperator("INSERT OVERWRITE DIRECTORY", SqlKind.INSERT_OVERWRITE);
  private boolean isLocal;
  private SqlNode format;
  private SqlNodeList optionList;

  public SqlInsertOverwriteDir(SqlParserPos pos,
      boolean isLocal,
      SqlNode target,
      SqlNode format,
      SqlNodeList optionList,
      SqlNode source) {
    super(OPERATOR, target, source, pos);
    this.isLocal = isLocal;
    this.format = format;
    this.optionList = optionList;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    if (isLocal) {
      writer.keyword("INSERT OVERWRITE LOCAL DIRECTORY");
    } else {
      writer.keyword("INSERT OVERWRITE DIRECTORY");
    }
    getTarget().unparse(writer, leftPrec, rightPrec);
    writer.newlineAndIndent();

    writer.keyword("USING");
    format.unparse(writer, leftPrec, rightPrec);
    writer.newlineAndIndent();

    if (!optionList.isEmpty()) {
      writer.keyword("OPTIONS");
      optionList.unparse(writer, leftPrec, rightPrec);
      writer.newlineAndIndent();
    }

    getSource().unparse(writer, leftPrec, rightPrec);
  }
}
