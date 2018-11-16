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
package org.apache.calcite.sql.ddl;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code CREATE FUNCTION} statement.
 */
public class SqlCreateFunction extends SqlCreate {
  private final SqlIdentifier name;
  private final SqlNode className;
  private final SqlNodeList jarList;
  private final SqlNodeList fileList;
  private final SqlNodeList archiveList;
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE FUNCTION", SqlKind.OTHER_DDL);

  /**
   * Creates a SqlCreateFunction.
   */
  public SqlCreateFunction(SqlParserPos pos, boolean replace,
      boolean ifNotExists, SqlIdentifier name,
      SqlNode className, SqlNodeList jarList,
      SqlNodeList fileList, SqlNodeList archiveList) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name);
    this.className = className;
    this.jarList = jarList;
    this.fileList = fileList;
    this.archiveList = archiveList;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec,
      int rightPrec) {
    writer.keyword(getReplace() ? "CREATE OR REPLACE" : "CREATE");
    writer.keyword("FUNCTION");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, 0, 0);
    writer.keyword("AS");
    className.unparse(writer, 0, 0);
    if (jarList != null) {
      writerInfo(writer, "JAR =", jarList);
    }
    if (fileList != null) {
      if (jarList != null) {
        writer.keyword(",");
      }
      writerInfo(writer, "FILE =", fileList);
    }
    if (archiveList != null) {
      if (jarList != null  || fileList != null) {
        writer.keyword(",");
      }
      writerInfo(writer, "ARCHIVE =", fileList);
    }
  }

  private void writerInfo(SqlWriter writer, String s, SqlNodeList fileList) {
    writer.keyword(s);
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode c : fileList) {
      writer.sep(",");
      c.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return Arrays.asList(name, className, jarList);
  }
}

// End SqlCreateFunction.java
