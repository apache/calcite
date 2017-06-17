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
package org.apache.calcite.sql.parser.parserextensiontesting;

import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * Simple test example of a custom alter system call.
 */
public class SqlUploadJarNode extends SqlAlter {
  public static final SqlOperator OPERATOR =
      new SqlSpecialOperator("UPLOAD JAR", SqlKind.OTHER_DDL);

  private final List<SqlNode> jarPaths;

  /** Creates a SqlUploadJarNode. */
  public SqlUploadJarNode(SqlParserPos pos, String scope, List<SqlNode> jarPaths) {
    super(pos, scope);
    this.jarPaths = jarPaths;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return jarPaths;
  }

  @Override protected void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("UPLOAD");
    writer.keyword("JAR");
    SqlWriter.Frame frame = writer.startList("", "");
    for (SqlNode jarPath : jarPaths) {
      jarPath.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }
}

// End SqlUploadJarNode.java
