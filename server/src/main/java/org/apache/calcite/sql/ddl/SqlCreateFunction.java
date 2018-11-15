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

/**
 * Parse tree for {@code CREATE FUNCTION} statement.
 */
public class SqlCreateFunction extends SqlCreate {
  private final SqlIdentifier funcName;
  private final SqlNode className;
  private final SqlNodeList jarList;
  private static final SqlSpecialOperator OPERATOR =
          new SqlSpecialOperator("CREATE FUNCTION", SqlKind.OTHER_DDL);

  /**
   * Creates a SqlCreateFunction.
   */
  public SqlCreateFunction(SqlParserPos pos, boolean replace,
                           boolean ifNotExists, SqlIdentifier funcName,
                           SqlNode className, SqlNodeList jarList) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.funcName = funcName;
    this.className = className;
    this.jarList = jarList;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    UnparseUtil u = new UnparseUtil(writer, leftPrec, rightPrec);
    if (getReplace()) {
      u.keyword("CREATE OR REPLACE");
    } else {
      u.keyword("CREATE");
    }
    u.keyword("FUNCTION");
    if (ifNotExists) {
      u.keyword("IF NOT EXISTS");
    }
    u.node(funcName).keyword("AS").node(className);
    if (jarList != null) {
      u.keyword("USING").nodeList(jarList);
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return Arrays.asList(funcName, className, jarList);
  }


  public SqlIdentifier funcName() {
    return funcName;
  }

  public SqlNode className() {
    return className;
  }

  public SqlNodeList jarList() {
    return jarList;
  }

  /** Creates a inner class. */
  class UnparseUtil   {
    private final SqlWriter writer;
    private final int leftPrec;
    private final int rightPrec;
    UnparseUtil(SqlWriter writer, int leftPrec, int rightPrec) {
      this.writer = writer;
      this.leftPrec = leftPrec;
      this.rightPrec = rightPrec;
    }
    UnparseUtil keyword(String... keywords) {
      Arrays.stream(keywords).forEach(writer::keyword);
      return this;
    }
    UnparseUtil node(SqlNode n) {
      n.unparse(writer, leftPrec, rightPrec);
      return this;
    }
    UnparseUtil nodeList(SqlNodeList l) {
      writer.keyword("(");
      if (l.size() > 0) {
        l.get(0).unparse(writer, leftPrec, rightPrec);
        for (int i = 1; i < l.size(); ++i) {
          writer.keyword(",");
          l.get(i).unparse(writer, leftPrec, rightPrec);
        }
      }
      writer.keyword(")");
      return this;
    }
  }
}

// End SqlCreateFunction.java
