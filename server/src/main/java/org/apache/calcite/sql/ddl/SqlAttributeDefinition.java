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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Parse tree for SqlAttributeDefinition,
 * which is part of a {@link SqlCreateType}.
 */
public class SqlAttributeDefinition extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("ATTRIBUTE_DEF", SqlKind.ATTRIBUTE_DEF);

  final SqlIdentifier name;
  final SqlDataTypeSpec dataType;
  final SqlNode expression;
  final SqlCollation collation;

  /** Creates a SqlAttributeDefinition; use {@link SqlDdlNodes#attribute}. */
  SqlAttributeDefinition(SqlParserPos pos, SqlIdentifier name,
      SqlDataTypeSpec dataType, SqlNode expression, SqlCollation collation) {
    super(pos);
    this.name = name;
    this.dataType = dataType;
    this.expression = expression;
    this.collation = collation;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, dataType);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, 0, 0);
    dataType.unparse(writer, 0, 0);
    if (collation != null) {
      writer.keyword("COLLATE");
      collation.unparse(writer);
    }
    if (dataType.getNullable() != null && !dataType.getNullable()) {
      writer.keyword("NOT NULL");
    }
    if (expression != null) {
      writer.keyword("DEFAULT");
      exp(writer);
    }
  }

  // TODO: refactor this to a util class to share with SqlColumnDeclaration
  private void exp(SqlWriter writer) {
    if (writer.isAlwaysUseParentheses()) {
      expression.unparse(writer, 0, 0);
    } else {
      writer.sep("(");
      expression.unparse(writer, 0, 0);
      writer.sep(")");
    }
  }
}

// End SqlAttributeDefinition.java
