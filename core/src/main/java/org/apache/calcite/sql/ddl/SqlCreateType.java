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
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree for {@code CREATE TYPE} statement.
 */
public class SqlCreateType extends SqlCreate {
  public final SqlIdentifier name;
  public final @Nullable SqlNodeList attributeDefs;
  public final @Nullable SqlDataTypeSpec dataType;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TYPE", SqlKind.CREATE_TYPE);

  /** Creates a SqlCreateType. */
  SqlCreateType(SqlParserPos pos, boolean replace, SqlIdentifier name,
      @Nullable SqlNodeList attributeDefs, @Nullable SqlDataTypeSpec dataType) {
    super(OPERATOR, pos, replace, false);
    this.name = requireNonNull(name, "name");
    this.attributeDefs = attributeDefs; // may be null
    this.dataType = dataType; // may be null
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, attributeDefs);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (getReplace()) {
      writer.keyword("CREATE OR REPLACE");
    } else {
      writer.keyword("CREATE");
    }
    writer.keyword("TYPE");
    name.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    if (attributeDefs != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode a : attributeDefs) {
        writer.sep(",");
        a.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    } else if (dataType != null) {
      dataType.unparse(writer, leftPrec, rightPrec);
    }
  }
}
