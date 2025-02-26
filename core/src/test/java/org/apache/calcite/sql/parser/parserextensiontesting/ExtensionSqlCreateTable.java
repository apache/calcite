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

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.function.BiConsumer;

import static java.util.Objects.requireNonNull;

/**
 * Simple test example of a CREATE TABLE statement.
 */
public class ExtensionSqlCreateTable extends SqlCreateTable {
  /** Creates a SqlCreateTable. */
  public ExtensionSqlCreateTable(SqlParserPos pos, SqlIdentifier name,
      SqlNodeList columnList, SqlNode query) {
    super(pos, false, false, name, columnList, query);
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      forEachNameType((name, typeSpec) -> {
        writer.sep(",");
        name.unparse(writer, leftPrec, rightPrec);
        typeSpec.unparse(writer, leftPrec, rightPrec);
        if (Boolean.FALSE.equals(typeSpec.getNullable())) {
          writer.keyword("NOT NULL");
        }
      });
      writer.endList(frame);
    }
    if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, 0, 0);
    }
  }

  /** Calls an action for each (name, type) pair from {@code columnList}, in which
   * they alternate. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void forEachNameType(BiConsumer<SqlIdentifier, SqlDataTypeSpec> consumer) {
    final List list = requireNonNull(columnList, "columnList");
    Pair.forEach((List<SqlIdentifier>) Util.quotientList(list, 2, 0),
        Util.quotientList((List<SqlDataTypeSpec>) list, 2, 1), consumer);
  }
}
