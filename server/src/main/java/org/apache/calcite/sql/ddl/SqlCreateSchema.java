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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code CREATE SCHEMA} statement.
 */
public class SqlCreateSchema extends SqlCreate
    implements SqlExecutableStatement {
  private final SqlIdentifier name;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE SCHEMA", SqlKind.CREATE_SCHEMA);

  /** Creates a SqlCreateSchema. */
  SqlCreateSchema(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier name) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name);
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (getReplace()) {
      writer.keyword("CREATE OR REPLACE");
    } else {
      writer.keyword("CREATE");
    }
    writer.keyword("SCHEMA");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
  }

  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair =
        SqlDdlNodes.schema(context, true, name);
    final SchemaPlus subSchema0 = pair.left.plus().getSubSchema(pair.right);
    if (subSchema0 != null) {
      if (!getReplace() && !ifNotExists) {
        throw SqlUtil.newContextException(name.getParserPosition(),
            RESOURCE.schemaExists(pair.right));
      }
    }
    final Schema subSchema = new AbstractSchema();
    pair.left.add(pair.right, subSchema);
  }
}

// End SqlCreateSchema.java
