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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code CREATE TYPE} statement.
 */
public class SqlCreateType extends SqlCreate
    implements SqlExecutableStatement {
  private final SqlIdentifier name;
  private final SqlNodeList attributeDefs;
  private final SqlDataTypeSpec dataType;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TYPE", SqlKind.CREATE_TYPE);

  /** Creates a SqlCreateType. */
  SqlCreateType(SqlParserPos pos, boolean replace, SqlIdentifier name,
      SqlNodeList attributeDefs, SqlDataTypeSpec dataType) {
    super(OPERATOR, pos, replace, false);
    this.name = Objects.requireNonNull(name);
    this.attributeDefs = attributeDefs; // may be null
    this.dataType = dataType; // may be null
  }

  @Override public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair =
        SqlDdlNodes.schema(context, true, name);
    final SqlValidator validator = SqlDdlNodes.validator(context, false);
    pair.left.add(pair.right, typeFactory -> {
      if (dataType != null) {
        return dataType.deriveType(validator);
      } else {
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (SqlNode def : attributeDefs) {
          final SqlAttributeDefinition attributeDef =
              (SqlAttributeDefinition) def;
          final SqlDataTypeSpec typeSpec = attributeDef.dataType;
          final RelDataType type = typeSpec.deriveType(validator);
          builder.add(attributeDef.name.getSimple(), type);
        }
        return builder.build();
      }
    });
  }

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

// End SqlCreateType.java
