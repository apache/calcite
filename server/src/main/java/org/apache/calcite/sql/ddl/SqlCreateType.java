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
import org.apache.calcite.rel.type.RelProtoDataType;
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
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

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
    this.name = Preconditions.checkNotNull(name);
    this.attributeDefs = attributeDefs; // may be null
    this.dataType = dataType; // may be null
  }

  /** Returns the schema in which to create an object. */
  Pair<CalciteSchema, String> schema(CalciteSchema schema, SqlIdentifier id) {
    final String name;
    final List<String> path;
    if (id.isSimple()) {
      path = ImmutableList.of();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }
    return Pair.of(schema, name);
  }

  @Override public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair =
        Util.schema(context.getMutableRootSchema(), context.getDefaultSchemaPath(), name);
    pair.left.add(pair.right, new RelProtoDataType() {
      @Override public RelDataType apply(RelDataTypeFactory a0) {
        if (dataType != null) {
          return dataType.deriveType(a0);
        } else {
          return a0.createStructType(
              attributeDefs.getList().stream().map(
                  (def) -> {
                    final SqlDataTypeSpec typeSpec = ((SqlAttributeDefinition) def).dataType;
                    RelDataType relDataType = typeSpec.deriveType(a0);
                    if (relDataType == null) {
                      Pair<CalciteSchema, String> pair =
                          schema(context.getRootSchema(), typeSpec.getTypeName());
                      relDataType = pair.left.getType(pair.right, false).getType().apply(a0);
                    }
                    return relDataType;
                  }).collect(Collectors.toList()),
              attributeDefs.getList().stream().map(
                  (def) -> ((SqlAttributeDefinition) def).name.getSimple())
                  .collect(Collectors.toList()));
        }
      }
    });
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, attributeDefs);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
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
