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
import org.apache.calcite.materialize.MaterializationKey;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code CREATE MATERIALIZED VIEW} statement.
 */
public class SqlCreateMaterializedView extends SqlCreate
    implements SqlExecutableStatement {
  private final SqlIdentifier name;
  private final SqlNodeList columnList;
  private final SqlNode query;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE MATERIALIZED VIEW",
          SqlKind.CREATE_MATERIALIZED_VIEW);

  /** Creates a SqlCreateView. */
  SqlCreateMaterializedView(SqlParserPos pos, boolean replace,
      boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList,
      SqlNode query) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name);
    this.columnList = columnList; // may be null
    this.query = Objects.requireNonNull(query);
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("MATERIALIZED VIEW");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    writer.keyword("AS");
    writer.newlineAndIndent();
    query.unparse(writer, 0, 0);
  }

  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair =
        SqlDdlNodes.schema(context, true, name);
    if (pair.left.plus().getTable(pair.right) != null) {
      // Materialized view exists.
      if (!ifNotExists) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(name.getParserPosition(),
            RESOURCE.tableExists(pair.right));
      }
      return;
    }
    final SqlNode q = SqlDdlNodes.renameColumns(columnList, query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final List<String> schemaPath = pair.left.path(null);
    final ViewTableMacro viewTableMacro =
        ViewTable.viewMacro(pair.left.plus(), sql, schemaPath,
            context.getObjectPath(), false);
    final TranslatableTable x = viewTableMacro.apply(ImmutableList.of());
    final RelDataType rowType = x.getRowType(context.getTypeFactory());

    // Table does not exist. Create it.
    final MaterializedViewTable table =
        new MaterializedViewTable(pair.right, RelDataTypeImpl.proto(rowType));
    pair.left.add(pair.right, table);
    SqlDdlNodes.populate(name, query, context);
    table.key =
        MaterializationService.instance().defineMaterialization(pair.left, null,
            sql, schemaPath, pair.right, true, true);
  }

  /** A table that implements a materialized view. */
  private static class MaterializedViewTable
      extends SqlCreateTable.MutableArrayTable {
    /** The key with which this was stored in the materialization service,
     * or null if not (yet) materialized. */
    MaterializationKey key;

    MaterializedViewTable(String name, RelProtoDataType protoRowType) {
      super(name, protoRowType, protoRowType,
          NullInitializerExpressionFactory.INSTANCE);
    }

    @Override public Schema.TableType getJdbcTableType() {
      return Schema.TableType.MATERIALIZED_VIEW;
    }

    @Override public <C> C unwrap(Class<C> aClass) {
      if (MaterializationKey.class.isAssignableFrom(aClass)
          && aClass.isInstance(key)) {
        return aClass.cast(key);
      }
      return super.unwrap(aClass);
    }
  }
}

// End SqlCreateMaterializedView.java
