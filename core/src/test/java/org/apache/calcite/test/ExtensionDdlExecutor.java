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
package org.apache.calcite.test;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.parserextensiontesting.ExtensionSqlParserImpl;
import org.apache.calcite.sql.parser.parserextensiontesting.SqlCreateTable;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Reader;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/** Executes the few DDL commands supported by
 * {@link ExtensionSqlParserImpl}. */
public class ExtensionDdlExecutor extends DdlExecutorImpl {
  static final ExtensionDdlExecutor INSTANCE = new ExtensionDdlExecutor();

  /** Parser factory. */
  @SuppressWarnings("unused") // used via reflection
  public static final SqlParserImplFactory PARSER_FACTORY =
      new SqlParserImplFactory() {
        @Override public SqlAbstractParserImpl getParser(Reader stream) {
          return ExtensionSqlParserImpl.FACTORY.getParser(stream);
        }

        @Override public DdlExecutor getDdlExecutor() {
          return ExtensionDdlExecutor.INSTANCE;
        }
      };

  /** Returns the schema in which to create an object. */
  static Pair<CalciteSchema, String> schema(CalcitePrepare.Context context,
      boolean mutable, SqlIdentifier id) {
    final String name;
    final List<String> path;
    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }
    CalciteSchema schema = mutable ? context.getMutableRootSchema()
        : context.getRootSchema();
    for (String p : path) {
      schema = requireNonNull(schema.getSubSchema(p, true));
    }
    return Pair.of(schema, name);
  }

  /** Wraps a query to rename its columns. Used by CREATE VIEW and CREATE
   * MATERIALIZED VIEW. */
  static SqlNode renameColumns(@Nullable SqlNodeList columnList,
      SqlNode query) {
    if (columnList == null) {
      return query;
    }
    final SqlParserPos p = query.getParserPosition();
    final SqlNodeList selectList = SqlNodeList.SINGLETON_STAR;
    final SqlCall from =
        SqlStdOperatorTable.AS.createCall(p,
            ImmutableList.<SqlNode>builder()
                .add(query)
                .add(new SqlIdentifier("_", p))
                .addAll(columnList)
                .build());
    return new SqlSelect(p, null, selectList, from, null, null, null, null,
        null, null, null, null);
  }

  /** Executes a {@code CREATE TABLE} command. Called via reflection. */
  public void execute(SqlCreateTable create, CalcitePrepare.Context context) {
    final CalciteSchema schema =
        Schemas.subSchema(context.getRootSchema(),
            context.getDefaultSchemaPath());
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final RelDataType queryRowType;
    if (create.query != null) {
      // A bit of a hack: pretend it's a view, to get its row type
      final String sql =
          create.query.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
      final ViewTableMacro viewTableMacro =
          ViewTable.viewMacro(schema.plus(), sql, schema.path(null),
              context.getObjectPath(), false);
      final TranslatableTable x = viewTableMacro.apply(ImmutableList.of());
      queryRowType = x.getRowType(typeFactory);

      if (create.columnList != null
          && queryRowType.getFieldCount() != create.columnList.size()) {
        throw SqlUtil.newContextException(create.columnList.getParserPosition(),
            RESOURCE.columnCountMismatch());
      }
    } else {
      queryRowType = null;
    }
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    if (create.columnList != null) {
      final SqlValidator validator = new ContextSqlValidator(context, false);
      create.forEachNameType((name, typeSpec) ->
          builder.add(name.getSimple(), typeSpec.deriveType(validator, true)));
    } else {
      if (queryRowType == null) {
        // "CREATE TABLE t" is invalid; because there is no "AS query" we need
        // a list of column names and types, "CREATE TABLE t (INT c)".
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.createTableRequiresColumnList());
      }
      builder.addAll(queryRowType.getFieldList());
    }
    final RelDataType rowType = builder.build();
    schema.add(create.name.getSimple(),
        new MutableArrayTable(create.name.getSimple(),
            RelDataTypeImpl.proto(rowType)));
    if (create.query != null) {
      populate(create.name, create.query, context);
    }
  }

  /** Executes a {@code CREATE VIEW} command. */
  public void execute(SqlCreateView create,
      CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair =
        schema(context, true, create.name);
    final SchemaPlus schemaPlus = pair.left.plus();
    for (Function function : schemaPlus.getFunctions(pair.right)) {
      if (function.getParameters().isEmpty()) {
        if (!create.getReplace()) {
          throw SqlUtil.newContextException(create.name.getParserPosition(),
              RESOURCE.viewExists(pair.right));
        }
        pair.left.removeFunction(pair.right);
      }
    }
    final SqlNode q = renameColumns(create.columnList, create.query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final ViewTableMacro viewTableMacro =
        ViewTable.viewMacro(schemaPlus, sql, pair.left.path(null),
            context.getObjectPath(), false);
    final TranslatableTable x = viewTableMacro.apply(ImmutableList.of());
    Util.discard(x);
    schemaPlus.add(pair.right, viewTableMacro);
  }

  /** Populates the table called {@code name} by executing {@code query}. */
  protected static void populate(SqlIdentifier name, SqlNode query,
      CalcitePrepare.Context context) {
    // Generate, prepare and execute an "INSERT INTO table query" statement.
    // (It's a bit inefficient that we convert from SqlNode to SQL and back
    // again.)
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(
            requireNonNull(
                Schemas.subSchema(context.getRootSchema(),
                    context.getDefaultSchemaPath())).plus())
        .build();
    final Planner planner = Frameworks.getPlanner(config);
    try {
      final StringBuilder buf = new StringBuilder();
      final SqlPrettyWriter w =
          new SqlPrettyWriter(
              SqlPrettyWriter.config()
                  .withDialect(CalciteSqlDialect.DEFAULT)
                  .withAlwaysUseParentheses(false),
              buf);
      buf.append("INSERT INTO ");
      name.unparse(w, 0, 0);
      buf.append(" ");
      query.unparse(w, 0, 0);
      final String sql = buf.toString();
      final SqlNode query1 = planner.parse(sql);
      final SqlNode query2 = planner.validate(query1);
      final RelRoot r = planner.rel(query2);
      final PreparedStatement prepare =
          context.getRelRunner().prepareStatement(r.rel);
      int rowCount = prepare.executeUpdate();
      Util.discard(rowCount);
      prepare.close();
    } catch (SqlParseException | ValidationException
        | RelConversionException | SQLException e) {
      throw Util.throwAsRuntime(e);
    }
  }

  /** Table backed by a Java list. */
  private static class MutableArrayTable
      extends AbstractModifiableTable {
    final List list = new ArrayList();
    private final RelProtoDataType protoRowType;

    MutableArrayTable(String name, RelProtoDataType protoRowType) {
      super(name);
      this.protoRowType = protoRowType;
    }

    public Collection getModifiableCollection() {
      return list;
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        public Enumerator<T> enumerator() {
          //noinspection unchecked
          return (Enumerator<T>) Linq4j.enumerator(list);
        }
      };
    }

    public Type getElementType() {
      return Object[].class;
    }

    public Expression getExpression(SchemaPlus schema, String tableName,
        Class clazz) {
      return Schemas.tableExpression(schema, getElementType(),
          tableName, clazz);
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return protoRowType.apply(typeFactory);
    }
  }
}
