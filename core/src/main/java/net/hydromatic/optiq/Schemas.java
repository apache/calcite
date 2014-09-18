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
package net.hydromatic.optiq;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.config.OptiqConnectionConfig;
import net.hydromatic.optiq.config.OptiqConnectionConfigImpl;
import net.hydromatic.optiq.config.OptiqConnectionProperty;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.*;
import net.hydromatic.optiq.materialize.Lattice;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelProtoDataType;
import org.eigenbase.sql.type.SqlTypeUtil;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.lang.reflect.*;
import java.sql.Connection;
import java.util.*;

/**
 * Utility functions for schemas.
 */
public final class Schemas {
  private static final com.google.common.base.Function<OptiqSchema.LatticeEntry,
      OptiqSchema.TableEntry> TO_TABLE_ENTRY =
      new com.google.common.base.Function<OptiqSchema.LatticeEntry,
          OptiqSchema.TableEntry>() {
        public OptiqSchema.TableEntry apply(OptiqSchema.LatticeEntry entry) {
          final OptiqSchema.TableEntry starTable = entry.getStarTable();
          assert starTable.getTable().getJdbcTableType()
              == Schema.TableType.STAR;
          return entry.getStarTable();
        }
      };

  private static final com.google.common.base.Function<OptiqSchema.LatticeEntry,
      Lattice> TO_LATTICE =
      new com.google.common.base.Function<OptiqSchema.LatticeEntry, Lattice>() {
        public Lattice apply(OptiqSchema.LatticeEntry entry) {
          return entry.getLattice();
        }
      };

  private Schemas() {
    throw new AssertionError("no instances!");
  }

  public static OptiqSchema.FunctionEntry resolve(
      RelDataTypeFactory typeFactory,
      String name,
      Collection<OptiqSchema.FunctionEntry> functionEntries,
      List<RelDataType> argumentTypes) {
    final List<OptiqSchema.FunctionEntry> matches =
        new ArrayList<OptiqSchema.FunctionEntry>();
    for (OptiqSchema.FunctionEntry entry : functionEntries) {
      if (matches(typeFactory, entry.getFunction(), argumentTypes)) {
        matches.add(entry);
      }
    }
    switch (matches.size()) {
    case 0:
      return null;
    case 1:
      return matches.get(0);
    default:
      throw new RuntimeException(
          "More than one match for " + name
          + " with arguments " + argumentTypes);
    }
  }

  private static boolean matches(RelDataTypeFactory typeFactory,
      Function member, List<RelDataType> argumentTypes) {
    List<FunctionParameter> parameters = member.getParameters();
    if (parameters.size() != argumentTypes.size()) {
      return false;
    }
    for (int i = 0; i < argumentTypes.size(); i++) {
      RelDataType argumentType = argumentTypes.get(i);
      FunctionParameter parameter = parameters.get(i);
      if (!canConvert(argumentType, parameter.getType(typeFactory))) {
        return false;
      }
    }
    return true;
  }

  private static boolean canConvert(RelDataType fromType, RelDataType toType) {
    return SqlTypeUtil.canAssignFrom(toType, fromType);
  }

  /** Returns the expression for a schema. */
  public static Expression expression(SchemaPlus schema) {
    return schema.getExpression(schema.getParentSchema(), schema.getName());
  }

  /** Returns the expression for a sub-schema. */
  public static Expression subSchemaExpression(SchemaPlus schema, String name,
      Class type) {
    // (Type) schemaExpression.getSubSchema("name")
    final Expression schemaExpression = expression(schema);
    Expression call =
        Expressions.call(
            schemaExpression,
            BuiltinMethod.SCHEMA_GET_SUB_SCHEMA.method,
            Expressions.constant(name));
    //CHECKSTYLE: IGNORE 2
    //noinspection unchecked
    if (false && type != null && !type.isAssignableFrom(Schema.class)) {
      return unwrap(call, type);
    }
    return call;
  }

  /** Converts a schema expression to a given type by calling the
   * {@link net.hydromatic.optiq.SchemaPlus#unwrap(Class)} method. */
  public static Expression unwrap(Expression call, Class type) {
    return Expressions.convert_(
        Expressions.call(call, BuiltinMethod.SCHEMA_PLUS_UNWRAP.method,
            Expressions.constant(type)),
        type);
  }

  /** Returns the expression to access a table within a schema. */
  public static Expression tableExpression(SchemaPlus schema, Type elementType,
      String tableName, Class clazz) {
    final MethodCallExpression expression;
    if (Table.class.isAssignableFrom(clazz)) {
      expression = Expressions.call(
          expression(schema),
          BuiltinMethod.SCHEMA_GET_TABLE.method,
          Expressions.constant(tableName));
    } else {
      expression = Expressions.call(
          BuiltinMethod.SCHEMAS_QUERYABLE.method,
          DataContext.ROOT,
          expression(schema),
          Expressions.constant(elementType),
          Expressions.constant(tableName));
    }
    return Types.castIfNecessary(
        clazz, expression);
  }

  public static DataContext createDataContext(Connection connection) {
    return new DummyDataContext((OptiqConnection) connection);
  }

  /** Returns a {@link Queryable}, given a fully-qualified table name. */
  public static <E> Queryable<E> queryable(DataContext root, Class<E> clazz,
      String... names) {
    return queryable(root, clazz, Arrays.asList(names));
  }

  /** Returns a {@link Queryable}, given a fully-qualified table name as an
   * iterable. */
  public static <E> Queryable<E> queryable(DataContext root, Class<E> clazz,
      Iterable<? extends String> names) {
    SchemaPlus schema = root.getRootSchema();
    for (Iterator<? extends String> iterator = names.iterator();;) {
      String name = iterator.next();
      if (iterator.hasNext()) {
        schema = schema.getSubSchema(name);
      } else {
        return queryable(root, schema, clazz, name);
      }
    }
  }

  /** Returns a {@link Queryable}, given a schema and table name. */
  public static <E> Queryable<E> queryable(DataContext root, SchemaPlus schema,
      Class<E> clazz, String tableName) {
    QueryableTable table = (QueryableTable) schema.getTable(tableName);
    return table.asQueryable(root.getQueryProvider(), schema, tableName);
  }

  /** Parses and validates a SQL query. For use within Optiq only. */
  public static OptiqPrepare.ParseResult parse(
      final OptiqConnection connection, final OptiqSchema schema,
      final List<String> schemaPath, final String sql) {
    final OptiqPrepare prepare = OptiqPrepare.DEFAULT_FACTORY.apply();
    final OptiqPrepare.Context context =
        makeContext(connection, schema, schemaPath,
            ImmutableMap.<OptiqConnectionProperty, String>of());
    OptiqPrepare.Dummy.push(context);
    try {
      return prepare.parse(context, sql);
    } finally {
      OptiqPrepare.Dummy.pop(context);
    }
  }

  /** Parses and validates a SQL query and converts to relational algebra. For
   * use within Optiq only. */
  public static OptiqPrepare.ConvertResult convert(
      final OptiqConnection connection, final OptiqSchema schema,
      final List<String> schemaPath, final String sql) {
    final OptiqPrepare prepare = OptiqPrepare.DEFAULT_FACTORY.apply();
    final OptiqPrepare.Context context =
        makeContext(connection, schema, schemaPath,
            ImmutableMap.<OptiqConnectionProperty, String>of());
    OptiqPrepare.Dummy.push(context);
    try {
      return prepare.convert(context, sql);
    } finally {
      OptiqPrepare.Dummy.pop(context);
    }
  }

  /** Prepares a SQL query for execution. For use within Optiq only. */
  public static OptiqPrepare.PrepareResult<Object> prepare(
      final OptiqConnection connection, final OptiqSchema schema,
      final List<String> schemaPath, final String sql,
      final ImmutableMap<OptiqConnectionProperty, String> map) {
    final OptiqPrepare prepare = OptiqPrepare.DEFAULT_FACTORY.apply();
    final OptiqPrepare.Context context =
        makeContext(connection, schema, schemaPath, map);
    OptiqPrepare.Dummy.push(context);
    try {
      return prepare.prepareSql(context, sql, null, Object[].class, -1);
    } finally {
      OptiqPrepare.Dummy.pop(context);
    }
  }

  public static OptiqPrepare.Context makeContext(
      final OptiqConnection connection, final OptiqSchema schema,
      final List<String> schemaPath,
      final ImmutableMap<OptiqConnectionProperty, String> propValues) {
    if (connection == null) {
      final OptiqPrepare.Context context0 = OptiqPrepare.Dummy.peek();
      final OptiqConnectionConfig config =
          mutate(context0.config(), propValues);
      return makeContext(config, context0.getTypeFactory(),
          context0.getDataContext(), schema, schemaPath);
    } else {
      final OptiqConnectionConfig config =
          mutate(connection.config(), propValues);
      return makeContext(config, connection.getTypeFactory(),
          createDataContext(connection), schema, schemaPath);
    }
  }

  private static OptiqConnectionConfig mutate(OptiqConnectionConfig config,
      ImmutableMap<OptiqConnectionProperty, String> propValues) {
    for (Map.Entry<OptiqConnectionProperty, String> e : propValues.entrySet()) {
      config =
          ((OptiqConnectionConfigImpl) config).set(e.getKey(), e.getValue());
    }
    return config;
  }

  private static OptiqPrepare.Context makeContext(
      final OptiqConnectionConfig connectionConfig,
      final JavaTypeFactory typeFactory,
      final DataContext dataContext,
      final OptiqSchema schema,
      final List<String> schemaPath) {
    return new OptiqPrepare.Context() {
      public JavaTypeFactory getTypeFactory() {
        return typeFactory;
      }

      public OptiqRootSchema getRootSchema() {
        return schema.root();
      }

      public List<String> getDefaultSchemaPath() {
        // schemaPath is usually null. If specified, it overrides schema
        // as the context within which the SQL is validated.
        if (schemaPath == null) {
          return schema.path(null);
        }
        return schemaPath;
      }

      public OptiqConnectionConfig config() {
        return connectionConfig;
      }

      public DataContext getDataContext() {
        return dataContext;
      }

      public OptiqPrepare.SparkHandler spark() {
        final boolean enable = config().spark();
        return OptiqPrepare.Dummy.getSparkHandler(enable);
      }
    };
  }

  /** Returns an implementation of
   * {@link RelProtoDataType}
   * that asks a given table for its row type with a given type factory. */
  public static RelProtoDataType proto(final Table table) {
    return new RelProtoDataType() {
      public RelDataType apply(RelDataTypeFactory typeFactory) {
        return table.getRowType(typeFactory);
      }
    };
  }

  /** Returns an implementation of {@link RelProtoDataType}
   * that asks a given scalar function for its return type with a given type
   * factory. */
  public static RelProtoDataType proto(final ScalarFunction function) {
    return new RelProtoDataType() {
      public RelDataType apply(RelDataTypeFactory typeFactory) {
        return function.getReturnType(typeFactory);
      }
    };
  }

  /** Returns the star tables defined in a schema.
   *
   * @param schema Schema */
  public static List<OptiqSchema.TableEntry> getStarTables(OptiqSchema schema) {
    final List<OptiqSchema.LatticeEntry> list = getLatticeEntries(schema);
    return Lists.transform(list, TO_TABLE_ENTRY);
  }

  /** Returns the lattices defined in a schema.
   *
   * @param schema Schema */
  public static List<Lattice> getLattices(OptiqSchema schema) {
    final List<OptiqSchema.LatticeEntry> list = getLatticeEntries(schema);
    return Lists.transform(list, TO_LATTICE);
  }

  /** Returns the lattices defined in a schema.
   *
   * @param schema Schema */
  public static List<OptiqSchema.LatticeEntry> getLatticeEntries(
      OptiqSchema schema) {
    final List<OptiqSchema.LatticeEntry> list = Lists.newArrayList();
    gatherLattices(schema, list);
    return list;
  }

  private static void gatherLattices(OptiqSchema schema,
      List<OptiqSchema.LatticeEntry> list) {
    list.addAll(schema.getLatticeMap().values());
    for (OptiqSchema subSchema : schema.getSubSchemaMap().values()) {
      gatherLattices(subSchema, list);
    }
  }

  public static OptiqSchema subSchema(OptiqSchema schema, List<String> names) {
    for (String string : names) {
      schema = schema.getSubSchema(string, false);
    }
    return schema;
  }

  /** Dummy data context that has no variables. */
  private static class DummyDataContext implements DataContext {
    private final OptiqConnection connection;
    private final ImmutableMap<String, Object> map;

    public DummyDataContext(OptiqConnection connection) {
      this.connection = connection;
      this.map =
          ImmutableMap.<String, Object>of("timeZone", TimeZone.getDefault());
    }

    public SchemaPlus getRootSchema() {
      return connection.getRootSchema();
    }

    public JavaTypeFactory getTypeFactory() {
      return connection.getTypeFactory();
    }

    public QueryProvider getQueryProvider() {
      return connection;
    }

    public Object get(String name) {
      return map.get(name);
    }
  }
}

// End Schemas.java
