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
package org.apache.calcite.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.calcite.jdbc.CalciteSchema.LatticeEntry;

/**
 * Utility functions for schemas.
 */
public final class Schemas {

  private Schemas() {
    throw new AssertionError("no instances!");
  }

  public static CalciteSchema.FunctionEntry resolve(
      RelDataTypeFactory typeFactory,
      String name,
      Collection<CalciteSchema.FunctionEntry> functionEntries,
      List<RelDataType> argumentTypes) {
    final List<CalciteSchema.FunctionEntry> matches = new ArrayList<>();
    for (CalciteSchema.FunctionEntry entry : functionEntries) {
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
      throw new RuntimeException("More than one match for " + name
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
            BuiltInMethod.SCHEMA_GET_SUB_SCHEMA.method,
            Expressions.constant(name));
    //CHECKSTYLE: IGNORE 2
    //noinspection unchecked
    if (false && type != null && !type.isAssignableFrom(Schema.class)) {
      return unwrap(call, type);
    }
    return call;
  }

  /** Converts a schema expression to a given type by calling the
   * {@link SchemaPlus#unwrap(Class)} method. */
  public static Expression unwrap(Expression call, Class type) {
    return Expressions.convert_(
        Expressions.call(call, BuiltInMethod.SCHEMA_PLUS_UNWRAP.method,
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
          BuiltInMethod.SCHEMA_GET_TABLE.method,
          Expressions.constant(tableName));
      if (ScannableTable.class.isAssignableFrom(clazz)) {
        return Expressions.call(
            BuiltInMethod.SCHEMAS_ENUMERABLE_SCANNABLE.method,
            Expressions.convert_(expression, ScannableTable.class),
            DataContext.ROOT);
      }
      if (FilterableTable.class.isAssignableFrom(clazz)) {
        return Expressions.call(
            BuiltInMethod.SCHEMAS_ENUMERABLE_FILTERABLE.method,
            Expressions.convert_(expression, FilterableTable.class),
            DataContext.ROOT);
      }
      if (ProjectableFilterableTable.class.isAssignableFrom(clazz)) {
        return Expressions.call(
            BuiltInMethod.SCHEMAS_ENUMERABLE_PROJECTABLE_FILTERABLE.method,
            Expressions.convert_(expression, ProjectableFilterableTable.class),
            DataContext.ROOT);
      }
    } else {
      expression = Expressions.call(
          BuiltInMethod.SCHEMAS_QUERYABLE.method,
          DataContext.ROOT,
          expression(schema),
          Expressions.constant(elementType),
          Expressions.constant(tableName));
    }
    return Types.castIfNecessary(clazz, expression);
  }

  public static DataContext createDataContext(
      Connection connection, SchemaPlus rootSchema) {
    return new DummyDataContext((CalciteConnection) connection, rootSchema);
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

  /** Returns an {@link org.apache.calcite.linq4j.Enumerable} over the rows of
   * a given table, representing each row as an object array. */
  public static Enumerable<Object[]> enumerable(final ScannableTable table,
      final DataContext root) {
    return table.scan(root);
  }

  /** Returns an {@link org.apache.calcite.linq4j.Enumerable} over the rows of
   * a given table, not applying any filters, representing each row as an object
   * array. */
  public static Enumerable<Object[]> enumerable(final FilterableTable table,
      final DataContext root) {
    return table.scan(root, ImmutableList.of());
  }

  /** Returns an {@link org.apache.calcite.linq4j.Enumerable} over the rows of
   * a given table, not applying any filters and projecting all columns,
   * representing each row as an object array. */
  public static Enumerable<Object[]> enumerable(
      final ProjectableFilterableTable table, final DataContext root) {
    return table.scan(root, ImmutableList.of(),
        identity(table.getRowType(root.getTypeFactory()).getFieldCount()));
  }

  private static int[] identity(int count) {
    final int[] integers = new int[count];
    for (int i = 0; i < integers.length; i++) {
      integers[i] = i;
    }
    return integers;
  }

  /** Returns an {@link org.apache.calcite.linq4j.Enumerable} over object
   * arrays, given a fully-qualified table name which leads to a
   * {@link ScannableTable}. */
  public static Table table(DataContext root, String... names) {
    SchemaPlus schema = root.getRootSchema();
    final List<String> nameList = Arrays.asList(names);
    for (Iterator<? extends String> iterator = nameList.iterator();;) {
      String name = iterator.next();
      if (iterator.hasNext()) {
        schema = schema.getSubSchema(name);
      } else {
        return schema.getTable(name);
      }
    }
  }

  /** Parses and validates a SQL query. For use within Calcite only. */
  public static CalcitePrepare.ParseResult parse(
      final CalciteConnection connection, final CalciteSchema schema,
      final List<String> schemaPath, final String sql) {
    final CalcitePrepare prepare = CalcitePrepare.DEFAULT_FACTORY.apply();
    final ImmutableMap<CalciteConnectionProperty, String> propValues =
        ImmutableMap.of();
    final CalcitePrepare.Context context =
        makeContext(connection, schema, schemaPath, null, propValues);
    CalcitePrepare.Dummy.push(context);
    try {
      return prepare.parse(context, sql);
    } finally {
      CalcitePrepare.Dummy.pop(context);
    }
  }

  /** Parses and validates a SQL query and converts to relational algebra. For
   * use within Calcite only. */
  public static CalcitePrepare.ConvertResult convert(
      final CalciteConnection connection, final CalciteSchema schema,
      final List<String> schemaPath, final String sql) {
    final CalcitePrepare prepare = CalcitePrepare.DEFAULT_FACTORY.apply();
    final ImmutableMap<CalciteConnectionProperty, String> propValues =
        ImmutableMap.of();
    final CalcitePrepare.Context context =
        makeContext(connection, schema, schemaPath, null, propValues);
    CalcitePrepare.Dummy.push(context);
    try {
      return prepare.convert(context, sql);
    } finally {
      CalcitePrepare.Dummy.pop(context);
    }
  }

  /** Analyzes a view. For use within Calcite only. */
  public static CalcitePrepare.AnalyzeViewResult analyzeView(
      final CalciteConnection connection, final CalciteSchema schema,
      final List<String> schemaPath, final String viewSql,
      List<String> viewPath, boolean fail) {
    final CalcitePrepare prepare = CalcitePrepare.DEFAULT_FACTORY.apply();
    final ImmutableMap<CalciteConnectionProperty, String> propValues =
        ImmutableMap.of();
    final CalcitePrepare.Context context =
        makeContext(connection, schema, schemaPath, viewPath, propValues);
    CalcitePrepare.Dummy.push(context);
    try {
      return prepare.analyzeView(context, viewSql, fail);
    } finally {
      CalcitePrepare.Dummy.pop(context);
    }
  }

  /** Prepares a SQL query for execution. For use within Calcite only. */
  public static CalcitePrepare.CalciteSignature<Object> prepare(
      final CalciteConnection connection, final CalciteSchema schema,
      final List<String> schemaPath, final String sql,
      final ImmutableMap<CalciteConnectionProperty, String> map) {
    final CalcitePrepare prepare = CalcitePrepare.DEFAULT_FACTORY.apply();
    final CalcitePrepare.Context context =
        makeContext(connection, schema, schemaPath, null, map);
    CalcitePrepare.Dummy.push(context);
    try {
      return prepare.prepareSql(context, CalcitePrepare.Query.of(sql),
          Object[].class, -1);
    } finally {
      CalcitePrepare.Dummy.pop(context);
    }
  }

  /**
   * Creates a context for the purposes of preparing a statement.
   *
   * @param connection Connection
   * @param schema Schema
   * @param schemaPath Path wherein to look for functions
   * @param objectPath Path of the object being analyzed (usually a view),
   *                  or null
   * @param propValues Connection properties
   * @return Context
   */
  private static CalcitePrepare.Context makeContext(
      CalciteConnection connection, CalciteSchema schema,
      List<String> schemaPath, List<String> objectPath,
      final ImmutableMap<CalciteConnectionProperty, String> propValues) {
    if (connection == null) {
      final CalcitePrepare.Context context0 = CalcitePrepare.Dummy.peek();
      final CalciteConnectionConfig config =
          mutate(context0.config(), propValues);
      return makeContext(config, context0.getTypeFactory(),
          context0.getDataContext(), schema, schemaPath, objectPath);
    } else {
      final CalciteConnectionConfig config =
          mutate(connection.config(), propValues);
      return makeContext(config, connection.getTypeFactory(),
          createDataContext(connection, schema.root().plus()), schema,
          schemaPath, objectPath);
    }
  }

  private static CalciteConnectionConfig mutate(CalciteConnectionConfig config,
      ImmutableMap<CalciteConnectionProperty, String> propValues) {
    for (Map.Entry<CalciteConnectionProperty, String> e
        : propValues.entrySet()) {
      config =
          ((CalciteConnectionConfigImpl) config).set(e.getKey(), e.getValue());
    }
    return config;
  }

  private static CalcitePrepare.Context makeContext(
      final CalciteConnectionConfig connectionConfig,
      final JavaTypeFactory typeFactory,
      final DataContext dataContext,
      final CalciteSchema schema,
      final List<String> schemaPath, final List<String> objectPath_) {
    final ImmutableList<String> objectPath =
        objectPath_ == null ? null : ImmutableList.copyOf(objectPath_);
    return new CalcitePrepare.Context() {
      public JavaTypeFactory getTypeFactory() {
        return typeFactory;
      }

      public CalciteSchema getRootSchema() {
        return schema.root();
      }

      public CalciteSchema getMutableRootSchema() {
        return getRootSchema();
      }

      public List<String> getDefaultSchemaPath() {
        // schemaPath is usually null. If specified, it overrides schema
        // as the context within which the SQL is validated.
        if (schemaPath == null) {
          return schema.path(null);
        }
        return schemaPath;
      }

      public List<String> getObjectPath() {
        return objectPath;
      }

      public CalciteConnectionConfig config() {
        return connectionConfig;
      }

      public DataContext getDataContext() {
        return dataContext;
      }

      public RelRunner getRelRunner() {
        throw new UnsupportedOperationException();
      }

      public CalcitePrepare.SparkHandler spark() {
        final boolean enable = config().spark();
        return CalcitePrepare.Dummy.getSparkHandler(enable);
      }
    };
  }

  /** Returns an implementation of
   * {@link RelProtoDataType}
   * that asks a given table for its row type with a given type factory. */
  public static RelProtoDataType proto(final Table table) {
    return table::getRowType;
  }

  /** Returns an implementation of {@link RelProtoDataType}
   * that asks a given scalar function for its return type with a given type
   * factory. */
  public static RelProtoDataType proto(final ScalarFunction function) {
    return function::getReturnType;
  }

  /** Returns the star tables defined in a schema.
   *
   * @param schema Schema */
  public static List<CalciteSchema.TableEntry> getStarTables(
      CalciteSchema schema) {
    final List<CalciteSchema.LatticeEntry> list = getLatticeEntries(schema);
    return Lists.transform(list, entry -> {
      final CalciteSchema.TableEntry starTable =
          Objects.requireNonNull(entry).getStarTable();
      assert starTable.getTable().getJdbcTableType()
          == Schema.TableType.STAR;
      return entry.getStarTable();
    });
  }

  /** Returns the lattices defined in a schema.
   *
   * @param schema Schema */
  public static List<Lattice> getLattices(CalciteSchema schema) {
    final List<CalciteSchema.LatticeEntry> list = getLatticeEntries(schema);
    return Lists.transform(list, CalciteSchema.LatticeEntry::getLattice);
  }

  /** Returns the lattices defined in a schema.
   *
   * @param schema Schema */
  public static List<CalciteSchema.LatticeEntry> getLatticeEntries(
      CalciteSchema schema) {
    final List<LatticeEntry> list = new ArrayList<>();
    gatherLattices(schema, list);
    return list;
  }

  private static void gatherLattices(CalciteSchema schema,
      List<CalciteSchema.LatticeEntry> list) {
    list.addAll(schema.getLatticeMap().values());
    for (CalciteSchema subSchema : schema.getSubSchemaMap().values()) {
      gatherLattices(subSchema, list);
    }
  }

  /** Returns a sub-schema of a given schema obtained by following a sequence
   * of names.
   *
   * <p>The result is null if the initial schema is null or any sub-schema does
   * not exist.
   */
  public static CalciteSchema subSchema(CalciteSchema schema,
      Iterable<String> names) {
    for (String string : names) {
      if (schema == null) {
        return null;
      }
      schema = schema.getSubSchema(string, false);
    }
    return schema;
  }

  /** Generates a table name that is unique within the given schema. */
  public static String uniqueTableName(CalciteSchema schema, String base) {
    String t = Objects.requireNonNull(base);
    for (int x = 0; schema.getTable(t, true) != null; x++) {
      t = base + x;
    }
    return t;
  }

  /** Creates a path with a given list of names starting from a given root
   * schema. */
  public static Path path(CalciteSchema rootSchema, Iterable<String> names) {
    final ImmutableList.Builder<Pair<String, Schema>> builder =
        ImmutableList.builder();
    Schema schema = rootSchema.plus();
    final Iterator<String> iterator = names.iterator();
    if (!iterator.hasNext()) {
      return PathImpl.EMPTY;
    }
    if (!rootSchema.name.isEmpty()) {
      Preconditions.checkState(rootSchema.name.equals(iterator.next()));
    }
    for (;;) {
      final String name = iterator.next();
      builder.add(Pair.of(name, schema));
      if (!iterator.hasNext()) {
        return path(builder.build());
      }
      schema = schema.getSubSchema(name);
    }
  }

  public static PathImpl path(ImmutableList<Pair<String, Schema>> build) {
    return new PathImpl(build);
  }

  /** Returns the path to get to a schema from its root. */
  public static Path path(SchemaPlus schema) {
    List<Pair<String, Schema>> list = new ArrayList<>();
    for (SchemaPlus s = schema; s != null; s = s.getParentSchema()) {
      list.add(Pair.of(s.getName(), s));
    }
    return new PathImpl(ImmutableList.copyOf(Lists.reverse(list)));
  }

  /** Dummy data context that has no variables. */
  private static class DummyDataContext implements DataContext {
    private final CalciteConnection connection;
    private final SchemaPlus rootSchema;
    private final ImmutableMap<String, Object> map;

    DummyDataContext(CalciteConnection connection, SchemaPlus rootSchema) {
      this.connection = connection;
      this.rootSchema = rootSchema;
      this.map = ImmutableMap.of();
    }

    public SchemaPlus getRootSchema() {
      return rootSchema;
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

  /** Implementation of {@link Path}. */
  private static class PathImpl
      extends AbstractList<Pair<String, Schema>> implements Path {
    private final ImmutableList<Pair<String, Schema>> pairs;

    private static final PathImpl EMPTY =
        new PathImpl(ImmutableList.of());

    PathImpl(ImmutableList<Pair<String, Schema>> pairs) {
      this.pairs = pairs;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof PathImpl
          && pairs.equals(((PathImpl) o).pairs);
    }

    @Override public int hashCode() {
      return pairs.hashCode();
    }

    public Pair<String, Schema> get(int index) {
      return pairs.get(index);
    }

    public int size() {
      return pairs.size();
    }

    public Path parent() {
      if (pairs.isEmpty()) {
        throw new IllegalArgumentException("at root");
      }
      return new PathImpl(pairs.subList(0, pairs.size() - 1));
    }

    public List<String> names() {
      return new AbstractList<String>() {
        public String get(int index) {
          return pairs.get(index + 1).left;
        }

        public int size() {
          return pairs.size() - 1;
        }
      };
    }

    public List<Schema> schemas() {
      return Pair.right(pairs);
    }
  }
}

// End Schemas.java
