/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelProtoDataType;
import org.eigenbase.sql.type.SqlTypeUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.*;

/**
 * Utility functions for schemas.
 */
public final class Schemas {
  private Schemas() {
    throw new AssertionError("no instances!");
  }

  public static OptiqSchema.TableFunctionEntry resolve(
      RelDataTypeFactory typeFactory,
      String name,
      Collection<OptiqSchema.TableFunctionEntry> tableFunctions,
      List<RelDataType> argumentTypes) {
    final List<OptiqSchema.TableFunctionEntry> matches =
        new ArrayList<OptiqSchema.TableFunctionEntry>();
    for (OptiqSchema.TableFunctionEntry member : tableFunctions) {
      if (matches(typeFactory, member.getTableFunction(), argumentTypes)) {
        matches.add(member);
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
      TableFunction member, List<RelDataType> argumentTypes) {
    List<Parameter> parameters = member.getParameters();
    if (parameters.size() != argumentTypes.size()) {
      return false;
    }
    for (int i = 0; i < argumentTypes.size(); i++) {
      RelDataType argumentType = argumentTypes.get(i);
      Parameter parameter = parameters.get(i);
      if (!canConvert(argumentType, parameter.getType(typeFactory))) {
        return false;
      }
    }
    return true;
  }

  private static boolean canConvert(RelDataType fromType, RelDataType toType) {
    return SqlTypeUtil.canAssignFrom(toType, fromType);
  }

  public static TableFunction methodMember(
      final Method method,
      final JavaTypeFactory typeFactory) {
    final List<Parameter> parameters = new ArrayList<Parameter>();
    for (final Class<?> parameterType : method.getParameterTypes()) {
      parameters.add(
          new Parameter() {
            final int ordinal = parameters.size();
            final RelDataType type =
                typeFactory.createType(parameterType);

            public int getOrdinal() {
              return ordinal;
            }

            public String getName() {
              return "a" + ordinal;
            }

            public RelDataType getType(RelDataTypeFactory typeFactory) {
              return type;
            }
          }
      );
    }
    return new TableFunction() {
      public List<Parameter> getParameters() {
        return parameters;
      }

      public Table apply(List<Object> arguments) {
        try {
          //noinspection unchecked
          return (Table) method.invoke(null, arguments.toArray());
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final Class<?> returnType = method.getReturnType();
        return ((JavaTypeFactory) typeFactory).createType(returnType);
      }
    };
  }

  /** Returns the path of a schema, appending the name of a table if not
   * null. */
  public static List<String> path(Schema schema, String name) {
    final List<String> list = new ArrayList<String>();
    if (name != null) {
      list.add(name);
    }
    for (Schema s = schema; s != null; s = s.getParentSchema()) {
      if (s.getParentSchema() != null || !s.getName().equals("")) {
        // Omit the root schema's name from the path if it's the empty string,
        // which it usually is.
        list.add(s.getName());
      }
    }
    return ImmutableList.copyOf(Lists.reverse(list));
  }

  /** Returns the root schema. */
  public static Schema root(Schema schema) {
    for (Schema s = schema;;) {
      Schema previous = s;
      s = s.getParentSchema();
      if (s == null) {
        return previous;
      }
    }
  }

  /** Returns the expression for a sub-schema. */
  public static Expression subSchemaExpression(Schema schema,
      String name, Class type) {
    // (Type) schemaExpression.getSubSchema("name")
    Expression call =
        Expressions.call(schema.getExpression(),
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
  public static Expression tableExpression(Schema schema, Type elementType,
      String tableName, Class clazz) {
    final MethodCallExpression expression;
    if (Table.class.isAssignableFrom(clazz)) {
      expression = Expressions.call(
          schema.getExpression(),
          BuiltinMethod.SCHEMA_GET_TABLE.method,
          Expressions.constant(tableName));
    } else {
      expression = Expressions.call(
          BuiltinMethod.SCHEMAS_QUERYABLE.method,
          DataContext.ROOT,
          schema.getExpression(),
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
    SchemaPlus schema = root.getRootSchema();
    for (int i = 0; i < names.length - 1; i++) {
      String name = names[i];
      schema = schema.getSubSchema(name);
    }
    final String tableName = names[names.length - 1];
    return queryable(root, schema, clazz, tableName);
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
        makeContext(connection, schema, schemaPath);
    OptiqPrepare.Dummy.push(context);
    try {
      return prepare.parse(context, sql);
    } finally {
      OptiqPrepare.Dummy.pop(context);
    }
  }

  /** Prepares a SQL query for execution. For use within Optiq only. */
  public static OptiqPrepare.PrepareResult<Object> prepare(
      final OptiqConnection connection, final OptiqSchema schema,
      final List<String> schemaPath, final String sql) {
    final OptiqPrepare prepare = OptiqPrepare.DEFAULT_FACTORY.apply();
    return prepare.prepareSql(
        makeContext(connection, schema, schemaPath), sql, null, Object[].class,
        -1);
  }

  private static OptiqPrepare.Context makeContext(
      final OptiqConnection connection, final OptiqSchema schema,
      final List<String> schemaPath) {
    if (connection == null) {
      final OptiqPrepare.Context context0 = OptiqPrepare.Dummy.peek();
      return makeContext(context0.config(), context0.getTypeFactory(),
          context0.getDataContext(), schema, schemaPath);
    } else {
      return makeContext(connection.config(), connection.getTypeFactory(),
          createDataContext(connection), schema, schemaPath);
    }
  }

  private static OptiqPrepare.Context makeContext(
      final ConnectionConfig connectionConfig,
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
          return path(schema.schema, null);
        }
        return schemaPath;
      }

      public ConnectionConfig config() {
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
