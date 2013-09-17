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

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Utility functions for schemas.
 */
public final class Schemas {
  private Schemas() {
    throw new AssertionError("no instances!");
  }

  public static Schema.TableFunctionInSchema resolve(
      String name,
      Collection<Schema.TableFunctionInSchema> tableFunctions,
      List<RelDataType> argumentTypes) {
    final List<Schema.TableFunctionInSchema> matches =
        new ArrayList<Schema.TableFunctionInSchema>();
    for (Schema.TableFunctionInSchema member : tableFunctions) {
      if (matches(member.getTableFunction(), argumentTypes)) {
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

  private static boolean matches(
      TableFunction<?> member,
      List<RelDataType> argumentTypes) {
    List<Parameter> parameters = member.getParameters();
    if (parameters.size() != argumentTypes.size()) {
      return false;
    }
    for (int i = 0; i < argumentTypes.size(); i++) {
      RelDataType argumentType = argumentTypes.get(i);
      Parameter parameter = parameters.get(i);
      if (!canConvert(argumentType, parameter.getType())) {
        return false;
      }
    }
    return true;
  }

  private static boolean canConvert(RelDataType fromType, RelDataType toType) {
    return SqlTypeUtil.canAssignFrom(toType, fromType);
  }

  public static <T> TableFunction<T> methodMember(
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

            public RelDataType getType() {
              return type;
            }
          }
      );
    }
    return new TableFunction<T>() {
      public List<Parameter> getParameters() {
        return parameters;
      }

      public Table<T> apply(List<Object> arguments) {
        try {
          //noinspection unchecked
          return (Table) method.invoke(null, arguments.toArray());
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
          throw new RuntimeException(e);
        }
      }

      public Type getElementType() {
        return method.getReturnType();
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

  /** Parses and validates a SQL query. For use within Optiq only. */
  public static OptiqPrepare.ParseResult parse(final Schema schema,
      final List<String> schemaPath, final String sql) {
    final OptiqPrepare prepare = OptiqPrepare.DEFAULT_FACTORY.apply();
    return prepare.parse(makeContext(schema, schemaPath), sql);
  }

  /** Prepares a SQL query for execution. For use within Optiq only. */
  public static OptiqPrepare.PrepareResult<Object> prepare(final Schema schema,
      final List<String> schemaPath,
      final String sql) {
    final OptiqPrepare prepare = OptiqPrepare.DEFAULT_FACTORY.apply();
    return prepare.prepareSql(
        makeContext(schema, schemaPath), sql, null, Object[].class, -1);
  }

  private static OptiqPrepare.Context makeContext(final Schema schema,
      final List<String> schemaPath) {
    final OptiqConnection connection =
        (OptiqConnection) schema.getQueryProvider();
    return new OptiqPrepare.Context() {
      public JavaTypeFactory getTypeFactory() {
        return schema.getTypeFactory();
      }

      public Schema getRootSchema() {
        return connection.getRootSchema();
      }

      public List<String> getDefaultSchemaPath() {
        // schemaPath is usually null. If specified, it overrides schema
        // as the context within which the SQL is validated.
        if (schemaPath == null) {
          return path(schema, null);
        }
        return schemaPath;
      }

      public ConnectionProperty.ConnectionConfig config() {
        return ConnectionProperty.connectionConfig(connection.getProperties());
      }
    };
  }
}

// End Schemas.java
