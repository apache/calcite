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

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.type.SqlTypeUtil;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility functions for schemas.
 */
public final class Schemas {
  private Schemas() {
    throw new AssertionError("no instances!");
  }

  public static TableFunction resolve(
      String name,
      List<TableFunction> tableFunctions,
      List<RelDataType> argumentTypes) {
    final List<TableFunction> matches = new ArrayList<TableFunction>();
    for (TableFunction member : tableFunctions) {
      if (matches(member, argumentTypes)) {
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
      TableFunction member,
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
}

// End Schemas.java
