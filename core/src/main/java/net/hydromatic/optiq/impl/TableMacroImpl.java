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
package net.hydromatic.optiq.impl;

import net.hydromatic.optiq.FunctionParameter;
import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.TableMacro;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.eigenbase.util.Static.*;

/**
 * Implementation of {@link TableMacro} based on a method.
*/
public class TableMacroImpl implements TableMacro {
  private final List<FunctionParameter> parameters;
  private final Method method;

  /** Private constructor; use {@link #create}. */
  private TableMacroImpl(List<FunctionParameter> parameters, Method method) {
    this.parameters = parameters;
    this.method = method;
  }

  /** Creates a {@code TableMacroImpl} from a class, looking for an "eval"
   * method. Returns null if there is no such method. */
  public static TableMacro create(Class<?> clazz) {
    final Method method = ScalarFunctionImpl.findMethod(clazz, "eval");
    if (method == null) {
      return null;
    }
    if ((method.getModifiers() & Modifier.STATIC) == 0) {
      if (!ScalarFunctionImpl.classHasPublicZeroArgsConstructor(clazz)) {
        throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex();
      }
    }
    final Class<?> returnType = method.getReturnType();
    if (!Table.class.isAssignableFrom(returnType)) {
      return null;
    }
    return create(method);
  }

  /** Creates a {@code TableMacroImpl} from a method. */
  public static TableMacro create(final Method method) {
    final List<FunctionParameter> parameters =
        new ArrayList<FunctionParameter>();
    for (final Class<?> parameterType : method.getParameterTypes()) {
      parameters.add(
          new FunctionParameter() {
            final int ordinal = parameters.size();

            public int getOrdinal() {
              return ordinal;
            }

            public String getName() {
              return "a" + ordinal;
            }

            public RelDataType getType(RelDataTypeFactory typeFactory) {
              return ((JavaTypeFactory) typeFactory).createType(parameterType);
            }
          }
      );
    }
    return new TableMacroImpl(parameters, method);
  }

  public List<FunctionParameter> getParameters() {
    return parameters;
  }

  public Table apply(List<Object> arguments) {
    try {
      Object o = null;
      if (!Modifier.isStatic(method.getModifiers())) {
        o = method.getDeclaringClass().newInstance();
      }
      //noinspection unchecked
      return (Table) method.invoke(o, arguments.toArray());
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Expected "
          + Arrays.asList(method.getParameterTypes()) + " actual "
          + arguments,
          e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final Class<?> returnType = method.getReturnType();
    return ((JavaTypeFactory) typeFactory).createType(returnType);
  }
}

// End TableMacroImpl.java
