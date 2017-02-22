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
package org.apache.calcite.schema.impl;

import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Implementation of {@link org.apache.calcite.schema.TableMacro} based on a
 * method.
*/
public class TableMacroImpl extends ReflectiveFunctionBase
    implements TableMacro {

  /** Private constructor; use {@link #create}. */
  private TableMacroImpl(Method method) {
    super(method);
  }

  /** Creates a {@code TableMacro} from a class, looking for an "eval"
   * method. Returns null if there is no such method. */
  public static TableMacro create(Class<?> clazz) {
    final Method method = findMethod(clazz, "eval");
    if (method == null) {
      return null;
    }
    return create(method);
  }

  /** Creates a {@code TableMacro} from a method. */
  public static TableMacro create(final Method method) {
    Class clazz = method.getDeclaringClass();
    if (!Modifier.isStatic(method.getModifiers())) {
      if (!classHasPublicZeroArgsConstructor(clazz)) {
        throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex();
      }
    }
    final Class<?> returnType = method.getReturnType();
    if (!TranslatableTable.class.isAssignableFrom(returnType)) {
      return null;
    }
    return new TableMacroImpl(method);
  }

  /**
   * Applies arguments to yield a table.
   *
   * @param arguments Arguments
   * @return Table
   */
  public TranslatableTable apply(List<Object> arguments) {
    try {
      Object o = null;
      if (!Modifier.isStatic(method.getModifiers())) {
        final Constructor<?> constructor =
            method.getDeclaringClass().getConstructor();
        o = constructor.newInstance();
      }
      //noinspection unchecked
      return (TranslatableTable) method.invoke(o, arguments.toArray());
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Expected "
          + Arrays.toString(method.getParameterTypes()) + " actual "
          + arguments,
          e);
    } catch (IllegalAccessException | InvocationTargetException
        | NoSuchMethodException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }
}

// End TableMacroImpl.java
