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
package org.apache.calcite.runtime;

import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;


import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * HoistedVariables is a container for runtime variables populated by
 * {@link org.apache.calcite.adapter.enumerable.EnumerableRel} implementations.
 */
public class HoistedVariables {
  private final List<VariableSlot> indexedVariables = new ArrayList<>();

  public static final ParameterExpression VARIABLES = Expressions.parameter(Modifier.FINAL,
      HoistedVariables.class, "variables");

  /**
   * Acquires an unbound {@link VariableSlot} that can be later referenced in generated code.
   *
   * @param variableName  a variable name used only for debugging -- it is not required to be unique
   *
   * @return reserved position in the collection of variables
   */
  public Integer registerVariable(final String variableName) {
    indexedVariables.add(new VariableSlot(variableName));
    return Integer.valueOf(indexedVariables.size());
  }

  /**
   * Bounds the variable at variableIndex to the provided value.
   *
   * @param variableIndex the reserved position acquired from {@link #registeredVariable(String)}
   * @param value the value to store. Can be any Object and can be null
   *
   * @throws {@link IllegalArgumentException} when variableIndex hasn't been reserved.
   */
  public void setVariable(Integer variableIndex, Object value) {
    if (variableIndex > indexedVariables.size()) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ENGLISH,
              "No variable at index %d", variableIndex));
    }
    indexedVariables.get(variableIndex - 1)
      .setVariable(value);
  }

  /**
   * Fetches the bound variable at the provided variableIndex slot.
   *
   * @param variableIndex the reserved position returned from {@link #registeredVariable(String)}
   *
   * @return the bound variable for this slot that was set by {@link #setVariable}.
   * @throws {@link IllegalArgumentException} when variableIndex has not been reserved
   * @throws {@link IllegalArgumentException} when the variable at variableIndex hasn't been set.
   */
  public Object getVariable(Integer variableIndex) {
    if (variableIndex > indexedVariables.size()) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ENGLISH,
              "No variable at index %d", variableIndex));
    }
    return indexedVariables.get(variableIndex - 1)
      .getVariable();
  }

  /**
   * Runtime check to ensure all the variables that have been registered have also been set. A
   * A variable is reserved by calling {@link #registerVariable(String)} and a variable is later set
   * by calling {@link #setVariable(Integer, Object)}.
   *
   * @return returns true when all reserved variables have been set
   */
  public boolean allVariablesBound() {
    return this.indexedVariables
      .stream()
      .allMatch(VariableSlot::isSet);
  }

  /**
   * A reserved slot in the variable pool that is expected to be filled at bind time.
   */
  private static class VariableSlot {
    private Object value;
    private boolean variableSet;
    private String variableName;

    VariableSlot(final String variableName) {
      this.value = null;
      this.variableName = variableName;
    }

    public void setVariable(final Object v) {
      // @TODO: if is already variableSet true, throw an error.
      this.value = v;
      variableSet = true;
    }

    public boolean isSet() {
      return variableSet;
    }

    public Object getVariable() {
      if (!variableSet) {
        throw new IllegalArgumentException(
            String.format(
                Locale.ENGLISH, "%s was declared but never set", variableName));
      }
      return value;
    }
  }
}
// End HoistedVariables.java
