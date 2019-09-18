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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.runtime.HoistedVariables;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.base.Preconditions;

/**
 * A relational expression of one of the
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention} calling
 * conventions.
 */
public interface EnumerableRel
    extends RelNode {
  RelFactories.FilterFactory FILTER_FACTORY =
      (input, condition, variablesSet) -> {
        Preconditions.checkArgument(variablesSet.isEmpty(),
            "EnumerableFilter does not allow variables");
        return EnumerableFilter.create(input, condition);
      };

  RelFactories.ProjectFactory PROJECT_FACTORY = EnumerableProject::create;

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a plan for this expression according to a calling convention.
   *
   * @param implementor Implementor
   * @param pref Preferred representation for rows in result expression
   * @param v hoistedVariables instance that can be used to register slots
   * @return Plan for this expression according to a calling convention
   */
  Result implement(EnumerableRelImplementor implementor, Prefer pref, HoistedVariables v);

  /**
   * Creates {@link HoistedVariables} to be used within the {@link Result}
   * objects.
   *
   * @param variables to hoisted into compiled code.
   *
   */
  default void hoistedVariables(HoistedVariables variables) {
    getInputs()
        .stream()
        .forEach(rel -> {
          final EnumerableRel enumerable = (EnumerableRel) rel;
          enumerable.hoistedVariables(variables);
        });
  }

  /** Preferred physical type. */
  enum Prefer {
    /** Records must be represented as arrays. */
    ARRAY,
    /** Consumer would prefer that records are represented as arrays, but can
     * accommodate records represented as objects. */
    ARRAY_NICE,
    /** Records must be represented as objects. */
    CUSTOM,
    /** Consumer would prefer that records are represented as objects, but can
     * accommodate records represented as arrays. */
    CUSTOM_NICE,
    /** Consumer has no preferred representation. */
    ANY;

    public JavaRowFormat preferCustom() {
      return prefer(JavaRowFormat.CUSTOM);
    }

    public JavaRowFormat preferArray() {
      return prefer(JavaRowFormat.ARRAY);
    }

    public JavaRowFormat prefer(JavaRowFormat format) {
      switch (this) {
      case CUSTOM:
        return JavaRowFormat.CUSTOM;
      case ARRAY:
        return JavaRowFormat.ARRAY;
      default:
        return format;
      }
    }

    public Prefer of(JavaRowFormat format) {
      switch (format) {
      case ARRAY:
        return ARRAY;
      default:
        return CUSTOM;
      }
    }
  }

  /** Result of implementing an enumerable relational expression by generating
   * Java code. */
  class Result {
    public final BlockStatement block;

    /**
     * Describes the Java type returned by this relational expression, and the
     * mapping between it and the fields of the logical row type.
     */
    public final PhysType physType;
    public final JavaRowFormat format;

    public Result(BlockStatement block, PhysType physType,
        JavaRowFormat format) {
      this.block = block;
      this.physType = physType;
      this.format = format;
    }
  }

  /**
   * Returns an {@link Expression} that pulls the variable at variableIndex out of
   * {@link HoistedVariables}.
   *
   * @param variableIndex the allocated index within the {@code HoistedVariables}
   * @param type the desired class to cast the Object into.
   *
   * @return an Expression to use within java code to fetch a hoisted variable and cast it.
   */
  static Expression lookupValue(int variableIndex, Class<?> type) {
    return Expressions.convert_(
        Expressions.call(HoistedVariables.VARIABLES,
            BuiltInMethod.HOISTED_VARIABLE_GET.method,
            Expressions.constant(variableIndex)),
        type);
  }
}

// End EnumerableRel.java
