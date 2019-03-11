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

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Physical type of a row.
 *
 * <p>Consists of the SQL row type (returned by {@link #getRowType()}), the Java
 * type of the row (returned by {@link #getJavaRowType()}), and methods to
 * generate expressions to access fields, generate records, and so forth.
 * Together, the records encapsulate how the logical type maps onto the physical
 * type.</p>
 */
public interface PhysType {
  /** Returns the Java type (often a Class) that represents a row. For
   * example, in one row format, always returns {@code Object[].class}. */
  Type getJavaRowType();

  /**
   * Returns the Java class that is used to store the field with the given
   * ordinal.
   *
   * <p>For instance, when the java row type is {@code Object[]}, the java
   * field type is {@code Object} even if the field is not nullable.</p> */
  Type getJavaFieldType(int field);

  /** Returns the physical type of a field. */
  PhysType field(int ordinal);

  /** Returns the physical type of a given field's component type. */
  PhysType component(int field);

  /** Returns the SQL row type. */
  RelDataType getRowType();

  /** Returns the Java class of the field with the given ordinal. */
  Class fieldClass(int field);

  /** Returns whether a given field allows null values. */
  boolean fieldNullable(int index);

  /** Generates a reference to a given field in an expression.
   *
   * <p>For example given {@code expression=employee} and {@code field=2},
   * generates
   *
   * <blockquote><pre>{@code employee.deptno}</pre></blockquote>
   *
   * @param expression Expression
   * @param field Ordinal of field
   * @return Expression to access the field of the expression
   */
  Expression fieldReference(Expression expression, int field);

  /** Generates a reference to a given field in an expression.
   *
   * <p>This method optimizes for the target storage type (i.e. avoids
   * casts).
   *
   * <p>For example given {@code expression=employee} and {@code field=2},
   * generates
   *
   * <blockquote><pre>{@code employee.deptno}</pre></blockquote>
   *
   * @param expression Expression
   * @param field Ordinal of field
   * @param storageType optional hint for storage class
   * @return Expression to access the field of the expression
   */
  Expression fieldReference(Expression expression, int field,
      Type storageType);

  /** Generates an accessor function for a given list of fields.  The resulting
   * object is a {@link List} (implementing {@link Object#hashCode()} and
   * {@link Object#equals(Object)} per that interface) and also implements
   * {@link Comparable}.
   *
   * <p>For example:
   *
   * <blockquote><pre>
   * new Function1&lt;Employee, Object[]&gt; {
   *    public Object[] apply(Employee v1) {
   *        return FlatLists.of(v1.&lt;fieldN&gt;, v1.&lt;fieldM&gt;);
   *    }
   * }
   * }</pre></blockquote>
   */
  Expression generateAccessor(List<Integer> fields);

  /** Generates a selector for the given fields from an expression, with the
   * default row format. */
  Expression generateSelector(
      ParameterExpression parameter,
      List<Integer> fields);

  /** Generates a lambda expression that is a selector for the given fields from
   * an expression. */
  Expression generateSelector(
      ParameterExpression parameter,
      List<Integer> fields,
      JavaRowFormat targetFormat);

  /** Generates a lambda expression that is a selector for the given fields from
   * an expression.
   *
   * <p>{@code usedFields} must be a subset of {@code fields}.
   * For each field, there is a corresponding indicator field.
   * If a field is used, its value is assigned and its indicator is left
   * {@code false}.
   * If a field is not used, its value is not assigned and its indicator is
   * set to {@code true};
   * This will become a value of 1 when {@code GROUPING(field)} is called. */
  Expression generateSelector(
      ParameterExpression parameter,
      List<Integer> fields,
      List<Integer> usedFields,
      JavaRowFormat targetFormat);

  /** Generates a selector for the given fields from an expression.
   * Only used by EnumerableWindow. */
  Pair<Type, List<Expression>> selector(
      ParameterExpression parameter,
      List<Integer> fields,
      JavaRowFormat targetFormat);

  /** Projects a given collection of fields from this input record, into
   * a particular preferred output format. The output format is optimized
   * if there are 0 or 1 fields. */
  PhysType project(
      List<Integer> integers,
      JavaRowFormat format);

  /** Projects a given collection of fields from this input record, optionally
   * with indicator fields, into a particular preferred output format.
   *
   * <p>The output format is optimized if there are 0 or 1 fields
   * and indicators are disabled. */
  PhysType project(
      List<Integer> integers,
      boolean indicator,
      JavaRowFormat format);

  /** Returns a lambda to create a collation key and a comparator. The
   * comparator is sometimes null. */
  Pair<Expression, Expression> generateCollationKey(
      List<RelFieldCollation> collations);

  /** Returns a comparator. Unlike the comparator returned by
   * {@link #generateCollationKey(java.util.List)}, this comparator acts on the
   * whole element. */
  Expression generateComparator(
      RelCollation collation);

  /** Returns a expression that yields a comparer, or null if this type
   * is comparable. */
  Expression comparer();

  /** Generates an expression that creates a record for a row, initializing
   * its fields with the given expressions. There must be one expression per
   * field.
   *
   * @param expressions Expression to initialize each field
   * @return Expression to create a row
   */
  Expression record(List<Expression> expressions);

  /** Returns the format. */
  JavaRowFormat getFormat();

  List<Expression> accessors(Expression parameter, List<Integer> argList);

  /** Returns a copy of this type that allows nulls if {@code nullable} is
   * true. */
  PhysType makeNullable(boolean nullable);

  /** Converts an enumerable of this physical type to an enumerable that uses a
   * given physical type for its rows.
   *
   * @deprecated Use {@link #convertTo(Expression, JavaRowFormat)}.
   * The use of PhysType as a second parameter is misleading since only the row
   * format of the expression is affected by the conversion. Moreover it requires
   * to have at hand a PhysType object which is not really necessary for achieving
   * the desired result. */
  @Deprecated // to be removed before 2.0
  Expression convertTo(Expression expression, PhysType targetPhysType);

  /** Converts an enumerable of this physical type to an enumerable that uses
   * the <code>targetFormat</code> for representing its rows. */
  Expression convertTo(Expression expression, JavaRowFormat targetFormat);
}

// End PhysType.java
