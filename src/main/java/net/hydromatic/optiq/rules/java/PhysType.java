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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.ParameterExpression;

import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Pair;

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

  /** Returns the SQL row type. */
  RelDataType getRowType();

  /** Returns the Java class of the field with the given ordinal. */
  Class fieldClass(int field);

  /** Returns whether a given field allows null values. */
  boolean fieldNullable(int index);

  /** Generates a reference to a given field in an expression.
   *
   * <p>For example given {@code expression=employee} and {@code field=2},
   * generates</p>
   * <pre>{@code employee.deptno}</pre>
   *
   * @param expression Expression
   * @param field Ordinal of field
   * @return Expression to access the field of the expression
   */
  Expression fieldReference(Expression expression, int field);

  /** Generates an accessor function for a given list of fields.
   *
   * <p>For example:</p>
   * <pre>{@code
   * new Function1<Employee, Object[]> {
   *    public Object[] apply(Employee v1) {
   *        return new Object[] {v1.<fieldN>, v1.<fieldM>};
   *    }
   * }
   * }</pre>
   */
  Expression generateAccessor(List<Integer> fields);

  /** Generates a selector with the default row format. */
  Expression generateSelector(
      ParameterExpression parameter,
      List<Integer> fields);

  /** Projects a given collection of fields from this input record, into
   * a particular preferred output format. The output format is optimized
   * if there are 0 or 1 fields. */
  PhysType project(
      List<Integer> integers,
      JavaRowFormat format);

  /** Returns a lambda to create a collation key and a comparator. The
   * comparator is sometimes null. */
  Pair<Expression, Expression> generateCollationKey(
      List<RelFieldCollation> collations);

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
}

// End PhysType.java
