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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

/**
 * Test for {@link org.apache.calcite.adapter.enumerable.PhysTypeImpl}.
 */
public final class PhysTypeTest {
  private static final JavaTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test void testFieldClassOnColumnOfOneFieldStructType() {
    RelDataType columnType =
        TYPE_FACTORY.createStructType(
            ImmutableList.of(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)),
            ImmutableList.of("intField"));
    RelDataType rowType =
        TYPE_FACTORY.createStructType(ImmutableList.of(columnType),
            ImmutableList.of("structField"));

    PhysType rowPhysType =
        PhysTypeImpl.of(TYPE_FACTORY, rowType, JavaRowFormat.ARRAY);
    assertThat(rowPhysType.fieldClass(0), is(Object[].class));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test void testFieldClassOnColumnOfTwoFieldStructType() {
    RelDataType columnType =
        TYPE_FACTORY.createStructType(
            ImmutableList.of(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
                TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)),
            ImmutableList.of("intField", "strField"));
    RelDataType rowType =
        TYPE_FACTORY.createStructType(ImmutableList.of(columnType),
            ImmutableList.of("structField"));

    PhysType rowPhysType =
        PhysTypeImpl.of(TYPE_FACTORY, rowType, JavaRowFormat.ARRAY);
    assertThat(rowPhysType.fieldClass(0), is(Object[].class));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3364">[CALCITE-3364]
   * Can't group table function result due to a type cast error if table function
   * returns a row with a single value</a>. */
  @Test void testOneColumnJavaRowFormatConversion() {
    RelDataType rowType =
        TYPE_FACTORY.createStructType(
            ImmutableList.of(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)),
            ImmutableList.of("intField"));
    final PhysType rowPhysType =
        PhysTypeImpl.of(TYPE_FACTORY, rowType, JavaRowFormat.ARRAY, false);
    final Expression e =
        rowPhysType.convertTo(Expressions.parameter(Enumerable.class, "input"),
            JavaRowFormat.SCALAR);
    final String expected = "input.select(new org.apache.calcite.linq4j.function.Function1() {\n"
        + "  public int apply(Object[] o) {\n"
        + "    return org.apache.calcite.runtime.SqlFunctions.toInt(o[0]);\n"
        + "  }\n"
        + "  public Object apply(Object o) {\n"
        + "    return apply(\n"
        + "      (Object[]) o);\n"
        + "  }\n"
        + "}\n"
        + ")";
    assertThat(expected, is(Expressions.toString(e)));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7206">[CALCITE-7206]
   * Duplicate 'compare' method in the Enumerable merge join comparator when the
   * row Java class is Object</a>.
   *
   * <p>When the row is a single column whose Java class is {@link Object} (for
   * example a merge join key of type {@code ANY}), the primary
   * {@code compare(Object, Object)} method and the generated bridge method have
   * the same signature, so the emitted {@link java.util.Comparator} must not
   * declare the bridge method twice or it fails to compile. */
  @Test void testMergeJoinComparatorWithObjectRowHasNoDuplicateBridge() {
    final RelDataType rowType =
        TYPE_FACTORY.createStructType(
            ImmutableList.of(
                TYPE_FACTORY.createSqlType(SqlTypeName.ANY)),
            ImmutableList.of("anyField"));
    final PhysType rowPhysType =
        PhysTypeImpl.of(TYPE_FACTORY, rowType, JavaRowFormat.SCALAR, false);
    final RelCollation collation = RelCollations.of(0);
    final Expression comparator =
        rowPhysType.generateMergeJoinComparator(collation);
    final String generated = Expressions.toString(comparator);
    // The primary compare method is still generated ...
    assertThat(generated, containsString("public int compare(Object v0, Object v1)"));
    // ... but the bridge compare(Object o0, Object o1) would collide with it and
    // must therefore be omitted.
    assertThat(generated, not(containsString("Object o0, Object o1")));
  }
}
