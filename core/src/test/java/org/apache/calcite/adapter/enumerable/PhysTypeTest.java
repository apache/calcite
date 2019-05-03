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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link org.apache.calcite.adapter.enumerable.PhysTypeImpl}.
 */
public final class PhysTypeTest {
  private static final JavaTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test public void testFieldClassOnColumnOfOneFieldStructType() {
    RelDataType columnType = TYPE_FACTORY.createStructType(
        ImmutableList.of(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)),
        ImmutableList.of("intField"));
    RelDataType rowType = TYPE_FACTORY.createStructType(
        ImmutableList.of(columnType),
        ImmutableList.of("structField"));

    PhysType rowPhysType = PhysTypeImpl.of(TYPE_FACTORY, rowType, JavaRowFormat.ARRAY);
    assertEquals(Object[].class, rowPhysType.fieldClass(0));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test public void testFieldClassOnColumnOfTwoFieldStructType() {
    RelDataType columnType = TYPE_FACTORY.createStructType(
        ImmutableList.of(
            TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)),
        ImmutableList.of(
            "intField",
            "strField"));
    RelDataType rowType = TYPE_FACTORY.createStructType(
        ImmutableList.of(columnType),
        ImmutableList.of("structField"));

    PhysType rowPhysType = PhysTypeImpl.of(TYPE_FACTORY, rowType, JavaRowFormat.ARRAY);
    assertEquals(Object[].class, rowPhysType.fieldClass(0));
  }

}

// End PhysTypeTest.java
