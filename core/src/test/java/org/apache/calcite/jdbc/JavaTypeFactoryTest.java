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
package org.apache.calcite.jdbc;

import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.test.SqlTests;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.lang.reflect.Type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link org.apache.calcite.jdbc.JavaTypeFactoryImpl}.
 */
public final class JavaTypeFactoryTest {
  private static final JavaTypeFactoryImpl TYPE_FACTORY = new JavaTypeFactoryImpl();

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test public void testGetJavaClassWithOneFieldStructDataTypeV1() {
    RelDataType structWithOneField = TYPE_FACTORY.createStructType(OneFieldStruct.class);
    assertEquals(OneFieldStruct.class, TYPE_FACTORY.getJavaClass(structWithOneField));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test public void testGetJavaClassWithOneFieldStructDataTypeV2() {
    RelDataType structWithOneField = TYPE_FACTORY.createStructType(
        ImmutableList.of(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)),
        ImmutableList.of("intField"));
    assertRecordType(TYPE_FACTORY.getJavaClass(structWithOneField));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test public void testGetJavaClassWithTwoFieldsStructDataType() {
    RelDataType structWithTwoFields = TYPE_FACTORY.createStructType(TwoFieldStruct.class);
    assertEquals(TwoFieldStruct.class, TYPE_FACTORY.getJavaClass(structWithTwoFields));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2677">[CALCITE-2677]
   * Struct types with one field are not mapped correctly to Java Classes</a>. */
  @Test public void testGetJavaClassWithTwoFieldsStructDataTypeV2() {
    RelDataType structWithTwoFields = TYPE_FACTORY.createStructType(
        ImmutableList.of(
            TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
            TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR)),
        ImmutableList.of("intField", "strField"));
    assertRecordType(TYPE_FACTORY.getJavaClass(structWithTwoFields));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3029">[CALCITE-3029]
   * Java-oriented field type is wrongly forced to be NOT NULL after being converted to
   * SQL-oriented</a>. */
  @Test public void testFieldNullabilityAfterConvertingToSqlStructType() {
    RelDataType javaStructType = TYPE_FACTORY.createStructType(
        ImmutableList.of(
            TYPE_FACTORY.createJavaType(Integer.class),
            TYPE_FACTORY.createJavaType(int.class)),
        ImmutableList.of("a", "b"));
    RelDataType sqlStructType = TYPE_FACTORY.toSql(javaStructType);
    assertEquals("RecordType(INTEGER a, INTEGER NOT NULL b) NOT NULL",
        SqlTests.getTypeString(sqlStructType));
  }

  private void assertRecordType(Type actual) {
    String errorMessage =
        "Type {" + actual.getTypeName() + "} is not a subtype of Types.RecordType";
    assertTrue(errorMessage, actual instanceof Types.RecordType);
  }

  /***/
  private static class OneFieldStruct {
    public Integer intField;
  }

  /***/
  private static class TwoFieldStruct {
    public Integer intField;
    public String strField;
  }
}

// End JavaTypeFactoryTest.java
