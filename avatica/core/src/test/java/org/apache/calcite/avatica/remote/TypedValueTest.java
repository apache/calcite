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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.util.ByteString;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

/**
 * Test serialization of TypedValue.
 */
public class TypedValueTest {

  private void serializeAndEqualityCheck(TypedValue value) {
    TypedValue copy = TypedValue.fromProto(value.toProto());

    assertEquals(value.type, copy.type);
    assertEquals(value.value, copy.value);
  }

  @Test
  public void testBoolean() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_BOOLEAN, true));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.BOOLEAN, Boolean.TRUE));
  }

  @Test
  public void testByte() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_BYTE, (byte) 4));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.BYTE, Byte.valueOf((byte) 4)));
  }

  @Test
  public void testShort() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_SHORT, (short) 42));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.SHORT, Short.valueOf((short) 42)));
  }

  @Test
  public void testInteger() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_INT, (int) 42000));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.INTEGER, Integer.valueOf((int) 42000)));
  }

  @Test
  public void testLong() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_LONG, Long.MAX_VALUE));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.LONG, Long.valueOf(Long.MAX_VALUE)));
  }

  @Test
  public void testFloat() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_FLOAT, 3.14159f));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.FLOAT, Float.valueOf(3.14159f)));
  }

  @Test
  public void testDouble() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_DOUBLE, Double.MAX_VALUE));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.DOUBLE, Double.valueOf(Double.MAX_VALUE)));
  }

  @Test
  public void testChar() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_CHAR, 'c'));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.CHARACTER, Character.valueOf('c')));
  }

  @Test
  public void testString() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.STRING, "qwertyasdf"));
  }

  @Test
  public void testByteString() {
    serializeAndEqualityCheck(
        TypedValue.ofLocal(Rep.BYTE_STRING,
            new ByteString("qwertyasdf".getBytes(StandardCharsets.UTF_8))));
  }

  @Test
  public void testSqlDate() {
    // days since epoch
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.JAVA_SQL_DATE, 25));
  }

  @Test
  public void testUtilDate() {
    serializeAndEqualityCheck(
        TypedValue.ofLocal(Rep.JAVA_UTIL_DATE, System.currentTimeMillis()));
  }

  @Test
  public void testSqlTime() {
    // millis since epoch
    serializeAndEqualityCheck(
        TypedValue.ofLocal(Rep.JAVA_SQL_TIME, 42 * 1024 * 1024));
  }

  @Test
  public void testSqlTimestamp() {
    serializeAndEqualityCheck(
        TypedValue.ofLocal(Rep.JAVA_SQL_TIMESTAMP, 42L * 1024 * 1024 * 1024));
  }
}

// End TypedValueTest.java
