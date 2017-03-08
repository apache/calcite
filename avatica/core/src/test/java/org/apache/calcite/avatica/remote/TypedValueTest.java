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
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.util.Base64;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.Calendar;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test serialization of TypedValue.
 */
public class TypedValueTest {

  private void serializeAndEqualityCheck(TypedValue value) {
    TypedValue copy = TypedValue.fromProto(value.toProto());

    assertEquals(value.type, copy.type);
    assertEquals(value.value, copy.value);
  }

  @Test public void testBoolean() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_BOOLEAN, true));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.BOOLEAN, Boolean.TRUE));
  }

  @Test public void testByte() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_BYTE, (byte) 4));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.BYTE, Byte.valueOf((byte) 4)));
  }

  @Test public void testShort() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_SHORT, (short) 42));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.SHORT, Short.valueOf((short) 42)));
  }

  @Test public void testInteger() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_INT, (int) 42000));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.INTEGER, Integer.valueOf((int) 42000)));
  }

  @Test public void testLong() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_LONG, Long.MAX_VALUE));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.LONG, Long.valueOf(Long.MAX_VALUE)));
  }

  @Test public void testFloat() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_FLOAT, 3.14159f));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.FLOAT, Float.valueOf(3.14159f)));
  }

  @Test public void testDouble() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_DOUBLE, Double.MAX_VALUE));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.DOUBLE, Double.valueOf(Double.MAX_VALUE)));
  }

  @Test public void testDecimal() {
    final BigDecimal decimal = new BigDecimal("1.2345");
    final TypedValue decimalTypedValue = TypedValue.ofLocal(Rep.NUMBER, decimal);
    serializeAndEqualityCheck(decimalTypedValue);

    final Common.TypedValue protoTypedValue = decimalTypedValue.toProto();
    assertEquals(Common.Rep.BIG_DECIMAL, protoTypedValue.getType());
    final String strValue = protoTypedValue.getStringValue();
    assertNotNull(strValue);
    assertEquals(decimal.toPlainString(), strValue);
  }

  @Test public void testChar() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.PRIMITIVE_CHAR, 'c'));
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.CHARACTER, Character.valueOf('c')));
  }

  @Test public void testString() {
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.STRING, "qwertyasdf"));
  }

  @Test public void testByteString() {
    serializeAndEqualityCheck(
        TypedValue.ofLocal(Rep.BYTE_STRING,
            new ByteString("qwertyasdf".getBytes(UTF_8))));
  }

  @Test public void testBase64() {
    byte[] bytes = "qwertyasdf".getBytes(UTF_8);
    // Plain bytes get put into protobuf for simplicitly
    Common.TypedValue proto = Common.TypedValue.newBuilder().setBytesValue(
        com.google.protobuf.ByteString.copyFrom(bytes))
        .setType(Common.Rep.BYTE_STRING).build();

    // But we should get back a b64-string to make sure TypedValue doesn't get confused.
    Object deserializedObj = TypedValue.getSerialFromProto(proto);
    assertThat(deserializedObj, is(instanceOf(String.class)));
    assertEquals(new ByteString(bytes).toBase64String(), (String) deserializedObj);

    // But we should get a non-b64 byte array as the JDBC representation
    deserializedObj =
        TypedValue.protoToJdbc(proto, DateTimeUtils.calendar());
    assertThat(deserializedObj, is(instanceOf(byte[].class)));
    assertArrayEquals(bytes, (byte[]) deserializedObj);
  }

  @Test public void testSqlDate() {
    // days since epoch
    serializeAndEqualityCheck(TypedValue.ofLocal(Rep.JAVA_SQL_DATE, 25));
  }

  @Test public void testUtilDate() {
    serializeAndEqualityCheck(
        TypedValue.ofLocal(Rep.JAVA_UTIL_DATE, System.currentTimeMillis()));
  }

  @Test public void testSqlTime() {
    // millis since epoch
    serializeAndEqualityCheck(
        TypedValue.ofLocal(Rep.JAVA_SQL_TIME, 42 * 1024 * 1024));
  }

  @Test public void testSqlTimestamp() {
    serializeAndEqualityCheck(
        TypedValue.ofLocal(Rep.JAVA_SQL_TIMESTAMP, 42L * 1024 * 1024 * 1024));
  }

  @Test public void testLegacyDecimalParsing() {
    final BigDecimal decimal = new BigDecimal("123451234512345");
    final Calendar calendar = DateTimeUtils.calendar();

    // CALCITE-1103 Decimals were (incorrectly) getting serialized as normal "numbers" which
    // caused them to use the numberValue field. TypedValue should still be able to handle
    // values like this (but large values will be truncated and return bad values).
    Common.TypedValue oldProtoStyle = Common.TypedValue.newBuilder().setType(Common.Rep.NUMBER)
        .setNumberValue(decimal.longValue()).build();

    TypedValue fromProtoTv = TypedValue.fromProto(oldProtoStyle);
    Object o = fromProtoTv.toJdbc(calendar);
    assertEquals(decimal, o);
  }

  @Test public void testProtobufBytesNotSentAsBase64() {
    final byte[] bytes = "asdf".getBytes(UTF_8);
    final byte[] b64Bytes = Base64.encodeBytes(bytes).getBytes(UTF_8);
    TypedValue tv = TypedValue.ofLocal(Rep.BYTE_STRING, new ByteString(bytes));
    // JSON encodes it as base64
    assertEquals(new String(b64Bytes, UTF_8), tv.value);

    // Get the protobuf variant
    Common.TypedValue protoTv = tv.toProto();
    Common.Rep protoRep = protoTv.getType();
    assertEquals(Common.Rep.BYTE_STRING, protoRep);

    // The pb variant should have the native bytes of the original value
    com.google.protobuf.ByteString protoByteString = protoTv.getBytesValue();
    assertNotNull(protoByteString);
    assertArrayEquals(bytes, protoByteString.toByteArray());

    // We should have the b64 string as a backwards compatibility feature
    assertEquals(new String(b64Bytes, UTF_8),
        protoTv.getStringValue());
  }

  @Test public void testLegacyBase64StringEncodingForBytes() {
    // CALCITE-1103 CALCITE-1209 We observed that binary data was being
    // serialized as base-64 encoded strings instead of the native binary
    // data type in protobufs. We need to still handle older clients sending
    // data in this form.
    final byte[] bytes = "asdf".getBytes(UTF_8);
    final String base64Str = Base64.encodeBytes(bytes);
    Common.TypedValue.Builder builder = Common.TypedValue.newBuilder();
    builder.setStringValue(base64Str);
    builder.setType(Common.Rep.BYTE_STRING);
    Common.TypedValue protoTv = builder.build();

    TypedValue tv = TypedValue.fromProto(protoTv);
    assertEquals(Rep.BYTE_STRING, tv.type);
    assertEquals(base64Str, tv.value);
  }
}

// End TypedValueTest.java
