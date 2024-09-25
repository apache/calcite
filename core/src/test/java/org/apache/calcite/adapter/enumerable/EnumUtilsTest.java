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

import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.runtime.XmlFunctions;
import org.apache.calcite.util.BuiltInMethod;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link EnumUtils}.
 */
public final class EnumUtilsTest {

  @Test void testDateTypeToInnerTypeConvert() {
    // java.sql.Date x;
    final ParameterExpression date =
        Expressions.parameter(0, java.sql.Date.class, "x");
    final Expression dateToInt =
        EnumUtils.convert(date, int.class);
    final Expression dateToInteger =
        EnumUtils.convert(date, Integer.class);
    assertThat(Expressions.toString(dateToInt),
        is("org.apache.calcite.runtime.SqlFunctions.toInt(x)"));
    assertThat(Expressions.toString(dateToInteger),
        is("org.apache.calcite.runtime.SqlFunctions.toIntOptional(x)"));

    // java.sql.Time x;
    final ParameterExpression time =
        Expressions.parameter(0, java.sql.Time.class, "x");
    final Expression timeToInt =
        EnumUtils.convert(time, int.class);
    final Expression timeToInteger =
        EnumUtils.convert(time, Integer.class);
    assertThat(Expressions.toString(timeToInt),
        is("org.apache.calcite.runtime.SqlFunctions.toInt(x)"));
    assertThat(Expressions.toString(timeToInteger),
        is("org.apache.calcite.runtime.SqlFunctions.toIntOptional(x)"));

    // java.sql.TimeStamp x;
    final ParameterExpression timestamp =
        Expressions.parameter(0, java.sql.Timestamp.class, "x");
    final Expression timeStampToLongPrimitive =
        EnumUtils.convert(timestamp, long.class);
    final Expression timeStampToLong =
        EnumUtils.convert(timestamp, Long.class);
    assertThat(Expressions.toString(timeStampToLongPrimitive),
        is("org.apache.calcite.runtime.SqlFunctions.toLong(x)"));
    assertThat(Expressions.toString(timeStampToLong),
        is("org.apache.calcite.runtime.SqlFunctions.toLongOptional(x)"));
  }

  @Test void testTypeConvertFromPrimitiveToBox() {
    final Expression intVariable =
        Expressions.parameter(0, int.class, "intV");

    // (byte)(int) -> Byte: Byte.valueOf((byte) intV)
    final Expression bytePrimitiveConverted =
        Expressions.convert_(intVariable, byte.class);
    final Expression converted0 =
        EnumUtils.convert(bytePrimitiveConverted, Byte.class);
    assertThat(Expressions.toString(converted0),
        is("Byte.valueOf((byte) intV)"));

    // (char)(int) -> Character: Character.valueOf((char) intV)
    final Expression characterPrimitiveConverted =
        Expressions.convert_(intVariable, char.class);
    final Expression converted1 =
        EnumUtils.convert(characterPrimitiveConverted, Character.class);
    assertThat(Expressions.toString(converted1),
        is("Character.valueOf((char) intV)"));

    // (short)(int) -> Short: Short.valueOf((short) intV)
    final Expression shortPrimitiveConverted =
        Expressions.convert_(intVariable, short.class);
    final Expression converted2 =
        EnumUtils.convert(shortPrimitiveConverted, Short.class);
    assertThat(Expressions.toString(converted2),
        is("Short.valueOf((short) intV)"));

    // (long)(int) -> Long: Long.valueOf(intV)
    final Expression longPrimitiveConverted =
        Expressions.convert_(intVariable, long.class);
    final Expression converted3 =
        EnumUtils.convert(longPrimitiveConverted, Long.class);
    assertThat(Expressions.toString(converted3),
        is("Long.valueOf(intV)"));

    // (float)(int) -> Float: Float.valueOf(intV)
    final Expression floatPrimitiveConverted =
        Expressions.convert_(intVariable, float.class);
    final Expression converted4 =
        EnumUtils.convert(floatPrimitiveConverted, Float.class);
    assertThat(Expressions.toString(converted4),
        is("Float.valueOf(intV)"));

    // (double)(int) -> Double: Double.valueOf(intV)
    final Expression doublePrimitiveConverted =
        Expressions.convert_(intVariable, double.class);
    final Expression converted5 =
        EnumUtils.convert(doublePrimitiveConverted, Double.class);
    assertThat(Expressions.toString(converted5),
        is("Double.valueOf(intV)"));

    final Expression byteConverted =
        EnumUtils.convert(intVariable, Byte.class);
    assertThat(Expressions.toString(byteConverted),
        is("Byte.valueOf((byte) intV)"));

    final Expression shortConverted =
        EnumUtils.convert(intVariable, Short.class);
    assertThat(Expressions.toString(shortConverted),
        is("Short.valueOf((short) intV)"));

    final Expression integerConverted =
        EnumUtils.convert(intVariable, Integer.class);
    assertThat(Expressions.toString(integerConverted),
        is("Integer.valueOf(intV)"));

    final Expression longConverted =
        EnumUtils.convert(intVariable, Long.class);
    assertThat(Expressions.toString(longConverted),
        is("Long.valueOf((long) intV)"));

    final Expression floatConverted =
        EnumUtils.convert(intVariable, Float.class);
    assertThat(Expressions.toString(floatConverted),
        is("Float.valueOf((float) intV)"));

    final Expression doubleConverted =
        EnumUtils.convert(intVariable, Double.class);
    assertThat(Expressions.toString(doubleConverted),
        is("Double.valueOf((double) intV)"));
  }

  @Test void testTypeConvertToString() {
    // Constant Expression: "null"
    final ConstantExpression nullLiteral1 = Expressions.constant(null);
    // Constant Expression: "(Object) null"
    final ConstantExpression nullLiteral2 = Expressions.constant(null, Object.class);
    final Expression e1 = EnumUtils.convert(nullLiteral1, String.class);
    final Expression e2 = EnumUtils.convert(nullLiteral2, String.class);
    assertThat(Expressions.toString(e1), is("(String) null"));
    assertThat(Expressions.toString(e2), is("(String) (Object) null"));
  }

  @Test void testMethodCallExpression() {
    // test for Object.class method parameter type
    final ConstantExpression arg0 = Expressions.constant(1, int.class);
    final ConstantExpression arg1 = Expressions.constant("x", String.class);
    final MethodCallExpression arrayMethodCall =
        EnumUtils.call(null, SqlFunctions.class,
            BuiltInMethod.ARRAY.getMethodName(), Arrays.asList(arg0, arg1));
    assertThat(Expressions.toString(arrayMethodCall),
        is("org.apache.calcite.runtime.SqlFunctions.array(1, \"x\")"));

    // test for Object.class argument type
    final ConstantExpression nullLiteral = Expressions.constant(null);
    final MethodCallExpression xmlExtractMethodCall =
        EnumUtils.call(null, XmlFunctions.class,
            BuiltInMethod.EXTRACT_VALUE.getMethodName(),
            Arrays.asList(arg1, nullLiteral));
    assertThat(Expressions.toString(xmlExtractMethodCall),
        is("org.apache.calcite.runtime.XmlFunctions.extractValue(\"x\", (String) null)"));

    // test "mod(decimal, long)" match to "mod(decimal, decimal)"
    final ConstantExpression arg2 = Expressions.constant(12.5, BigDecimal.class);
    final ConstantExpression arg3 = Expressions.constant(3, long.class);
    final MethodCallExpression modMethodCall =
        EnumUtils.call(null, SqlFunctions.class, "mod",
            Arrays.asList(arg2, arg3));
    assertThat(Expressions.toString(modMethodCall),
        is("org.apache.calcite.runtime.SqlFunctions.mod("
            + "java.math.BigDecimal.valueOf(125L, 1), "
            + "new java.math.BigDecimal(\n  3L))"));

    // test "ST_MakePoint(int, int)" match to "ST_MakePoint(decimal, decimal)"
    final ConstantExpression arg4 = Expressions.constant(1, int.class);
    final ConstantExpression arg5 = Expressions.constant(2, int.class);
    final MethodCallExpression geoMethodCall =
        EnumUtils.call(null, SpatialTypeFunctions.class, "ST_MakePoint",
            Arrays.asList(arg4, arg5));
    assertThat(Expressions.toString(geoMethodCall),
        is("org.apache.calcite.runtime.SpatialTypeFunctions.ST_MakePoint("
            + "new java.math.BigDecimal(\n  1), "
            + "new java.math.BigDecimal(\n  2))"));
  }
}
