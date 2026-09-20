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

import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link SqlFunctions#cast} and {@link SqlFunctions#castArray}, the
 * conversions used when the type of a value is not known until run time.
 *
 * <p>Test case for
 * <a href="https://issues.apache.org/jira/browse/CALCITE-7801">[CALCITE-7801]
 * JSON_VALUE(..., RETURNING DOUBLE) throws ClassCastException when the JSON
 * number is an integer</a>.
 */
class SqlFunctionsCastTest {

  @Test void testCastToScalarType() {
    // ANY, and a null value, are returned unchanged.
    assertThat(cast("x", SqlTypeName.ANY), is("x"));
    assertThat(cast(null, SqlTypeName.INTEGER), nullValue());

    // Exact numerics round towards zero and are range-checked.
    assertThat(cast(1, SqlTypeName.TINYINT), is((byte) 1));
    assertThat(cast(1, SqlTypeName.SMALLINT), is((short) 1));
    assertThat(cast(1, SqlTypeName.BIGINT), is(1L));
    assertThat(cast(1.7d, SqlTypeName.BIGINT), is(1L));
    assertThat(cast(-1.7d, SqlTypeName.SMALLINT), is((short) -1));
    assertThat(cast(new BigInteger("42"), SqlTypeName.BIGINT), is(42L));
    assertThrows(ArithmeticException.class,
        () -> cast(100000, SqlTypeName.TINYINT));
    assertThrows(ArithmeticException.class,
        () -> cast(100000, SqlTypeName.SMALLINT));

    // Approximate numerics.
    assertThat(cast(1, SqlTypeName.REAL), is(1.0f));
    assertThat(cast("1.5", SqlTypeName.REAL), is(1.5f));
    assertThat(cast(1, SqlTypeName.FLOAT), is(1.0d));
    assertThat(cast(1, SqlTypeName.DOUBLE), is(1.0d));

    // A character value takes the same path as a CAST from one.
    assertThat(cast("100", SqlTypeName.BIGINT), is(100L));
    assertThat(cast("true", SqlTypeName.BOOLEAN), is(true));
    assertThrows(NumberFormatException.class,
        () -> cast("abc", SqlTypeName.BIGINT));

    // A target type that is not handled is an error, so that a caller can
    // apply its own error clause rather than the failure escaping.
    assertThrows(CalciteException.class,
        () -> cast("0102", SqlTypeName.VARBINARY));
    assertThrows(CalciteException.class,
        () -> cast(1, SqlTypeName.ARRAY));
  }

  @Test void testCastAppliesPrecisionAndScale() {
    assertThat(cast(100, SqlTypeName.DECIMAL, 5, 2), hasToString("100.00"));
    assertThat(cast("abcdef", SqlTypeName.VARCHAR, 3, -1), is("abc"));
    assertThat(cast("ab", SqlTypeName.CHAR, 4, -1), is("ab  "));

    // Fractional seconds beyond the precision of the type are truncated.
    // The value is a number of milliseconds, so the truncation is visible
    // here in a way that it is not through a result set.
    assertThat(cast("10:20:30.987", SqlTypeName.TIME, 3, -1), is(37230987));
    assertThat(cast("10:20:30.987", SqlTypeName.TIME, 0, -1), is(37230000));
    assertThat(cast("2020-01-01 10:20:30.987", SqlTypeName.TIMESTAMP, 3, -1),
        is(1577874030987L));
    assertThat(cast("2020-01-01 10:20:30.987", SqlTypeName.TIMESTAMP, 0, -1),
        is(1577874030000L));

    // Only a character value converts to a datetime.
    assertThrows(CalciteException.class,
        () -> cast(20200101, SqlTypeName.DATE, -1, -1));
  }

  @Test void testCastArray() {
    assertThat(castArray(Arrays.asList(1, 2), SqlTypeName.DOUBLE, 1),
        is(Arrays.asList(1.0d, 2.0d)));

    // A nested array is converted at every level.
    assertThat(
        castArray(
            Arrays.asList(Arrays.asList(1, 2), Collections.singletonList(3)),
            SqlTypeName.DOUBLE, 2),
        is(
            Arrays.asList(Arrays.asList(1.0d, 2.0d),
                Collections.singletonList(3.0d))));

    // Elements of any type the cast handles, here as the number of days
    // that a DATE is held as. Asserting the value rather than how it is
    // rendered keeps this independent of the default time zone.
    assertThat(
        castArray(Arrays.asList("2020-01-01", "2020-01-02"), SqlTypeName.DATE,
            1),
        is(Arrays.asList(18262, 18263)));

    // ANY, and a null value, are returned unchanged.
    assertThat(castArray(Arrays.asList(1, 2), SqlTypeName.ANY, 1),
        is(Arrays.asList(1, 2)));
    assertThat(castArray(null, SqlTypeName.INTEGER, 1), nullValue());

    // A value that is not an array cannot be converted to one.
    assertThrows(CalciteException.class,
        () ->
            castArray(Collections.singletonMap("x", 1), SqlTypeName.INTEGER,
                1));
  }

  /** Tests that the declared type, not the shape of the value, drives the
   * conversion: a value of the wrong shape is an error rather than an array
   * that does not match the type it is declared to have. */
  @Test void testCastArrayChecksShapeAgainstType() {
    // Declared one deep, but an element is itself an array.
    assertThrows(CalciteException.class,
        () ->
            castArray(Arrays.asList(1, Arrays.asList(2, 3)),
                SqlTypeName.INTEGER, 1));

    // Declared two deep, but the value is flat.
    assertThrows(CalciteException.class,
        () -> castArray(Arrays.asList(1, 2), SqlTypeName.INTEGER, 2));
  }

  private static @Nullable Object cast(@Nullable Object value,
      SqlTypeName typeName) {
    return cast(value, typeName, -1, -1);
  }

  private static @Nullable Object cast(@Nullable Object value,
      SqlTypeName typeName, int precision, int scale) {
    return SqlFunctions.cast(value, typeName, precision, scale,
        RoundingMode.DOWN);
  }

  private static @Nullable Object castArray(@Nullable Object value,
      SqlTypeName elementType, int depth) {
    return SqlFunctions.castArray(value, elementType, -1, -1,
        RoundingMode.DOWN, depth);
  }
}
