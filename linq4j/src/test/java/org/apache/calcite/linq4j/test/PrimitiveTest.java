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
package org.apache.calcite.linq4j.test;

import org.apache.calcite.linq4j.tree.Primitive;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit test for {@link Primitive}.
 */
class PrimitiveTest {
  @Test void testIsAssignableFrom() {
    assertTrue(Primitive.INT.assignableFrom(Primitive.BYTE));
    assertTrue(Primitive.INT.assignableFrom(Primitive.SHORT));
    assertTrue(Primitive.INT.assignableFrom(Primitive.CHAR));
    assertTrue(Primitive.INT.assignableFrom(Primitive.INT));
    assertTrue(Primitive.INT.assignableFrom(Primitive.SHORT));
    assertFalse(Primitive.INT.assignableFrom(Primitive.LONG));
    assertFalse(Primitive.INT.assignableFrom(Primitive.FLOAT));
    assertFalse(Primitive.INT.assignableFrom(Primitive.DOUBLE));

    assertTrue(Primitive.LONG.assignableFrom(Primitive.BYTE));
    assertTrue(Primitive.LONG.assignableFrom(Primitive.SHORT));
    assertTrue(Primitive.LONG.assignableFrom(Primitive.CHAR));
    assertTrue(Primitive.LONG.assignableFrom(Primitive.INT));
    assertTrue(Primitive.LONG.assignableFrom(Primitive.LONG));
    assertFalse(Primitive.LONG.assignableFrom(Primitive.FLOAT));
    assertFalse(Primitive.LONG.assignableFrom(Primitive.DOUBLE));

    // SHORT and CHAR cannot be assigned to each other

    assertTrue(Primitive.SHORT.assignableFrom(Primitive.BYTE));
    assertTrue(Primitive.SHORT.assignableFrom(Primitive.SHORT));
    assertFalse(Primitive.SHORT.assignableFrom(Primitive.CHAR));
    assertFalse(Primitive.SHORT.assignableFrom(Primitive.INT));
    assertFalse(Primitive.SHORT.assignableFrom(Primitive.LONG));
    assertFalse(Primitive.SHORT.assignableFrom(Primitive.FLOAT));
    assertFalse(Primitive.SHORT.assignableFrom(Primitive.DOUBLE));

    assertFalse(Primitive.CHAR.assignableFrom(Primitive.BYTE));
    assertFalse(Primitive.CHAR.assignableFrom(Primitive.SHORT));
    assertTrue(Primitive.CHAR.assignableFrom(Primitive.CHAR));
    assertFalse(Primitive.CHAR.assignableFrom(Primitive.INT));
    assertFalse(Primitive.CHAR.assignableFrom(Primitive.LONG));
    assertFalse(Primitive.CHAR.assignableFrom(Primitive.FLOAT));
    assertFalse(Primitive.CHAR.assignableFrom(Primitive.DOUBLE));

    assertTrue(Primitive.DOUBLE.assignableFrom(Primitive.BYTE));
    assertTrue(Primitive.DOUBLE.assignableFrom(Primitive.SHORT));
    assertTrue(Primitive.DOUBLE.assignableFrom(Primitive.CHAR));
    assertTrue(Primitive.DOUBLE.assignableFrom(Primitive.INT));
    assertTrue(Primitive.DOUBLE.assignableFrom(Primitive.LONG));
    assertTrue(Primitive.DOUBLE.assignableFrom(Primitive.FLOAT));
    assertTrue(Primitive.DOUBLE.assignableFrom(Primitive.DOUBLE));

    // cross-family assignments

    assertFalse(Primitive.BOOLEAN.assignableFrom(Primitive.INT));
    assertFalse(Primitive.INT.assignableFrom(Primitive.BOOLEAN));
  }

  @Test void testBox() {
    assertThat(Primitive.box(String.class), is(String.class));
    assertThat(Primitive.box(int.class), is(Integer.class));
    assertThat(Primitive.box(Integer.class), is(Integer.class));
    assertThat(Primitive.box(boolean[].class), is(boolean[].class));
  }

  @Test void testOfBox() {
    assertThat(Primitive.ofBox(Integer.class), is(Primitive.INT));
    assertNull(Primitive.ofBox(int.class));
    assertNull(Primitive.ofBox(String.class));
    assertNull(Primitive.ofBox(Integer[].class));
  }

  @Test void testOfBoxOr() {
    assertThat(Primitive.ofBox(Integer.class), is(Primitive.INT));
    assertNull(Primitive.ofBox(int.class));
    assertNull(Primitive.ofBox(String.class));
    assertNull(Primitive.ofBox(Integer[].class));
  }

  /** Tests the {@link Primitive#number(Number)} method. */
  @Test void testNumber() {
    Number number = Primitive.SHORT.number(2);
    assertThat(number, instanceOf(Short.class));
    assertThat(number.shortValue(), is((short) 2));

    number = Primitive.FLOAT.number(2);
    assertThat(number, instanceOf(Float.class));
    assertThat(number.doubleValue(), is(2.0d));

    try {
      number = Primitive.INT.number(null);
      fail("expected exception, got " + number);
    } catch (NullPointerException e) {
      // ok
    }

    // not a number
    try {
      number = Primitive.CHAR.number(3);
      fail("expected exception, got " + number);
    } catch (AssertionError e) {
      assertThat(e.getMessage(), is("CHAR: 3"));
    }

    // not a number
    try {
      number = Primitive.BOOLEAN.number(null);
      fail("expected exception, got " + number);
    } catch (AssertionError e) {
      assertThat(e.getMessage(), is("BOOLEAN: null"));
    }
  }

  /** Test for
   * {@link Primitive#send(org.apache.calcite.linq4j.tree.Primitive.Source, org.apache.calcite.linq4j.tree.Primitive.Sink)}. */
  @Test void testSendSource() {
    final List<Object> list = new ArrayList<>();
    for (Primitive primitive : Primitive.values()) {
      primitive.send(
          new Primitive.Source() {
            public boolean getBoolean() {
              list.add(boolean.class);
              return true;
            }

            public byte getByte() {
              list.add(byte.class);
              return 0;
            }

            public char getChar() {
              list.add(char.class);
              return 0;
            }

            public short getShort() {
              list.add(short.class);
              return 0;
            }

            public int getInt() {
              list.add(int.class);
              return 0;
            }

            public long getLong() {
              list.add(long.class);
              return 0;
            }

            public float getFloat() {
              list.add(float.class);
              return 0;
            }

            public double getDouble() {
              list.add(double.class);
              return 0;
            }

            public Object getObject() {
              list.add(Object.class);
              return 0;
            }
          },
          new Primitive.Sink() {
            public void set(boolean v) {
              list.add(boolean.class);
              list.add(v);
            }

            public void set(byte v) {
              list.add(byte.class);
              list.add(v);
            }

            public void set(char v) {
              list.add(char.class);
              list.add(v);
            }

            public void set(short v) {
              list.add(short.class);
              list.add(v);
            }

            public void set(int v) {
              list.add(int.class);
              list.add(v);
            }

            public void set(long v) {
              list.add(long.class);
              list.add(v);
            }

            public void set(float v) {
              list.add(float.class);
              list.add(v);
            }

            public void set(double v) {
              list.add(double.class);
              list.add(v);
            }

            public void set(Object v) {
              list.add(Object.class);
              list.add(v);
            }
          });
    }
    assertThat(list,
        hasToString("[boolean, boolean, true, "
            + "byte, byte, 0, "
            + "char, char, \u0000, "
            + "short, short, 0, "
            + "int, int, 0, "
            + "long, long, 0, "
            + "float, float, 0.0, "
            + "double, double, 0.0, "
            + "class java.lang.Object, class java.lang.Object, 0, "
            + "class java.lang.Object, class java.lang.Object, 0]"));
  }

  /** Test for {@link Primitive#permute(Object, int[])}. */
  @Test void testPermute() {
    char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g'};
    int[] sources = {1, 2, 3, 4, 5, 6, 0};
    final Object permute = Primitive.CHAR.permute(chars, sources);
    assertThat(permute, instanceOf(char[].class));
    assertThat(String.valueOf((char[]) permute), is("bcdefga"));
  }

  /** Test for {@link Primitive#arrayToString(Object)}. */
  @Test void testArrayToString() {
    char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g'};
    assertThat(Primitive.CHAR.arrayToString(chars),
        is("[a, b, c, d, e, f, g]"));
  }

  /** Test for {@link Primitive#sortArray(Object)}. */
  @Test void testArraySort() {
    char[] chars = {'m', 'o', 'n', 'o', 'l', 'a', 'k', 'e'};
    Primitive.CHAR.sortArray(chars);
    assertThat(Primitive.CHAR.arrayToString(chars),
        is("[a, e, k, l, m, n, o, o]"));

    // mixed true and false
    boolean[] booleans0 = {true, false, true, true, false};
    Primitive.BOOLEAN.sortArray(booleans0);
    assertThat(Primitive.BOOLEAN.arrayToString(booleans0),
        is("[false, false, true, true, true]"));

    // all false
    boolean[] booleans1 = {false, false, false, false, false};
    Primitive.BOOLEAN.sortArray(booleans1);
    assertThat(Primitive.BOOLEAN.arrayToString(booleans1),
        is("[false, false, false, false, false]"));

    // all true
    boolean[] booleans2 = {true, true, true, true, true};
    Primitive.BOOLEAN.sortArray(booleans2);
    assertThat(Primitive.BOOLEAN.arrayToString(booleans2),
        is("[true, true, true, true, true]"));

    // empty
    boolean[] booleans3 = {};
    Primitive.BOOLEAN.sortArray(booleans3);
    assertThat(Primitive.BOOLEAN.arrayToString(booleans3), is("[]"));

    // ranges specified
    boolean[] booleans4 = {true, true, false, false, true, false, false};
    Primitive.BOOLEAN.sortArray(booleans4, 1, 6);
    assertThat(Primitive.BOOLEAN.arrayToString(booleans4),
        is("[true, false, false, false, true, true, false]"));
  }
}
