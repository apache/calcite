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
package net.hydromatic.linq4j.test;

import net.hydromatic.linq4j.expressions.Primitive;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit test for {@link Primitive}.
 */
public class PrimitiveTest {
  @Test public void testIsAssignableFrom() {
    assertTrue(Primitive.INT.assignableFrom(Primitive.BYTE));
    assertTrue(Primitive.INT.assignableFrom(Primitive.SHORT));
    assertTrue(Primitive.INT.assignableFrom(Primitive.CHAR));
    assertTrue(Primitive.INT.assignableFrom(Primitive.INT));
    assertTrue(Primitive.INT.assignableFrom(Primitive.SHORT));
    assertFalse(Primitive.INT.assignableFrom(Primitive.LONG));

    assertTrue(Primitive.LONG.assignableFrom(Primitive.BYTE));
    assertTrue(Primitive.LONG.assignableFrom(Primitive.SHORT));
    assertTrue(Primitive.LONG.assignableFrom(Primitive.CHAR));
    assertTrue(Primitive.LONG.assignableFrom(Primitive.INT));
    assertTrue(Primitive.LONG.assignableFrom(Primitive.LONG));

    // SHORT and CHAR cannot be assigned to each other

    assertTrue(Primitive.SHORT.assignableFrom(Primitive.BYTE));
    assertTrue(Primitive.SHORT.assignableFrom(Primitive.SHORT));
    assertFalse(Primitive.SHORT.assignableFrom(Primitive.CHAR));
    assertFalse(Primitive.SHORT.assignableFrom(Primitive.INT));
    assertFalse(Primitive.SHORT.assignableFrom(Primitive.LONG));

    assertFalse(Primitive.CHAR.assignableFrom(Primitive.BYTE));
    assertFalse(Primitive.CHAR.assignableFrom(Primitive.SHORT));
    assertTrue(Primitive.CHAR.assignableFrom(Primitive.CHAR));
    assertFalse(Primitive.CHAR.assignableFrom(Primitive.INT));
    assertFalse(Primitive.CHAR.assignableFrom(Primitive.LONG));

    // cross-family assignments

    assertFalse(Primitive.BOOLEAN.assignableFrom(Primitive.INT));
    assertFalse(Primitive.INT.assignableFrom(Primitive.BOOLEAN));
  }

  @Test public void testBox() {
    assertEquals(String.class, Primitive.box(String.class));
    assertEquals(Integer.class, Primitive.box(int.class));
    assertEquals(Integer.class, Primitive.box(Integer.class));
    assertEquals(boolean[].class, Primitive.box(boolean[].class));
  }

  @Test public void testOfBox() {
    assertEquals(Primitive.INT, Primitive.ofBox(Integer.class));
    assertNull(Primitive.ofBox(int.class));
    assertNull(Primitive.ofBox(String.class));
    assertNull(Primitive.ofBox(Integer[].class));
  }

  @Test public void testOfBoxOr() {
    assertEquals(Primitive.INT, Primitive.ofBox(Integer.class));
    assertNull(Primitive.ofBox(int.class));
    assertNull(Primitive.ofBox(String.class));
    assertNull(Primitive.ofBox(Integer[].class));
  }

  /** Tests the {@link Primitive#number(Number)} method. */
  @Test public void testNumber() {
    Number number = Primitive.SHORT.number(Integer.valueOf(2));
    assertTrue(number instanceof Short);
    assertEquals(2, number.shortValue());

    number = Primitive.FLOAT.number(Integer.valueOf(2));
    assertTrue(number instanceof Float);
    assertEquals(2.0d, number.doubleValue(), 0d);

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
      // ok
    }

    // not a number
    try {
      number = Primitive.BOOLEAN.number(null);
      fail("expected exception, got " + number);
    } catch (AssertionError e) {
      // ok
    }
  }

  /** Test for {@link Primitive#send(net.hydromatic.linq4j.expressions.Primitive.Source, net.hydromatic.linq4j.expressions.Primitive.Sink)}. */
  @Test public void testSendSource() {
    final List<Object> list = new ArrayList<Object>();
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
    assertEquals(
        "[boolean, boolean, true, "
        + "byte, byte, 0, "
        + "char, char, \u0000, "
        + "short, short, 0, "
        + "int, int, 0, "
        + "long, long, 0, "
        + "float, float, 0.0, "
        + "double, double, 0.0, "
        + "class java.lang.Object, class java.lang.Object, 0, "
        + "class java.lang.Object, class java.lang.Object, 0]",
        list.toString());
  }

  /** Test for {@link Primitive#permute(Object, int[])}. */
  @Test public void testPermute() {
    char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g'};
    int[] sources = {1, 2, 3, 4, 5, 6, 0};
    final Object permute = Primitive.CHAR.permute(chars, sources);
    assertTrue(permute instanceof char[]);
    assertEquals("bcdefga", new String((char[]) permute));
  }

  /** Test for {@link Primitive#arrayToString(Object)}. */
  @Test public void testArrayToString() {
    char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g'};
    assertEquals("[a, b, c, d, e, f, g]", Primitive.CHAR.arrayToString(chars));
  }

  /** Test for {@link Primitive#sortArray(Object)}. */
  @Test public void testArraySort() {
    char[] chars = {'m', 'o', 'n', 'o', 'l', 'a', 'k', 'e'};
    Primitive.CHAR.sortArray(chars);
    assertEquals("[a, e, k, l, m, n, o, o]",
        Primitive.CHAR.arrayToString(chars));

    // mixed true and false
    boolean[] booleans0 = {true, false, true, true, false};
    Primitive.BOOLEAN.sortArray(booleans0);
    assertEquals("[false, false, true, true, true]",
        Primitive.BOOLEAN.arrayToString(booleans0));

    // all false
    boolean[] booleans1 = {false, false, false, false, false};
    Primitive.BOOLEAN.sortArray(booleans1);
    assertEquals("[false, false, false, false, false]",
        Primitive.BOOLEAN.arrayToString(booleans1));

    // all true
    boolean[] booleans2 = {true, true, true, true, true};
    Primitive.BOOLEAN.sortArray(booleans2);
    assertEquals("[true, true, true, true, true]",
        Primitive.BOOLEAN.arrayToString(booleans2));

    // empty
    boolean[] booleans3 = {};
    Primitive.BOOLEAN.sortArray(booleans3);
    assertEquals("[]", Primitive.BOOLEAN.arrayToString(booleans3));

    // ranges specified
    boolean[] booleans4 = {true, true, false, false, true, false, false};
    Primitive.BOOLEAN.sortArray(booleans4, 1, 6);
    assertEquals("[true, false, false, false, true, true, false]",
        Primitive.BOOLEAN.arrayToString(booleans4));
  }
}

// End PrimitiveTest.java
