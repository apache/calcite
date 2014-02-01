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
package net.hydromatic.linq4j.expressions;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test for {@link Types#gcd}.
 */
public class TypeTest {
  @Test public void testGcd() {
    int i = 0;
    char c = 0;
    byte b = 0;
    short s = 0;
    int l = 0;

    // int to long
    l = i;
    assertEquals(long.class, Types.gcd(int.class, long.class));

    // reverse args
    assertEquals(long.class, Types.gcd(long.class, int.class));

    // char to int
    i = c;
    assertEquals(int.class, Types.gcd(char.class, int.class));

    // can assign byte to short
    assertEquals(short.class, Types.gcd(byte.class, short.class));
    s = b;

    // cannot assign byte to char
    // cannot assign char to short
    // can assign byte and char to int
    // fails: c = b;
    // fails: s = c;
    i = b;
    i = c;
    assertEquals(int.class, Types.gcd(char.class, byte.class));

    assertEquals(int.class, Types.gcd(byte.class, char.class));

    // mix a primitive with an object
    // (correct answer is java.io.Serializable)
    assertEquals(Object.class, Types.gcd(String.class, int.class));
    java.io.Serializable o = true ? "x" : 1;
  }
}

// End TypeTest.java
