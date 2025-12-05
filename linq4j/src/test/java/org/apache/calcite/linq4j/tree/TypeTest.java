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
package org.apache.calcite.linq4j.tree;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test for {@link Types#gcd}.
 */
class TypeTest {
  @Test void testGcd() {
    int i = 0;
    char c = 0;
    byte b = 0;
    short s = 0;
    int l = 0;

    // int to long
    l = i;
    assertThat(Types.gcd(int.class, long.class), is(long.class));

    // reverse args
    assertThat(Types.gcd(long.class, int.class), is(long.class));

    // char to int
    i = c;
    assertThat(Types.gcd(char.class, int.class), is(int.class));

    // can assign byte to short
    assertThat(Types.gcd(byte.class, short.class), is(short.class));
    s = b;

    // cannot assign byte to char
    // cannot assign char to short
    // can assign byte and char to int
    // fails: c = b;
    // fails: s = c;
    i = b;
    i = c;
    assertThat(Types.gcd(char.class, byte.class), is(int.class));

    assertThat(Types.gcd(byte.class, char.class), is(int.class));

    // mix a primitive with an object
    // (correct answer is java.io.Serializable)
    assertThat(Types.gcd(String.class, int.class), is(Object.class));
    java.io.Serializable o = true ? "x" : 1;
  }
}
