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
package org.apache.calcite.avatica.test;

import org.apache.calcite.avatica.AvaticaUtils;

import org.junit.Test;

import java.math.BigInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Unit test for Avatica utilities.
 */
public class AvaticaUtilsTest {
  @Test public void testInstantiatePlugin() {
    final String s =
        AvaticaUtils.instantiatePlugin(String.class, "java.lang.String");
    assertThat(s, is(""));

    // No default constructor or INSTANCE member
    try {
      final Integer i =
          AvaticaUtils.instantiatePlugin(Integer.class, "java.lang.Integer");
      fail("expected error, got " + i);
    } catch (Throwable e) {
      assertThat(e.getMessage(),
          is("Property 'java.lang.Integer' not valid for plugin type java.lang.Integer"));
    }

    final BigInteger b =
        AvaticaUtils.instantiatePlugin(BigInteger.class, "java.math.BigInteger#ONE");
    assertThat(b, is(BigInteger.ONE));

    try {
      final BigInteger b2 =
          AvaticaUtils.instantiatePlugin(BigInteger.class,
              "java.math.BigInteger.ONE");
      fail("expected error, got " + b2);
    } catch (Throwable e) {
      assertThat(e.getMessage(),
          is("Property 'java.math.BigInteger.ONE' not valid for plugin type java.math.BigInteger"));
    }
  }
}

// End AvaticaUtilsTest.java
