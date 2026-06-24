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
package org.apache.calcite.model;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link ClassNameFilter}.
 */
public class ClassNameFilterTest {
  @Test void testDefaultProdValuesForDenyAllowList() {
    // This represents the default prod configuration of the project
    // where the system properties both default to empty and
    // basically every class is rejected mainly due to the empty allowlist
    ClassNameFilter cf = ClassNameFilter.of("", "");
    assertThrows(SecurityException.class, () -> cf.check("java.lang.String"));
    assertThrows(SecurityException.class,
        () -> cf.check("org.apache.calcite.adapter.jdbc.JdbcSchema$Factory"));
  }

  @Test void testAllowListWithSinglePackagePattern() {
    ClassNameFilter cf = ClassNameFilter.of("", "org.");
    assertThrows(SecurityException.class, () -> cf.check("java.lang.String"));
    assertDoesNotThrow(() -> cf.check("org.apache.calcite.adapter.jdbc.JdbcSchema$Factory"));
  }

  @Test void testAllowListWithSinglePackageNoPattern() {
    ClassNameFilter cf = ClassNameFilter.of("", "org");
    assertThrows(SecurityException.class, () -> cf.check("java.lang.String"));
    assertThrows(SecurityException.class,
        () -> cf.check("org.apache.calcite.adapter.jdbc.JdbcSchema$Factory"));
  }

  @Test void testAllowListWithMultiplePackagePatterns() {
    ClassNameFilter cf = ClassNameFilter.of("", "org.,java.");
    assertDoesNotThrow(() -> cf.check("java.lang.String"));
    assertDoesNotThrow(() -> cf.check("org.apache.calcite.adapter.jdbc.JdbcSchema$Factory"));
    assertThrows(SecurityException.class, () -> cf.check("com.sun.media.sound.Toolkit"));
  }

  @Test void testAllowListWithSingleClass() {
    ClassNameFilter cf = ClassNameFilter.of("", "java.lang.String");
    assertDoesNotThrow(() -> cf.check("java.lang.String"));
    assertThrows(SecurityException.class,
        () -> cf.check("org.apache.calcite.adapter.jdbc.JdbcSchema$Factory"));
    assertThrows(SecurityException.class, () -> cf.check("java.lang.Math"));
  }

  @Test void testAllowListWithMultipleClasses() {
    ClassNameFilter cf = ClassNameFilter.of("", "java.lang.String,java.lang.Math");
    assertDoesNotThrow(() -> cf.check("java.lang.String"));
    assertThrows(SecurityException.class, () -> cf.check("java.lang.StringBuffer"));
    assertThrows(SecurityException.class,
        () -> cf.check("org.apache.calcite.adapter.jdbc.JdbcSchema$Factory"));
    assertDoesNotThrow(() -> cf.check("java.lang.Math"));
  }

  @Test void testStandardFilter() {
    // Note that test specific system properties are in effect
    ClassNameFilter cf = ClassNameFilter.standard();
    assertDoesNotThrow(() -> cf.check("java.lang.String"));
    assertDoesNotThrow(() -> cf.check("org.apache.calcite.adapter.jdbc.JdbcSchema$Factory"));
    SecurityException x1 =
        assertThrows(SecurityException.class, () -> cf.check("com.sun.media.sound.Toolkit"));
    assertThat(x1.getMessage(), containsString("rejected by the allowlist"));
    SecurityException x2 =
        assertThrows(SecurityException.class, () -> cf.check("javax.naming.InitialContext"));
    assertThat(x2.getMessage(), containsString("rejected by the denylist"));
  }
}
