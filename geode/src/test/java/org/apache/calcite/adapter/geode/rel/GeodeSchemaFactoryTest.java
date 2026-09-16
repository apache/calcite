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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.adapter.geode.simple.GeodeSimpleSchemaFactory;
import org.apache.calcite.adapter.geode.util.GeodeUtils;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test case for
 * <a href="https://issues.apache.org/jira/browse/CALCITE-7787">[CALCITE-7787]
 * Make Geode pdxSerializablePackagePath schema operand opt-in via system property</a>.
 * Verifies that the {@code pdxSerializablePackagePath} operand is disabled by
 * default in both Geode schema factories, and enabled only when the
 * {@code calcite.geode.pdxSerializablePackagePath.allowed} system property is set.
 *
 * <p>Marked {@link Isolated} because one test toggles the JVM-wide cached value
 * of {@link CalciteSystemProperty#GEODE_PDX_PACKAGE_PATH_ALLOWED}.
 */
@Isolated
class GeodeSchemaFactoryTest {
  private static Map<String, Object> operand() {
    final Map<String, Object> operand = new HashMap<>();
    operand.put(GeodeSchemaFactory.LOCATOR_HOST, "localhost");
    operand.put(GeodeSchemaFactory.LOCATOR_PORT, "10334");
    operand.put(GeodeSchemaFactory.REGIONS, "region");
    operand.put(GeodeSchemaFactory.PDX_SERIALIZABLE_PACKAGE_PATH, "com.example.*");
    return operand;
  }

  private static SchemaPlus rootSchema() {
    return CalciteSchema.createRootSchema(false, false).plus();
  }

  /** Temporarily sets {@link CalciteSystemProperty#GEODE_PDX_PACKAGE_PATH_ALLOWED}
   * to {@code true}, returning an {@link AutoCloseable} that restores the
   * original value when closed. The system property is read once at class-load
   * time; this helper flips the cached value for the duration of a single test.
   */
  private static AutoCloseable overrideGeodePdxPackagePathAllowed() {
    final Field field;
    try {
      field = CalciteSystemProperty.class.getDeclaredField("value");
    } catch (NoSuchFieldException e) {
      throw new AssertionError(e);
    }
    field.setAccessible(true);
    final CalciteSystemProperty<Boolean> property =
        CalciteSystemProperty.GEODE_PDX_PACKAGE_PATH_ALLOWED;
    final Object original;
    try {
      original = field.get(property);
      field.set(property, true);
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    }
    return () -> field.set(property, original);
  }

  @Test void testPdxPackagePathOperandDisabledByDefault() {
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () ->
            new GeodeSchemaFactory().create(rootSchema(), "geode", operand()));
    assertThat(e.getMessage(), containsString("pdxSerializablePackagePath"));
  }

  @Test void testSimpleFactoryPdxPackagePathOperandDisabledByDefault() {
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () ->
            new GeodeSimpleSchemaFactory().create(rootSchema(), "geode", operand()));
    assertThat(e.getMessage(), containsString("pdxSerializablePackagePath"));
  }

  @Test void testPdxPackagePathOperandEnabled() throws Exception {
    try (AutoCloseable ignored = overrideGeodePdxPackagePathAllowed()) {
      new GeodeSchemaFactory().create(rootSchema(), "geode", operand());
    }
  }

  @Test void testSimpleFactoryPdxPackagePathOperandEnabledt() throws Exception {
    try (AutoCloseable ignored = overrideGeodePdxPackagePathAllowed()) {
      new GeodeSimpleSchemaFactory().create(rootSchema(), "geode", operand());
    }
  }

  /** A null operand is always accepted, independent of the opt-in property. */
  @Test void testPdxPackagePathOperandNullIsAlwaysAccepted() {
    assertDoesNotThrow(() -> GeodeUtils.checkPdxPackagePathOperand(null));
  }
}
