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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.util.Sources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static java.util.Objects.requireNonNull;

/**
 * Unit test for {@link ModelHandler}.
 */
public class ModelHandlerTest {

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7022">[CALCITE-7022]
   * Decouple ModelHandler from CalciteConnection</a>.
   * The test ensures/demonstrates that a Schema can be easily parsed/created from a model
   * file (JSON/YAML) without necessitating the creation of complex/heavy objects
   * (e.g., CalciteConnection). */
  @Test void testPopulateRootSchemaFromURL() throws IOException {
    SchemaPlus root = CalciteSchema.createRootSchema(false, false).plus();
    String mURI =
        Sources.of(requireNonNull(ModelHandlerTest.class.getResource("/hsqldb-scott.json")))
            .path();
    ModelHandler h = new ModelHandler(root, mURI);
    SchemaPlus scott = root.subSchemas().get("SCOTT");
    Set<String> tables = scott.tables().getNames(new LikePattern("%"));
    assertThat(tables, is(ImmutableSet.of("EMP", "DEPT", "BONUS", "SALGRADE")));
    assertThat(h.defaultSchemaName(), is("SCOTT"));
  }

  @Test void testDenyUdfClass() {
    SchemaPlus root = CalciteSchema.createRootSchema(false, false).plus();
    SecurityException e =
        assertThrows(SecurityException.class, () ->
            ModelHandler.addFunctions(root, "lookup", ImmutableList.of(),
                "javax.naming.InitialContext", "doLookup", false));
    assertThat(e.getMessage(), containsString("javax.naming."));
    assertThat(e.getMessage(), containsString("denylist"));
  }

  @Test void testCustomFilterPassedToConstructor() {
    // A ModelHandler built with a stricter filter must reject classes
    // its filter denies, even ones the standard filter would allow.
    SchemaPlus root = CalciteSchema.createRootSchema(false, false).plus();
    // java.lang.String is not in the standard denylist; the custom
    // filter denies the whole java.lang. package.
    ClassNameFilter strict = ClassNameFilter.of("java.lang.", "java.");
    String model = "inline:{"
        + "  version: '1.0',"
        + "  defaultSchema: 'X',"
        + "  schemas: [ {"
        + "    name: 'X',"
        + "    functions: [ {"
        + "      name: 'F',"
        + "      className: 'java.lang.String'"
        + "    } ]"
        + "  } ]"
        + "}";
    Throwable e =
        assertThrows(RuntimeException.class, () ->
            new ModelHandler(root, model, strict));
    while (e != null && !(e instanceof SecurityException)) {
      e = e.getCause();
    }
    assertThat("expected SecurityException in chain", e, notNullValue());
    assertThat(e.getMessage(), containsString("java.lang."));
  }

  @Test void testAddFunctionsWithExplicitFilterDeniesClass() {
    // The filter-taking overload of addFunctions must reject any class
    // its filter denies.
    SchemaPlus root = CalciteSchema.createRootSchema(false, false).plus();
    SecurityException e =
        assertThrows(SecurityException.class, () ->
            ModelHandler.addFunctions(ClassNameFilter.standard(), root,
                "lookup", "javax.naming.InitialContext", "doLookup", false));
    assertThat(e.getMessage(), containsString("javax.naming."));
  }

  @Test void testDenyFactory() {
    String model = "inline:{"
        + "  version: '1.0',"
        + "  defaultSchema: 'X',"
        + "  schemas: [ {"
        + "    name: 'X',"
        + "    type: 'custom',"
        + "    factory: 'javax.naming.InitialContext'"
        + "  } ]"
        + "}";
    Properties info = new Properties();
    info.setProperty("model", model);
    Exception e =
        assertThrows(Exception.class, () -> {
          try (Connection ignored =
                   DriverManager.getConnection("jdbc:calcite:", info)) {
            // unreachable
          }
        });
    Throwable cause = e;
    while (cause != null && !(cause instanceof SecurityException)) {
      cause = cause.getCause();
    }
    assertThat("expected a SecurityException in the chain",
        cause != null, is(true));
    assertThat(requireNonNull(cause, "cause").getMessage(),
        containsString("javax.naming."));
  }

  @Test void testStaticFieldRefIsCheckedAgainstClass() {
    // Avatica accepts "ClassName#FIELD" for plugin references; the filter
    // must reject the class portion regardless of which field is named.
    SecurityException e =
        assertThrows(SecurityException.class, () ->
            ClassNameFilter.standard().check(
                "java.lang.Runtime#anything"));
    assertThat(e.getMessage(), containsString("java.lang.Runtime"));
  }

  @Test void testLegitFactoryClassIsAllowed() {
    // Sanity: a class outside the denylist passes (no exception).
    ClassNameFilter.standard().check(
        "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory");
    ClassNameFilter.standard().check(
        "org.apache.calcite.schema.impl.AbstractSchema$Factory");
    ClassNameFilter.standard().check(
        "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory#INSTANCE");
  }

  @Test void testFactoryMethodsCacheInstances() {
    // standard() returns a single cached instance.
    assertThat(ClassNameFilter.standard(),
        sameInstance(ClassNameFilter.standard()));
    // of() returns the same instance for equal inputs.
    ClassNameFilter a = ClassNameFilter.of("com.evil.", "javax.");
    ClassNameFilter b = ClassNameFilter.of("com.evil.", "javax.");
    assertThat(a, sameInstance(b));
    // Different inputs produce different instances.
    ClassNameFilter c = ClassNameFilter.of("com.evil.,com.example.", "javax.");
    assertThat(a, not(sameInstance(c)));
    // The cached filter behaves as configured.
    assertThrows(SecurityException.class, () -> a.check("com.evil.Payload"));
    assertDoesNotThrow(() -> a.check("javax.naming.InitialContext"));
  }

  @Test void testAppendCombinesPatternStrings() {
    // The denylist extension wired into ClassNameFilter.standard() works
    // by string concatenation through ClassNameFilter.append.
    assertThat(ClassNameFilter.append("a.,b.", ""), is("a.,b."));
    assertThat(ClassNameFilter.append("", "c.,d."), is("c.,d."));
    assertThat(ClassNameFilter.append("a.", "b."), is("a.,b."));
    assertThat(ClassNameFilter.append("", ""), is(""));
  }

}
