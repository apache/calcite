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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

  // ----- Non-inline model-file handling ---------------------------------
  //
  // The following tests cover two related aspects of the non-inline branch
  // of the ModelHandler constructor:
  //
  //   1. The client-facing IOException on a bad model file carries only
  //      the path, not the file's contents and not whether the file
  //      exists. Legitimate operators find the detail in the log.
  //   2. When calcite.model.baseDirectory is set, model paths that
  //      resolve outside it are rejected.
  //
  // The inline: branch is intentionally untouched: inline text is
  // supplied by the caller, and detailed parse messages remain
  // load-bearing for model authors.

  @Test void testModelFileNoRestrictionWhenBaseDirectoryUnset() {
    assertThat(ModelHandler.modelFile("", "anywhere/model.json").getPath(),
        is(new File("anywhere/model.json").getPath()));
  }

  @Test void testModelFileRelativePathResolvesUnderBaseDirectory(
      @TempDir Path tempDir) {
    final File file =
        ModelHandler.modelFile(tempDir.toString(), "sub/model.json");
    assertThat(file.toPath().startsWith(tempDir), is(true));
    assertThat(file.getName(), is("model.json"));
  }

  @Test void testModelFileRelativeEscapeRejected(@TempDir Path tempDir) {
    SecurityException e =
        assertThrows(SecurityException.class, () ->
            ModelHandler.modelFile(tempDir.resolve("base").toString(),
                "../escape/model.json"));
    assertThat(e.getMessage(),
        containsString("calcite.model.baseDirectory"));
  }

  @Test void testModelFileAbsolutePathOutsideBaseDirectoryRejected(
      @TempDir Path tempDir) {
    final Path base = tempDir.resolve("base");
    final Path outside = tempDir.resolve("outside/model.json");
    assertThrows(SecurityException.class, () ->
        ModelHandler.modelFile(base.toString(), outside.toString()));
  }

  @Test void testModelFileAbsolutePathInsideBaseDirectoryAllowed(
      @TempDir Path tempDir) {
    final Path inside = tempDir.resolve("model.json");
    final File file =
        ModelHandler.modelFile(tempDir.toString(), inside.toString());
    assertThat(file.toPath(), is(inside));
  }

  /** A malformed model file surfaces a path-only message; source-byte
   * fragments Jackson would otherwise quote in the parse error do not
   * reach the client through the IOException chain. */
  @Test void testMalformedModelFileReportsPathOnly(@TempDir Path tempDir) {
    final String marker = "distinct-first-line-marker";
    Path modelFile = tempDir.resolve("scratch.json");
    try {
      Files.write(modelFile, (marker + " not json content").getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    SchemaPlus root = CalciteSchema.createRootSchema(false, false).plus();
    IOException e =
        assertThrows(IOException.class,
            () -> new ModelHandler(root, modelFile.toString()));
    assertThat(e.getMessage(), containsString(modelFile.toString()));
    assertThat(e.getMessage(), containsString("see log of")); // Full info in log
    // Neither the top-level message nor any cause in the chain names the
    // marker string from the file
    Throwable t = e;
    while (t != null) {
      assertThat(String.valueOf(t.getMessage()),
          not(containsString(marker)));
      t = t.getCause();
    }
  }

  /** A missing model file produces the same shape of message as a
   * malformed one: the client cannot distinguish "does not exist" from
   * "exists but unreadable" from "exists but invalid" via the error
   * string. */
  @Test void testMissingModelFileReportsGenericPathOnlyError(
      @TempDir Path tempDir) {
    final Path modelFile = tempDir.resolve("no-such-model.json");
    SchemaPlus root = CalciteSchema.createRootSchema(false, false).plus();
    IOException e =
        assertThrows(IOException.class,
            () -> new ModelHandler(root, modelFile.toString()));
    assertThat(e.getMessage(), containsString(modelFile.toString()));
    assertThat(e.getMessage(), containsString("see log of")); // Full info in log
    // The message must not carry the "FileNotFound"/"No such file"
    // signatures that would otherwise distinguish existence
    Throwable t = e;
    while (t != null) {
      assertThat(String.valueOf(t.getMessage()),
          not(containsString("FileNotFound")));
      assertThat(String.valueOf(t.getMessage()),
          not(containsString("No such file")));
      t = t.getCause();
    }
  }

  /** Inline-model errors keep the detailed Jackson message: inline text
   * comes from the caller, so echoing it back leaks nothing. */
  @Test void testInlineModelErrorsKeepDetail() {
    SchemaPlus root = CalciteSchema.createRootSchema(false, false).plus();
    IOException e =
        assertThrows(IOException.class,
            () -> new ModelHandler(root, "inline:{ not valid json"));
    // The inline branch throws the underlying Jackson IOException
    // unchanged (or a wrapped one whose chain still names the parser).
    // Assert only that we did not swap in the path-only message.
    assertThat(String.valueOf(e.getMessage()),
        not(containsString("see log of")));
  }

}
