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
package org.apache.calcite.adapter.file;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests that {@link FileSchema} dereferences only local {@code file} sources by
 * default, and requires an explicit operator opt-in (the
 * {@code calcite.file.remote.protocols.allowed} system property) and an optional list
 * of allowed hosts (the {@code calcite.file.remote.hosts.allowed} system property) for remote
 * URL table operands.
 */
class FileSchemaProtocolTest {

  private static Map<String, Object> table(String name, String url) {
    final Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", name);
    tableDef.put("url", url);
    return tableDef;
  }

  /** Skips a test that relies on the default protocol allowlist, if the test
   * JVM was started with {@code calcite.file.remote.protocols.allowed} set. */
  private static void assumeDefaultProtocols() {
    assumeTrue(
        CalciteSystemProperty.FILE_REMOTE_PROTOCOLS_ALLOWED.value().trim().isEmpty(),
        "calcite.file.remote.protocols.allowed is set");
  }

  @Test void testRemoteUrlRejectedByDefault() {
    assumeDefaultProtocols();
    final FileSchema schema =
        new FileSchema(null, "S", null,
            ImmutableList.of(table("T", "http://localhost/table.csv")));
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, schema::getTableMap);
    assertThat(e.getMessage(),
        containsString("Remote URL protocol 'http' is not allowed"));
    assertThat(e.getMessage(),
        containsString("calcite.file.remote.protocols.allowed"));
  }

  @Test void testFtpUrlRejectedByDefault() {
    assumeDefaultProtocols();
    final FileSchema schema =
        new FileSchema(null, "S", null,
            ImmutableList.of(table("T", "ftp://localhost/table.csv")));
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, schema::getTableMap);
    assertThat(e.getMessage(),
        containsString("Remote URL protocol 'ftp' is not allowed"));
    assertThat(e.getMessage(),
        containsString("calcite.file.remote.protocols.allowed"));
  }

  @Test void testFileUrlAllowedByDefault() {
    final FileSchema schema =
        new FileSchema(null, "S", null,
            ImmutableList.of(table("T", "file:foo.csv")));
    final Map<String, Table> tableMap = schema.getTableMap();
    assertThat(tableMap.containsKey("T"), is(true));
  }

  /** Exercises the check with an explicit allowlist rather than by mutating a
   * JVM-wide system property; {@link
   * org.apache.calcite.config.CalciteSystemProperty} caches at class-load, so
   * mid-run {@code System.setProperty} would not be observed by the read
   * inside {@link FileSchema#checkUrlAllowed(String)}. */
  @Test void testRemoteUrlAllowedWhenOptedIn() {
    assertDoesNotThrow(() ->
        FileSchema.checkUrlAllowed("http://localhost/table.csv",
            "http, https", ""));
    assertDoesNotThrow(() ->
        FileSchema.checkUrlAllowed("https://localhost/table.csv",
            "http, https", ""));
  }

  @Test void testExplicitAllowlistStillRejectsUnlisted() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () ->
            FileSchema.checkUrlAllowed("ftp://localhost/table.csv",
                "http, https", ""));
    assertThat(e.getMessage(),
        containsString("Remote URL protocol 'ftp' is not allowed"));
  }

  @Test void testHostAllowlistPermitsListedHost() {
    assertDoesNotThrow(() ->
        FileSchema.checkUrlAllowed("https://data.example.com/table.csv",
            "http, https", "data.example.com, ftp.example.com"));
  }

  @Test void testHostAllowlistRejectsUnlistedHost() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () ->
            FileSchema.checkUrlAllowed("https://bad.example.com/table.csv",
                "http, https", "data.example.com"));
    assertThat(e.getMessage(),
        containsString("Remote URL host 'bad.example.com' is not on"));
    assertThat(e.getMessage(),
        containsString("calcite.file.remote.hosts.allowed"));
  }

  @Test void testHostAllowlistIsCaseInsensitive() {
    assertDoesNotThrow(() ->
        FileSchema.checkUrlAllowed("https://DATA.Example.COM/table.csv",
            "https", "data.example.com"));
  }

  @Test void testHostAllowlistExactMatchOnly() {
    // A suffix must not slip through: "example.com" does not admit "bad-example.com".
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () ->
            FileSchema.checkUrlAllowed("https://bad-example.com/table.csv",
                "https", "example.com"));
    assertThat(e.getMessage(),
        containsString("Remote URL host 'bad-example.com' is not on"));
  }

  @Test void testHostAllowlistIgnoredWhenEmpty() {
    // Empty host allowlist means "any host allowed".
    assertDoesNotThrow(() ->
        FileSchema.checkUrlAllowed("https://whatever.example.com/table.csv",
            "https", ""));
  }

  @Test void testRemoteFileUrlRejectedByDefault() {
    assumeDefaultProtocols();
    final FileSchema schema =
        new FileSchema(null, "S", null,
            ImmutableList.of(
                table("T", "file://fileserver.example.com/share/table.csv")));
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, schema::getTableMap);
    assertThat(e.getMessage(),
        containsString("Remote 'file' URL (host 'fileserver.example.com')"
            + " is not allowed"));
    assertThat(e.getMessage(),
        containsString("calcite.file.remote.protocols.allowed"));
  }

  /** Tests that every form of {@code file} URL that Windows resolves to a UNC
   * path counts as remote, as does a {@code file} URL with any authority. */
  @Test void testRemoteFileUrlForms() {
    final String[] urls = {
        "file://fileserver.example.com/share/table.csv",
        "file:////fileserver.example.com/share/table.csv",
        "file://///fileserver.example.com/share/table.csv",
        "file:%5C%5Cfileserver.example.com%5Cshare%5Ctable.csv",
        "file:/%5C%5Cfileserver.example.com/share/table.csv",
    };
    for (String url : urls) {
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () ->
              FileSchema.checkUrlAllowed(url, "", ""), url);
      assertThat(url, e.getMessage(),
          containsString("(host 'fileserver.example.com')"));
    }
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () ->
            FileSchema.checkUrlAllowed("file://localhost/tmp/table.csv", "",
                ""));
    assertThat(e.getMessage(), containsString("(host 'localhost')"));
  }

  @Test void testLocalFileUrlForms() {
    final String[] urls = {
        "file:table.csv",
        "file:/tmp/table.csv",
        "file:///tmp/table.csv",
        "file:///C:/data/table.csv",
        "file:/C:/data/table.csv",
    };
    for (String url : urls) {
      assertDoesNotThrow(() -> FileSchema.checkUrlAllowed(url, "", ""), url);
    }
  }

  @Test void testRemoteFileUrlAllowedWhenOptedIn() {
    final String url = "file://fileserver.example.com/share/table.csv";
    assertDoesNotThrow(() -> FileSchema.checkUrlAllowed(url, "file", ""));
    assertDoesNotThrow(() ->
        FileSchema.checkUrlAllowed(url, "file", "fileserver.example.com"));
    assertDoesNotThrow(() ->
        FileSchema.checkUrlAllowed(
            "file:////fileserver.example.com/share/table.csv", "file",
            "fileserver.example.com"));
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () ->
            FileSchema.checkUrlAllowed(url, "file", "data.example.com"));
    assertThat(e.getMessage(),
        containsString("Remote URL host 'fileserver.example.com' is not on"));
  }
}
