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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests that {@link FileSchema} dereferences only local {@code file} sources by
 * default, and requires an explicit operator opt-in (the
 * {@code calcite.file.urls.allowed} system property) for remote URL table
 * operands.
 */
class FileSchemaProtocolTest {

  private static Map<String, Object> table(String name, String url) {
    final Map<String, Object> tableDef = new HashMap<>();
    tableDef.put("name", name);
    tableDef.put("url", url);
    return tableDef;
  }

  /** Skips a test that relies on the default allowlist, if the test JVM was
   * started with {@code calcite.file.urls.allowed} set. */
  private static void assumeDefaultAllowlist() {
    assumeTrue(CalciteSystemProperty.FILE_URLS_ALLOWED.value().isEmpty(),
        "calcite.file.urls.allowed is set");
  }

  private static void assertAllowed(String url, String allowedUrls) {
    assertDoesNotThrow(() -> checkUrlAllowed(url, allowedUrls),
        () -> url + " should be allowed by " + allowedUrls);
  }

  private static void checkUrlAllowed(String url, String allowedUrls) {
    FileSchema.checkUrlAllowed(url, () -> Pattern.compile(allowedUrls));
  }

  private static void assertRejected(String url, String allowedUrls) {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class,
            () -> checkUrlAllowed(url, allowedUrls),
            () -> url + " should be rejected by " + allowedUrls);
    assertThat(url, e.getMessage(),
        containsString("Remote URL '" + url + "' is not allowed"));
    assertThat(url, e.getMessage(),
        containsString("calcite.file.urls.allowed"));
  }

  @Test void testRemoteUrlRejectedByDefault() {
    assumeDefaultAllowlist();
    for (String remote : new String[] {"https://data.example.com/t.csv", "http://localhost/t.csv",
        "ftp://localhost/t.csv", "file://fileserver.example.com/share/t.csv"}) {
      final FileSchema schema =
          new FileSchema(null, "S", null,
              ImmutableList.of(table("T", remote)));
      IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, schema::getTableMap);
      assertThat(e.getMessage(),
          containsString("Remote URL '" + remote + "' is not allowed"));
      assertThat(e.getMessage(), containsString("calcite.file.urls.allowed"));
    }
  }

  @Test void testFileUrlAllowedByDefault() {
    final FileSchema schema =
        new FileSchema(null, "S", null,
            ImmutableList.of(table("T", "file:foo.csv")));
    final Map<String, Table> tableMap = schema.getTableMap();
    assertThat(tableMap.containsKey("T"), is(true));
  }

  /** Tests that the regular expression must match the whole URL, not just a part of it. */
  @Test void testRegexMatchesWholeUrl() {
    assertRejected("https://data.example.com/table.csv",
        "https://data\\.example\\.com/");
    assertRejected("https://data.example.com/table.csv", "data\\.example\\.com");
    assertRejected("https://other.example.com/?u=https://data.example.com/t.csv",
        "https://data\\.example\\.com/.*");
  }

  @Test void testRegexAlternation() {
    final String allowed =
        "https://.*|http://data\\.example\\.com:8080/.*|ftp://ftp\\.example\\.com/.*";
    assertAllowed("https://any.example.com:8443/x/table.csv", allowed);
    assertAllowed("http://data.example.com:8080/table.csv", allowed);
    assertAllowed("ftp://ftp.example.com/pub/table.csv", allowed);
    assertRejected("http://data.example.com/table.csv", allowed);
    assertRejected("ftp://other.example.com/pub/table.csv", allowed);
  }

  @Test void testIsLocalUri() throws Exception {
    final String[] remoteUrls = {
        "file://fileserver.example.com/share/table.csv",
        "file:////fileserver.example.com/share/table.csv",
        "file://///fileserver.example.com/share/table.csv",
        "file:%5C%5Cfileserver.example.com%5Cshare%5Ctable.csv",
        "file:/%5C%5Cfileserver.example.com/share/table.csv",
        "file:%2F%2Fhost/share/t.csv",
        "file://localhost/tmp/t.csv",
        // jar URLs are considered as remote, even when they wrap a local file
        "jar:file:/opt/app/lib/data.jar!/table.csv",
        "jar:https://files.example.com/data.jar!/table.csv",
    };
    for (String url : remoteUrls) {
      assertFalse(FileSchema.isLocalUri(new URI(url)), url);
    }

    final String[] localUrls = {
        "file:table.csv",
        "file:/tmp/table.csv",
        "file:///tmp/table.csv",
        "file:///C:/data/table.csv",
        "file:/C:/data/table.csv",
    };
    for (String url : localUrls) {
      assertTrue(FileSchema.isLocalUri(new URI(url)), url);
    }
  }
}
