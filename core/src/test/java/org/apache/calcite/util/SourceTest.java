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
package org.apache.calcite.util;
import com.google.common.io.CharSource;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.calcite.util.Sources.file;
import static org.apache.calcite.util.Sources.of;
import static org.apache.calcite.util.Sources.url;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Tests for {@link Source}.
 */
class SourceTest {
  private static final String ROOT_PREFIX = getRootPrefix();

  private static String getRootPrefix() {
    for (String s : new String[]{"/", "c:/"}) {
      if (new File(s).isAbsolute()) {
        return s;
      }
    }
    throw new IllegalStateException(
        "Unsupported operation system detected. Both / and c:/ produce relative paths");
  }

  /**
   * Read lines from {@link CharSource}.
   */
  @Test void charSource() throws IOException {
    Source source = Sources.fromCharSource(CharSource.wrap("a\nb"));
    for (Reader r : Arrays.asList(source.reader(),
        new InputStreamReader(source.openStream(), StandardCharsets.UTF_8.name()))) {
      try (BufferedReader reader = new BufferedReader(r)) {
        assertThat(reader.readLine(), is("a"));
        assertThat(reader.readLine(), is("b"));
        assertNull(reader.readLine());
      }
    }
  }

  static Stream<Arguments> relativePaths() {
    return Stream.of(
        arguments("abc def.txt", "file:abc%20def.txt"),
        arguments("abc+def.txt", "file:abc+def.txt"),
        arguments("path 1/ subfolder 2/abc.t x t", "file:path%201/%20subfolder%202/abc.t%20x%20t"),
        arguments("маленькой ёлочке холодно зимой.txt",
            "file:маленькой%20ёлочке%20холодно%20зимой.txt"));
  }

  private static String slashify(String path) {
    return path.replace(File.separatorChar, '/');
  }

  @ParameterizedTest
  @MethodSource("relativePaths")
  void testRelativeFileToUrl(String path, String expectedUrl) {
    URL url = of(new File(path)).url();

    assertNotNull(url, () -> "No URL generated for Sources.of(file " + path + ")");
    assertThat("Sources.of(file " + path + ").url()", url,
        hasToString(expectedUrl));
    assertThat("Sources.of(Sources.of(file " + path
            + ").url()).file().getPath()",
        slashify(Sources.of(url).file().getPath()), is(path));
  }

  @ParameterizedTest
  @MethodSource("relativePaths")
  @Disabled // Open when we really fix that
  void testAbsoluteFileToUrl(String path, String expectedUrl) throws URISyntaxException {
    File absoluteFile = new File(path).getAbsoluteFile();
    URL url = of(absoluteFile).url();

    assertNotNull(url, () -> "No URL generated for Sources.of(file(" + path + ").absoluteFile)");
    // Sources.of(url).file().getPath() does not always work
    // e.g. it might throw java.nio.file.InvalidPathException: Malformed input or input contains
    // unmappable characters: /home/.../ws/core/????????? ?????? ??????? ?????.txt
    //        at java.base/sun.nio.fs.UnixPath.encode(UnixPath.java:145)
    assertThat("Sources.of(Sources.of(file(" + path
        + ").absolutePath).url()).file().getPath()",
        url.toURI().getSchemeSpecificPart(),
        is(absoluteFile.getAbsolutePath()));
  }

  @Test void testAppendWithSpaces() {
    String fooRelative = "fo o+";
    String fooAbsolute = ROOT_PREFIX + "fo o+";
    String barRelative = "b ar+";
    String barAbsolute = ROOT_PREFIX + "b ar+";
    assertAppend(file(null, fooRelative), file(null, barRelative), "fo o+/b ar+");
    assertAppend(file(null, fooRelative), file(null, barAbsolute), barAbsolute);
    assertAppend(file(null, fooAbsolute), file(null, barRelative), ROOT_PREFIX + "fo o+/b ar+");
    assertAppend(file(null, fooAbsolute), file(null, barAbsolute), barAbsolute);

    String urlFooRelative = "file:fo%20o+";
    String urlFooAbsolute = "file:" + ROOT_PREFIX + "fo%20o+";
    String urlBarRelative = "file:b%20ar+";
    String urlBarAbsolute = "file:" + ROOT_PREFIX + "b%20ar+";
    assertAppend(url(urlFooRelative), url(urlBarRelative), "fo o+/b ar+");
    assertAppend(url(urlFooRelative), url(urlBarAbsolute), barAbsolute);
    assertAppend(url(urlFooAbsolute), url(urlBarRelative), ROOT_PREFIX + "fo o+/b ar+");
    assertAppend(url(urlFooAbsolute), url(urlBarAbsolute), barAbsolute);

    assertAppend(file(null, fooRelative), url(urlBarRelative), "fo o+/b ar+");
    assertAppend(file(null, fooRelative), url(urlBarAbsolute), barAbsolute);
    assertAppend(file(null, fooAbsolute), url(urlBarRelative), ROOT_PREFIX + "fo o+/b ar+");
    assertAppend(file(null, fooAbsolute), url(urlBarAbsolute), barAbsolute);

    assertAppend(url(urlFooRelative), file(null, barRelative), "fo o+/b ar+");
    assertAppend(url(urlFooRelative), file(null, barAbsolute), barAbsolute);
    assertAppend(url(urlFooAbsolute), file(null, barRelative), ROOT_PREFIX + "fo o+/b ar+");
    assertAppend(url(urlFooAbsolute), file(null, barAbsolute), barAbsolute);
  }

  @Test void testAppendHttp() {
    // I've truly no idea what append of two URLs should be, yet it does something
    assertAppendUrl(url("http://fo%20o+/ba%20r+"), file(null, "no idea what I am doing+"),
        "http://fo%20o+/ba%20r+/no%20idea%20what%20I%20am%20doing+");
    assertAppendUrl(url("http://fo%20o+"), file(null, "no idea what I am doing+"),
        "http://fo%20o+/no%20idea%20what%20I%20am%20doing+");
    assertAppendUrl(url("http://fo%20o+/ba%20r+"), url("file:no%20idea%20what%20I%20am%20doing+"),
        "http://fo%20o+/ba%20r+/no%20idea%20what%20I%20am%20doing+");
    assertAppendUrl(url("http://fo%20o+"), url("file:no%20idea%20what%20I%20am%20doing+"),
        "http://fo%20o+/no%20idea%20what%20I%20am%20doing+");
  }

  private void assertAppend(Source parent, Source child, String expected) {
    assertThat(parent + ".append(" + child + ")",
        parent.append(child).file(),
        // This should transparently support various OS
        hasToString(new File(expected).toString()));
  }

  private void assertAppendUrl(Source parent, Source child, String expected) {
    assertThat(parent + ".append(" + child + ")",
        parent.append(child).url(),
        hasToString(expected));
  }

  @Test void testSpaceInUrl() {
    String url = "file:" + ROOT_PREFIX + "dir%20name/test%20file.json";
    final Source foo = url(url);
    assertThat(url + " .file().getAbsolutePath()",
        foo.file().getAbsolutePath(),
        is(new File(ROOT_PREFIX + "dir name/test file.json")
            .getAbsolutePath()));
  }

  @Test void testSpaceInRelativeUrl() {
    String url = "file:dir%20name/test%20file.json";
    final Source foo = url(url);
    assertThat(url + " .file().getAbsolutePath()",
        foo.file().getPath().replace('\\', '/'),
        is("dir name/test file.json"));
  }

  @Test void testRelative() {
    final Source fooBar = file(null, ROOT_PREFIX + "foo/bar");
    final Source foo = file(null, ROOT_PREFIX + "foo");
    final Source baz = file(null, ROOT_PREFIX + "baz");
    final Source bar = fooBar.relative(foo);
    assertThat(bar.file(), hasToString("bar"));
    assertThat(fooBar.relative(baz), is(fooBar));
  }
}
