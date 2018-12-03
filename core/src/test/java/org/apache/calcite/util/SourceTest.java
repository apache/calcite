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

import org.junit.Test;

import java.io.File;

import static org.apache.calcite.util.Sources.file;
import static org.apache.calcite.util.Sources.url;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link Source}.
 */
public class SourceTest {
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

  @Test public void testAppendWithSpaces() {
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

  @Test public void testAppendHttp() {
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
        parent.append(child).file().toString(),
        // This should transparently support various OS
        is(new File(expected).toString()));
  }

  private void assertAppendUrl(Source parent, Source child, String expected) {
    assertThat(parent + ".append(" + child + ")",
        parent.append(child).url().toString(),
        is(expected));
  }

  @Test public void testSpaceInUrl() {
    String url = "file:" + ROOT_PREFIX + "dir%20name/test%20file.json";
    final Source foo = url(url);
    assertEquals(url + " .file().getAbsolutePath()",
        new File(ROOT_PREFIX + "dir name/test file.json").getAbsolutePath(),
        foo.file().getAbsolutePath());
  }

  @Test public void testSpaceInRelativeUrl() {
    String url = "file:dir%20name/test%20file.json";
    final Source foo = url(url);
    assertEquals(url + " .file().getAbsolutePath()",
        "dir name/test file.json",
        foo.file().getPath().replace('\\', '/'));
  }

  @Test public void testRelative() {
    final Source fooBar = file(null, ROOT_PREFIX + "foo/bar");
    final Source foo = file(null, ROOT_PREFIX + "foo");
    final Source baz = file(null, ROOT_PREFIX + "baz");
    final Source bar = fooBar.relative(foo);
    assertThat(bar.file().toString(), is("bar"));
    assertThat(fooBar.relative(baz), is(fooBar));
  }
}

// End SourceTest.java
