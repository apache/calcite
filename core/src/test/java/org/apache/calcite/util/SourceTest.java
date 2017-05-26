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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link Source}.
 */
public class SourceTest {
  @Test public void testAppend() {
    final Source foo = Sources.file(null, "/foo");
    final Source bar = Sources.file(null, "bar");
    final Source fooBar = foo.append(bar);
    assertThat(fooBar.file().toString(),
        is("/foo/bar".replace('/', File.separatorChar)));
  }

  @Test public void testRelative() {
    final Source fooBar = Sources.file(null, "/foo/bar");
    final Source foo = Sources.file(null, "/foo");
    final Source baz = Sources.file(null, "/baz");
    final Source bar = fooBar.relative(foo);
    assertThat(bar.file().toString(), is("bar"));
    assertThat(fooBar.relative(baz), is(fooBar));
  }
}

// End SourceTest.java
