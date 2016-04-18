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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

/**
 * Source of data.
 */
public interface Source {
  URL url();
  File file();
  String path();
  Reader reader() throws IOException;
  InputStream openStream() throws IOException;
  String protocol();

  /** Looks for a suffix on a path and returns
   * either the path with the suffix removed
   * or the original path. */
  Source trim(String suffix);

  /** Looks for a suffix on a path and returns
   * either the path with the suffix removed
   * or null. */
  Source trimOrNull(String suffix);

  /** Returns a source whose path concatenates this with a child.
   *
   * <p>For example,
   * <ul>
   *   <li>source("/foo").append(source("bar"))
   *   returns source("/foo/bar")
   *   <li>source("/foo").append(source("/bar"))
   *   returns source("/bar")
   *   because "/bar" was already absolute
   * </ul>
   */
  Source append(Source child);

  /** Returns a relative source, if this source is a child of a given base.
   *
   * <p>For example,
   * <ul>
   *   <li>source("/foo/bar").relative(source("/foo"))
   *   returns source("bar")
   *   <li>source("/baz/bar").relative(source("/foo"))
   *   returns source("/baz/bar")
   * </ul>
   */
  Source relative(Source source);
}

// End Source.java
