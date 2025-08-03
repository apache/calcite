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

import org.apache.calcite.util.Source;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * A Source implementation that reads directly from files without caching.
 * This is used for Parquet conversion to ensure we always get fresh data.
 */
public class DirectFileSource implements Source {
  private final File file;

  public DirectFileSource(File file) {
    this.file = file;
  }

  @Override public URL url() {
    try {
      return file.toURI().toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public File file() {
    return file;
  }

  @Override public Optional<File> fileOpt() {
    return Optional.of(file);
  }

  @Override public String path() {
    return file.getPath();
  }

  @Override public Reader reader() throws IOException {
    // Always create a fresh reader - no caching
    return new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
  }

  @Override public InputStream openStream() throws IOException {
    // Always create a fresh stream - no caching
    return new FileInputStream(file);
  }

  @Override public String protocol() {
    return "file";
  }

  @Override public Source append(Source child) {
    File childFile = child.file();
    if (childFile != null) {
      return new DirectFileSource(new File(file, childFile.getPath()));
    }
    // Fallback to path-based append
    return new DirectFileSource(new File(file, child.path()));
  }

  @Override public Source relative(Source source) {
    // Calculate relative path from source to this
    String basePath = source.path();
    String thisPath = this.path();

    if (thisPath.startsWith(basePath)) {
      String relativePath = thisPath.substring(basePath.length());
      if (relativePath.startsWith(File.separator)) {
        relativePath = relativePath.substring(1);
      }
      return new DirectFileSource(new File(relativePath));
    }

    // If not relative, return self
    return this;
  }

  @Override public Source trim(String suffix) {
    String path = file.getPath();
    if (suffix != null && path.endsWith(suffix)) {
      String newPath = path.substring(0, path.length() - suffix.length());
      return new DirectFileSource(new File(newPath));
    }
    return this;
  }

  @Override public Source trimOrNull(String suffix) {
    String path = file.getPath();
    if (suffix != null && path.endsWith(suffix)) {
      String newPath = path.substring(0, path.length() - suffix.length());
      return new DirectFileSource(new File(newPath));
    }
    return null;
  }
}
