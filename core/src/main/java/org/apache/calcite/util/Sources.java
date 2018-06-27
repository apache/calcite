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

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Utilities for {@link Source}.
 */
public abstract class Sources {
  private Sources() {}

  public static Source of(File file) {
    return new FileSource(file);
  }

  public static Source file(File baseDirectory, String fileName) {
    final File file = new File(fileName);
    if (baseDirectory != null && !file.isAbsolute()) {
      return of(new File(baseDirectory, fileName));
    } else {
      return of(file);
    }
  }

  public static Source url(String url) {
    try {
      URL url_ = new URL(url);
      return new FileSource(url_);
    } catch (MalformedURLException e) {
      throw new RuntimeException("Malformed URL: '" + url + "'", e);
    }
  }

  /** Looks for a suffix on a path and returns
   * either the path with the suffix removed
   * or null. */
  private static String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  private static boolean isFile(Source source) {
    return source.protocol().equals("file");
  }

  /** Implementation of {@link Source}. */
  private static class FileSource implements Source {
    private final File file;
    private final URL url;

    private FileSource(URL url) {
      this.url = Preconditions.checkNotNull(url);
      if (url.getProtocol().equals("file")) {
        this.file = new File(url.getFile());
      } else {
        this.file = null;
      }
    }

    private FileSource(File file) {
      this.file = Preconditions.checkNotNull(file);
      this.url = null;
    }

    @Override public String toString() {
      return (url != null ? url : file).toString();
    }

    public URL url() {
      if (url == null) {
        throw new UnsupportedOperationException();
      }
      return url;
    }

    public File file() {
      if (file == null) {
        throw new UnsupportedOperationException();
      }
      return file;
    }

    public String protocol() {
      return file != null ? "file" : url.getProtocol();
    }

    public String path() {
      return file != null ? file.getPath() : url.toExternalForm();
    }

    public Reader reader() throws IOException {
      final InputStream is;
      if (path().endsWith(".gz")) {
        final InputStream fis = openStream();
        is = new GZIPInputStream(fis);
      } else {
        is = openStream();
      }
      return new InputStreamReader(is, StandardCharsets.UTF_8);
    }

    public InputStream openStream() throws IOException {
      if (file != null) {
        return new FileInputStream(file);
      } else {
        return url.openStream();
      }
    }

    public Source trim(String suffix) {
      Source x = trimOrNull(suffix);
      return x == null ? this : x;
    }

    public Source trimOrNull(String suffix) {
      if (url != null) {
        final String s = Sources.trimOrNull(url.toExternalForm(), suffix);
        return s == null ? null : Sources.url(s);
      } else {
        final String s = Sources.trimOrNull(file.getPath(), suffix);
        return s == null ? null : of(new File(s));
      }
    }

    public Source append(Source child) {
      String path;
      if (isFile(child)) {
        path = child.file().getPath();
        if (child.file().isAbsolute()) {
          return child;
        }
      } else {
        path = child.url().getPath();
        if (path.startsWith("/")) {
          return child;
        }
      }
      if (url != null) {
        return Sources.url(url + "/" + path);
      } else {
        return Sources.file(file, path);
      }
    }

    public Source relative(Source parent) {
      if (isFile(parent)) {
        if (isFile(this)
            && file.getPath().startsWith(parent.file().getPath())) {
          String rest = file.getPath().substring(parent.file().getPath().length());
          if (rest.startsWith(File.separator)) {
            return Sources.file(null, rest.substring(File.separator.length()));
          }
        }
        return this;
      } else {
        if (!isFile(this)) {
          String rest = Sources.trimOrNull(url.toExternalForm(),
              parent.url().toExternalForm());
          if (rest != null
              && rest.startsWith("/")) {
            return Sources.file(null, rest.substring(1));
          }
        }
        return this;
      }
    }
  }
}

// End Sources.java
