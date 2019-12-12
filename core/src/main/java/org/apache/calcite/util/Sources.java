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

import org.apache.commons.io.input.ReaderInputStream;

import com.google.common.io.CharSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

/**
 * Utilities for {@link Source}.
 */
public abstract class Sources {
  private Sources() {}

  public static Source of(File file) {
    return new FileSource(file);
  }

  public static Source of(URL url) {
    return new FileSource(url);
  }


  public static Source file(File baseDirectory, String fileName) {
    final File file = new File(fileName);
    if (baseDirectory != null && !file.isAbsolute()) {
      return of(new File(baseDirectory, fileName));
    } else {
      return of(file);
    }
  }

  /**
   * Create {@link Source} from a generic text source such as string, {@link java.nio.CharBuffer}
   * or text file. Useful when data is already in memory or can't be directly read from
   * a file or url.
   *
   * @param source generic "re-redable" source of characters
   * @return {@code Source} delegate for {@code CharSource} (can't be null)
   * @throws NullPointerException when {@code source} is null
   */
  public static Source fromCharSource(CharSource source) {
    return new GuavaCharSource(source);
  }

  public static Source url(String url) {
    try {
      return of(new URL(url));
    } catch (MalformedURLException | IllegalArgumentException e) {
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

  /**
   * Adapter for {@link CharSource}
   */
  private static class GuavaCharSource implements Source {
    private final CharSource charSource;

    private GuavaCharSource(CharSource charSource) {
      this.charSource = Objects.requireNonNull(charSource, "charSource");
    }

    private UnsupportedOperationException unsupported() {
      return new UnsupportedOperationException(
          String.format(Locale.ROOT, "Invalid operation for '%s' protocol", protocol()));
    }

    @Override public URL url() {
      throw unsupported();
    }

    @Override public File file() {
      throw unsupported();
    }

    @Override public String path() {
      throw unsupported();
    }

    @Override public Reader reader() throws IOException {
      return charSource.openStream();
    }

    @Override public InputStream openStream() throws IOException {
      // use charSource.asByteSource() once calcite can use guava v21+
      return new ReaderInputStream(reader(), StandardCharsets.UTF_8.name());
    }

    @Override public String protocol() {
      return "memory";
    }

    @Override public Source trim(final String suffix) {
      throw unsupported();
    }

    @Override public Source trimOrNull(final String suffix) {
      throw unsupported();
    }

    @Override public Source append(final Source child) {
      throw unsupported();
    }

    @Override public Source relative(final Source source) {
      throw unsupported();
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "{" + protocol() + "}";
    }
  }

  /** Implementation of {@link Source} on the top of a {@link File} or {@link URL} */
  private static class FileSource implements Source {
    private final File file;
    private final URL url;

    private FileSource(URL url) {
      this.url = Objects.requireNonNull(url);
      this.file = urlToFile(url);
    }

    private FileSource(File file) {
      this.file = Objects.requireNonNull(file);
      this.url = null;
    }

    private static File urlToFile(URL url) {
      if (!"file".equals(url.getProtocol())) {
        return null;
      }
      URI uri;
      try {
        uri = url.toURI();
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Unable to convert URL " + url + " to URI", e);
      }
      if (uri.isOpaque()) {
        // It is like file:test%20file.c++
        // getSchemeSpecificPart would return "test file.c++"
        return new File(uri.getSchemeSpecificPart());
      }
      // See https://stackoverflow.com/a/17870390/1261287
      return Paths.get(uri).toFile();
    }

    @Override public String toString() {
      return (url != null ? url : file).toString();
    }

    @Override public URL url() {
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

    @Override public String protocol() {
      return file != null ? "file" : url.getProtocol();
    }

    @Override public String path() {
      if (file != null) {
        return file.getPath();
      }
      try {
        // Decode %20 and friends
        return url.toURI().getSchemeSpecificPart();
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Unable to convert URL " + url + " to URI", e);
      }
    }

    @Override public Reader reader() throws IOException {
      final InputStream is;
      if (path().endsWith(".gz")) {
        final InputStream fis = openStream();
        is = new GZIPInputStream(fis);
      } else {
        is = openStream();
      }
      return new InputStreamReader(is, StandardCharsets.UTF_8);
    }

    @Override public InputStream openStream() throws IOException {
      if (file != null) {
        return new FileInputStream(file);
      } else {
        return url.openStream();
      }
    }

    @Override public Source trim(String suffix) {
      Source x = trimOrNull(suffix);
      return x == null ? this : x;
    }

    @Override public Source trimOrNull(String suffix) {
      if (url != null) {
        final String s = Sources.trimOrNull(url.toExternalForm(), suffix);
        return s == null ? null : Sources.url(s);
      } else {
        final String s = Sources.trimOrNull(file.getPath(), suffix);
        return s == null ? null : of(new File(s));
      }
    }

    @Override public Source append(Source child) {
      if (isFile(child)) {
        if (child.file().isAbsolute()) {
          return child;
        }
      } else {
        try {
          URI uri = child.url().toURI();
          if (!uri.isOpaque()) {
            // The URL is "absolute" (it starts with a slash)
            return child;
          }
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException("Unable to convert URL " + child.url() + " to URI", e);
        }
      }
      String path = child.path();
      if (url != null) {
        String encodedPath = new File(".").toURI().relativize(new File(path).toURI())
            .getRawSchemeSpecificPart();
        return Sources.url(url + "/" + encodedPath);
      } else {
        return Sources.file(file, path);
      }
    }

    @Override public Source relative(Source parent) {
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
