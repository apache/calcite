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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import static java.util.Objects.requireNonNull;

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


  public static Source file(@Nullable File baseDirectory, String fileName) {
    final File file = new File(fileName);
    if (baseDirectory != null && !file.isAbsolute()) {
      return of(new File(baseDirectory, fileName));
    } else {
      return of(file);
    }
  }

  /** Creates a {@link Source} from a character sequence such as a
   * {@link String}. */
  public static Source of(CharSequence s) {
    return fromCharSource(CharSource.wrap(s));
  }

  /** Creates a {@link Source} from a generic text source such as string,
   * {@link java.nio.CharBuffer} or text file. Useful when data is already
   * in memory or can't be directly read from
   * a file or url.
   *
   * @param source generic "re-readable" source of characters
   * @return {@code Source} delegate for {@code CharSource} (can't be null)
   * @throws NullPointerException when {@code source} is null
   */
  public static Source fromCharSource(CharSource source) {
    return new GuavaCharSource(source);
  }

  public static Source url(String url) {
    try {
      return of(URI.create(url).toURL());
    } catch (MalformedURLException | IllegalArgumentException e) {
      throw new RuntimeException("Malformed URL: '" + url + "'", e);
    }
  }

  /** Looks for a suffix on a path and returns
   * either the path with the suffix removed
   * or null. */
  private static @Nullable String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  private static boolean isFile(Source source) {
    return source.protocol().equals("file");
  }

  /** Adapter for {@link CharSource}. */
  private static class GuavaCharSource implements Source {
    private final CharSource charSource;

    private GuavaCharSource(CharSource charSource) {
      this.charSource = requireNonNull(charSource, "charSource");
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

    @Override public Optional<File> fileOpt() {
      return Optional.empty();
    }

    @Override public String path() {
      throw unsupported();
    }

    @Override public Reader reader() throws IOException {
      return charSource.openStream();
    }

    @Override public InputStream openStream() throws IOException {
      return charSource.asByteSource(StandardCharsets.UTF_8).openStream();
    }

    @Override public String protocol() {
      return "memory";
    }

    @Override public Source trim(final String suffix) {
      throw unsupported();
    }

    @Override public @Nullable Source trimOrNull(final String suffix) {
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

  /** Implementation of {@link Source} on the top of a {@link File} or
   * {@link URL}. */
  private static class FileSource implements Source {
    private final @Nullable File file;
    private final URL url;

    /**
     * A flag indicating if the url is deduced from the file object.
     */
    private final boolean urlGenerated;

    private FileSource(URL url) {
      this.url = requireNonNull(url, "url");
      this.file = urlToFile(url);
      this.urlGenerated = false;
    }

    private FileSource(File file) {
      this.file = requireNonNull(file, "file");
      this.url = fileToUrl(file);
      this.urlGenerated = true;
    }

    private File fileNonNull() {
      return requireNonNull(file, "file");
    }

    private static @Nullable File urlToFile(URL url) {
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

    private static URL fileToUrl(File file) {
      String filePath = file.getPath();
      if (!file.isAbsolute()) {
        // convert relative file paths
        filePath = filePath.replace(File.separatorChar, '/');
        if (file.isDirectory() && !filePath.endsWith("/")) {
          filePath += "/";
        }
        try {
          // We need to encode path. For instance, " " should become "%20"
          // That is why java.net.URLEncoder.encode(java.lang.String, java.lang.String) is not
          // suitable because it replaces " " with "+".
          String encodedPath = new URI(null, null, filePath, null).getRawPath();
          return URI.create("file:" + encodedPath).toURL();
        } catch (MalformedURLException | URISyntaxException e) {
          throw new IllegalArgumentException("Unable to create URL for file " + filePath, e);
        }
      }

      URI uri = null;
      try {
        // convert absolute file paths
        uri = file.toURI();
        return uri.toURL();
      } catch (SecurityException e) {
        throw new IllegalArgumentException("No access to the underlying file " + filePath, e);
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException("Unable to convert URI " + uri + " to URL", e);
      }
    }

    @Override public String toString() {
      return (urlGenerated ? fileNonNull() : url).toString();
    }

    @Override public URL url() {
      return url;
    }

    @Override public File file() {
      if (file == null) {
        throw new UnsupportedOperationException();
      }
      return file;
    }

    @Override public Optional<File> fileOpt() {
      return Optional.ofNullable(file);
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
        return Files.newInputStream(file.toPath());
      } else {
        return url.openStream();
      }
    }

    @Override public Source trim(String suffix) {
      Source x = trimOrNull(suffix);
      return x == null ? this : x;
    }

    @Override public @Nullable Source trimOrNull(String suffix) {
      if (!urlGenerated) {
        final String s = Sources.trimOrNull(url.toExternalForm(), suffix);
        return s == null ? null : Sources.url(s);
      } else {
        final String s = Sources.trimOrNull(fileNonNull().getPath(), suffix);
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
      if (!urlGenerated) {
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
            && fileNonNull().getPath().startsWith(parent.file().getPath())) {
          String rest =
              fileNonNull().getPath().substring(parent.file().getPath().length());
          if (rest.startsWith(File.separator)) {
            return Sources.file(null, rest.substring(File.separator.length()));
          }
        }
        return this;
      } else {
        if (!isFile(this)) {
          String rest =
              Sources.trimOrNull(url.toExternalForm(),
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
