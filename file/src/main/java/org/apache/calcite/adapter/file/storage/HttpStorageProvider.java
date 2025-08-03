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
package org.apache.calcite.adapter.file.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Storage provider implementation for HTTP/HTTPS access.
 */
public class HttpStorageProvider implements StorageProvider {

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    // HTTP doesn't support directory listing
    throw new UnsupportedOperationException("HTTP storage does not support directory listing");
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    URL url;
    try {
      url = new URI(path).toURL();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URL: " + path, e);
    }
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("HEAD");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    try {
      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException("HTTP error code: " + responseCode + " for URL: " + path);
      }

      long contentLength = conn.getContentLengthLong();
      long lastModified = conn.getLastModified();
      String contentType = conn.getContentType();
      String etag = conn.getHeaderField("ETag");

      return new FileMetadata(path, contentLength, lastModified, contentType, etag);
    } finally {
      conn.disconnect();
    }
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    URL url;
    try {
      url = new URI(path).toURL();
    } catch (URISyntaxException e) {
      throw new IOException("Invalid URL: " + path, e);
    }
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    int responseCode = conn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      conn.disconnect();
      throw new IOException("HTTP error code: " + responseCode + " for URL: " + path);
    }

    return conn.getInputStream();
  }

  @Override public Reader openReader(String path) throws IOException {
    return new InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) throws IOException {
    try {
      URL url = new URI(path).toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("HEAD");
      conn.setConnectTimeout(30000);
      conn.setReadTimeout(30000);

      try {
        int responseCode = conn.getResponseCode();
        return responseCode == HttpURLConnection.HTTP_OK;
      } finally {
        conn.disconnect();
      }
    } catch (IOException | URISyntaxException e) {
      return false;
    }
  }

  @Override public boolean isDirectory(String path) {
    // HTTP URLs are never directories in the traditional sense
    return false;
  }

  @Override public String getStorageType() {
    return "http";
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    try {
      URI baseUri = new URI(basePath);
      URI resolved = baseUri.resolve(relativePath);
      return resolved.toString();
    } catch (Exception e) {
      // Fallback to simple string concatenation
      if (basePath.endsWith("/")) {
        return basePath + relativePath;
      } else {
        return basePath + "/" + relativePath;
      }
    }
  }
}
