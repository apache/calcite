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

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Storage provider implementation for FTP access.
 */
public class FtpStorageProvider implements StorageProvider {

  private static final int DEFAULT_PORT = 21;
  private static final int CONNECT_TIMEOUT = 30000;
  private static final int DATA_TIMEOUT = 60000;

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    FtpUri ftpUri = parseFtpUri(path);
    List<FileEntry> entries = new ArrayList<>();

    FTPClient ftpClient = createAndConnect(ftpUri);
    try {
      listFilesRecursive(ftpClient, ftpUri, ftpUri.path, entries, recursive);
      return entries;
    } finally {
      disconnect(ftpClient);
    }
  }

  private void listFilesRecursive(FTPClient ftpClient, FtpUri baseUri, String currentPath,
                                  List<FileEntry> entries, boolean recursive) throws IOException {
    FTPFile[] files = ftpClient.listFiles(currentPath);

    for (FTPFile file : files) {
      String filePath = currentPath.endsWith("/") ?
          currentPath + file.getName() : currentPath + "/" + file.getName();

      String fullUri =
          String.format(Locale.ROOT, "ftp://%s:%d%s", baseUri.host, baseUri.port, filePath);

      entries.add(
          new FileEntry(
          fullUri,
          file.getName(),
          file.isDirectory(),
          file.getSize(),
          file.getTimestamp().getTimeInMillis()));

      if (recursive && file.isDirectory()) {
        listFilesRecursive(ftpClient, baseUri, filePath, entries, true);
      }
    }
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    FtpUri ftpUri = parseFtpUri(path);

    FTPClient ftpClient = createAndConnect(ftpUri);
    try {
      FTPFile[] files = ftpClient.listFiles(ftpUri.path);
      if (files.length == 0) {
        throw new IOException("File not found: " + path);
      }

      FTPFile file = files[0];
      return new FileMetadata(
          path,
          file.getSize(),
          file.getTimestamp().getTimeInMillis(),
          "application/octet-stream", // FTP doesn't provide content type
          null); // No ETag in FTP
    } finally {
      disconnect(ftpClient);
    }
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    FtpUri ftpUri = parseFtpUri(path);

    FTPClient ftpClient = createAndConnect(ftpUri);
    ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

    InputStream stream = ftpClient.retrieveFileStream(ftpUri.path);
    if (stream == null) {
      disconnect(ftpClient);
      throw new IOException("Failed to retrieve file: " + path);
    }

    // Return a wrapper that disconnects on close
    return new FtpInputStream(stream, ftpClient);
  }

  @Override public Reader openReader(String path) throws IOException {
    return new InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) throws IOException {
    FtpUri ftpUri = parseFtpUri(path);

    FTPClient ftpClient = createAndConnect(ftpUri);
    try {
      FTPFile[] files = ftpClient.listFiles(ftpUri.path);
      return files.length > 0;
    } finally {
      disconnect(ftpClient);
    }
  }

  @Override public boolean isDirectory(String path) throws IOException {
    FtpUri ftpUri = parseFtpUri(path);

    FTPClient ftpClient = createAndConnect(ftpUri);
    try {
      return ftpClient.changeWorkingDirectory(ftpUri.path);
    } finally {
      disconnect(ftpClient);
    }
  }

  @Override public String getStorageType() {
    return "ftp";
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    if (relativePath.startsWith("ftp://")) {
      return relativePath;
    }

    try {
      FtpUri baseUri = parseFtpUri(basePath);
      String baseDirPath = baseUri.path;
      if (!baseDirPath.endsWith("/")) {
        int lastSlash = baseDirPath.lastIndexOf('/');
        if (lastSlash >= 0) {
          baseDirPath = baseDirPath.substring(0, lastSlash + 1);
        } else {
          baseDirPath = "/";
        }
      }

      String resolvedPath = baseDirPath + relativePath;
      return String.format(Locale.ROOT, "ftp://%s:%d%s", baseUri.host, baseUri.port, resolvedPath);
    } catch (IOException e) {
      // Fallback
      if (basePath.endsWith("/")) {
        return basePath + relativePath;
      } else {
        return basePath + "/" + relativePath;
      }
    }
  }

  private FTPClient createAndConnect(FtpUri ftpUri) throws IOException {
    FTPClient ftpClient = new FTPClient();
    ftpClient.setConnectTimeout(CONNECT_TIMEOUT);
    ftpClient.setDefaultTimeout(DATA_TIMEOUT);

    ftpClient.connect(ftpUri.host, ftpUri.port);
    
    // Set socket timeout after connection is established
    ftpClient.setSoTimeout(DATA_TIMEOUT);

    int replyCode = ftpClient.getReplyCode();
    if (!FTPReply.isPositiveCompletion(replyCode)) {
      ftpClient.disconnect();
      throw new IOException("FTP server refused connection: " + replyCode);
    }

    if (!ftpClient.login(ftpUri.username, ftpUri.password)) {
      ftpClient.disconnect();
      throw new IOException("FTP login failed");
    }

    ftpClient.enterLocalPassiveMode();
    ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

    return ftpClient;
  }

  private void disconnect(FTPClient ftpClient) {
    try {
      if (ftpClient.isConnected()) {
        ftpClient.logout();
        ftpClient.disconnect();
      }
    } catch (IOException e) {
      // Ignore disconnect errors
    }
  }

  private FtpUri parseFtpUri(String uri) throws IOException {
    if (!uri.startsWith("ftp://")) {
      throw new IOException("Invalid FTP URI: " + uri);
    }

    try {
      URI parsed = new URI(uri);
      String host = parsed.getHost();
      int port = parsed.getPort() > 0 ? parsed.getPort() : DEFAULT_PORT;
      String path = parsed.getPath() != null ? parsed.getPath() : "/";

      String userInfo = parsed.getUserInfo();
      String username = "anonymous";
      String password = "anonymous@";

      if (userInfo != null) {
        int colonIndex = userInfo.indexOf(':');
        if (colonIndex > 0) {
          username = userInfo.substring(0, colonIndex);
          password = userInfo.substring(colonIndex + 1);
        } else {
          username = userInfo;
        }
      }

      return new FtpUri(host, port, username, password, path);
    } catch (Exception e) {
      throw new IOException("Failed to parse FTP URI: " + uri, e);
    }
  }

  private static class FtpUri {
    final String host;
    final int port;
    final String username;
    final String password;
    final String path;

    FtpUri(String host, int port, String username, String password, String path) {
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.path = path;
    }
  }

  private static class FtpInputStream extends InputStream {
    private final InputStream wrapped;
    private final FTPClient ftpClient;

    FtpInputStream(InputStream wrapped, FTPClient ftpClient) {
      this.wrapped = wrapped;
      this.ftpClient = ftpClient;
    }

    @Override public int read() throws IOException {
      return wrapped.read();
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
      return wrapped.read(b, off, len);
    }

    @Override public void close() throws IOException {
      try {
        wrapped.close();
      } finally {
        if (ftpClient.isConnected()) {
          ftpClient.completePendingCommand();
          try {
            ftpClient.logout();
            ftpClient.disconnect();
          } catch (IOException e) {
            // Ignore
          }
        }
      }
    }
  }
}
