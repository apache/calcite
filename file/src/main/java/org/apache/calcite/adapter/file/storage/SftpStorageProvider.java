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

import org.apache.calcite.adapter.file.storage.cache.PersistentStorageCache;
import org.apache.calcite.adapter.file.storage.cache.StorageCacheManager;

import com.jcraft.jsch.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Vector;

/**
 * Storage provider implementation for SFTP (SSH File Transfer Protocol) access.
 * Uses JSch library for SSH/SFTP connections.
 */
public class SftpStorageProvider implements StorageProvider {

  private static final int DEFAULT_PORT = 22;
  private static final int CONNECT_TIMEOUT = 30000;
  private static final String SFTP_SUBSYSTEM = "sftp";

  private final String defaultUsername;
  private final String defaultPassword;
  private final String defaultPrivateKeyPath;
  private final boolean strictHostKeyChecking;

  // Persistent cache for restart-survivable caching
  private final PersistentStorageCache persistentCache;

  /**
   * Default constructor for factory creation.
   */
  public SftpStorageProvider() {
    this.defaultUsername = System.getProperty("user.name");
    this.defaultPassword = null;
    this.defaultPrivateKeyPath = findDefaultPrivateKey();
    this.strictHostKeyChecking = false;

    // Initialize persistent cache if cache manager is available
    PersistentStorageCache cache = null;
    try {
      cache = StorageCacheManager.getInstance().getCache("sftp");
    } catch (IllegalStateException e) {
      // Cache manager not initialized, persistent cache will be null
    }
    this.persistentCache = cache;
  }

  /**
   * Constructor with configuration options.
   */
  public SftpStorageProvider(String username, String password,
                            String privateKeyPath, boolean strictHostKeyChecking) {
    this.defaultUsername = username != null ? username : System.getProperty("user.name");
    this.defaultPassword = password;
    this.defaultPrivateKeyPath = privateKeyPath != null ? privateKeyPath : findDefaultPrivateKey();
    this.strictHostKeyChecking = strictHostKeyChecking;

    // Initialize persistent cache if cache manager is available
    PersistentStorageCache cache = null;
    try {
      cache = StorageCacheManager.getInstance().getCache("sftp");
    } catch (IllegalStateException e) {
      // Cache manager not initialized, persistent cache will be null
    }
    this.persistentCache = cache;
  }

  private String findDefaultPrivateKey() {
    String[] possibleKeys = {
        System.getProperty("user.home") + "/.ssh/id_rsa",
        System.getProperty("user.home") + "/.ssh/id_ed25519",
        System.getProperty("user.home") + "/.ssh/id_ecdsa",
        System.getProperty("user.home") + "/.ssh/id_dsa"
    };

    for (String keyPath : possibleKeys) {
      if (new java.io.File(keyPath).exists()) {
        return keyPath;
      }
    }
    return null;
  }

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    SftpUri sftpUri = parseSftpUri(path);
    List<FileEntry> entries = new ArrayList<>();

    Session session = null;
    ChannelSftp channelSftp = null;

    try {
      session = createSession(sftpUri);
      channelSftp = openSftpChannel(session);

      listFilesRecursive(channelSftp, sftpUri, sftpUri.path, entries, recursive);
      return entries;
    } catch (JSchException | SftpException e) {
      throw new IOException("SFTP operation failed: " + e.getMessage(), e);
    } finally {
      closeSftpChannel(channelSftp);
      closeSession(session);
    }
  }

  private void listFilesRecursive(ChannelSftp channelSftp, SftpUri baseUri, String currentPath,
                                  List<FileEntry> entries, boolean recursive)
      throws SftpException {

    @SuppressWarnings("unchecked")
    Vector<ChannelSftp.LsEntry> files = channelSftp.ls(currentPath);

    for (ChannelSftp.LsEntry entry : files) {
      String filename = entry.getFilename();

      // Skip . and .. entries
      if (".".equals(filename) || "..".equals(filename)) {
        continue;
      }

      String filePath = currentPath.endsWith("/") ?
          currentPath + filename : currentPath + "/" + filename;

      String fullUri;
      if (baseUri.port != DEFAULT_PORT) {
        fullUri =
            String.format(Locale.ROOT, "sftp://%s@%s:%d%s", baseUri.username, baseUri.host, baseUri.port, filePath);
      } else {
        fullUri =
            String.format(Locale.ROOT, "sftp://%s@%s%s", baseUri.username, baseUri.host, filePath);
      }

      SftpATTRS attrs = entry.getAttrs();
      boolean isDirectory = attrs.isDir();

      entries.add(
          new FileEntry(
          fullUri,
          filename,
          isDirectory,
          attrs.getSize(),
          attrs.getMTime() * 1000L)); // Convert seconds to milliseconds

      if (recursive && isDirectory) {
        listFilesRecursive(channelSftp, baseUri, filePath, entries, true);
      }
    }
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    SftpUri sftpUri = parseSftpUri(path);

    Session session = null;
    ChannelSftp channelSftp = null;

    try {
      session = createSession(sftpUri);
      channelSftp = openSftpChannel(session);

      SftpATTRS attrs = channelSftp.stat(sftpUri.path);

      return new FileMetadata(
          path,
          attrs.getSize(),
          attrs.getMTime() * 1000L, // Convert to milliseconds
          "application/octet-stream", // SFTP doesn't provide content type
          null); // No ETag in SFTP
    } catch (JSchException | SftpException e) {
      throw new IOException("Failed to get metadata for: " + path, e);
    } finally {
      closeSftpChannel(channelSftp);
      closeSession(session);
    }
  }

  @Override public InputStream openInputStream(String path) throws IOException {
    // Check persistent cache first if available
    if (persistentCache != null) {
      byte[] cachedData = persistentCache.getCachedData(path);
      FileMetadata cachedMetadata = persistentCache.getCachedMetadata(path);

      if (cachedData != null && cachedMetadata != null) {
        // Check if cached data is still fresh
        try {
          if (!hasChanged(path, cachedMetadata)) {
            return new java.io.ByteArrayInputStream(cachedData);
          }
        } catch (IOException e) {
          // If we can't check freshness, use cached data anyway
          return new java.io.ByteArrayInputStream(cachedData);
        }
      }
    }

    SftpUri sftpUri = parseSftpUri(path);

    Session session = null;
    ChannelSftp channelSftp = null;

    try {
      session = createSession(sftpUri);
      channelSftp = openSftpChannel(session);

      InputStream stream = channelSftp.get(sftpUri.path);

      // If persistent cache is available, read data and cache it
      if (persistentCache != null) {
        byte[] data = readAllBytes(stream);
        stream.close();
        closeSftpChannel(channelSftp);
        closeSession(session);

        // Get file metadata for caching
        FileMetadata metadata = getMetadata(path);
        persistentCache.cacheData(path, data, metadata, 0); // No TTL for SFTP

        return new java.io.ByteArrayInputStream(data);
      }

      // Return a wrapper that closes the channel and session on stream close
      return new SftpInputStream(stream, channelSftp, session);
    } catch (JSchException | SftpException e) {
      closeSftpChannel(channelSftp);
      closeSession(session);
      throw new IOException("Failed to open file: " + path, e);
    }
  }

  @Override public Reader openReader(String path) throws IOException {
    return new InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) throws IOException {
    SftpUri sftpUri = parseSftpUri(path);

    Session session = null;
    ChannelSftp channelSftp = null;

    try {
      session = createSession(sftpUri);
      channelSftp = openSftpChannel(session);

      channelSftp.stat(sftpUri.path);
      return true;
    } catch (SftpException e) {
      if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
        return false;
      }
      throw new IOException("Failed to check existence: " + path, e);
    } catch (JSchException e) {
      throw new IOException("SFTP connection failed", e);
    } finally {
      closeSftpChannel(channelSftp);
      closeSession(session);
    }
  }

  @Override public boolean isDirectory(String path) throws IOException {
    SftpUri sftpUri = parseSftpUri(path);

    Session session = null;
    ChannelSftp channelSftp = null;

    try {
      session = createSession(sftpUri);
      channelSftp = openSftpChannel(session);

      SftpATTRS attrs = channelSftp.stat(sftpUri.path);
      return attrs.isDir();
    } catch (JSchException | SftpException e) {
      throw new IOException("Failed to check if directory: " + path, e);
    } finally {
      closeSftpChannel(channelSftp);
      closeSession(session);
    }
  }

  @Override public String getStorageType() {
    return "sftp";
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    if (relativePath.startsWith("sftp://")) {
      return relativePath;
    }

    try {
      SftpUri baseUri = parseSftpUri(basePath);
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
      if (baseUri.port != DEFAULT_PORT) {
        return String.format(Locale.ROOT, "sftp://%s@%s:%d%s",
            baseUri.username, baseUri.host, baseUri.port, resolvedPath);
      } else {
        return String.format(Locale.ROOT, "sftp://%s@%s%s",
            baseUri.username, baseUri.host, resolvedPath);
      }
    } catch (IOException e) {
      // Fallback
      if (basePath.endsWith("/")) {
        return basePath + relativePath;
      } else {
        return basePath + "/" + relativePath;
      }
    }
  }

  private Session createSession(SftpUri sftpUri) throws JSchException {
    JSch jsch = new JSch();

    // Add known hosts if available
    String knownHosts = System.getProperty("user.home") + "/.ssh/known_hosts";
    if (new java.io.File(knownHosts).exists()) {
      jsch.setKnownHosts(knownHosts);
    }

    // Add private key if specified
    if (sftpUri.privateKeyPath != null) {
      if (sftpUri.passphrase != null) {
        jsch.addIdentity(sftpUri.privateKeyPath, sftpUri.passphrase);
      } else {
        jsch.addIdentity(sftpUri.privateKeyPath);
      }
    }

    Session session = jsch.getSession(sftpUri.username, sftpUri.host, sftpUri.port);

    if (sftpUri.password != null) {
      session.setPassword(sftpUri.password);
    }

    // Configure session
    Properties config = new Properties();
    config.put("StrictHostKeyChecking", sftpUri.strictHostKeyChecking ? "yes" : "no");
    config.put("PreferredAuthentications", "publickey,password");
    session.setConfig(config);

    session.setTimeout(CONNECT_TIMEOUT);
    session.connect();

    return session;
  }

  private ChannelSftp openSftpChannel(Session session) throws JSchException {
    Channel channel = session.openChannel(SFTP_SUBSYSTEM);
    channel.connect();
    return (ChannelSftp) channel;
  }

  private void closeSftpChannel(ChannelSftp channel) {
    if (channel != null && channel.isConnected()) {
      channel.disconnect();
    }
  }

  private void closeSession(Session session) {
    if (session != null && session.isConnected()) {
      session.disconnect();
    }
  }

  private byte[] readAllBytes(InputStream inputStream) throws IOException {
    java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
    byte[] data = new byte[8192];
    int nRead;
    while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }
    return buffer.toByteArray();
  }

  private SftpUri parseSftpUri(String uri) throws IOException {
    if (!uri.startsWith("sftp://")) {
      throw new IOException("Invalid SFTP URI: " + uri);
    }

    try {
      URI parsed = new URI(uri);
      String host = parsed.getHost();
      int port = parsed.getPort() > 0 ? parsed.getPort() : DEFAULT_PORT;
      String path = parsed.getPath() != null ? parsed.getPath() : "/";

      String userInfo = parsed.getUserInfo();
      String username = defaultUsername;
      String password = defaultPassword;

      if (userInfo != null) {
        int colonIndex = userInfo.indexOf(':');
        if (colonIndex > 0) {
          username = userInfo.substring(0, colonIndex);
          password = userInfo.substring(colonIndex + 1);
        } else {
          username = userInfo;
        }
      }

      // Use configured private key path
      String privateKeyPath = defaultPrivateKeyPath;

      return new SftpUri(host, port, username, password, path,
                        privateKeyPath, null, strictHostKeyChecking);
    } catch (Exception e) {
      throw new IOException("Failed to parse SFTP URI: " + uri, e);
    }
  }

  private static class SftpUri {
    final String host;
    final int port;
    final String username;
    final String password;
    final String path;
    final String privateKeyPath;
    final String passphrase;
    final boolean strictHostKeyChecking;

    SftpUri(String host, int port, String username, String password, String path,
            String privateKeyPath, String passphrase, boolean strictHostKeyChecking) {
      this.host = host;
      this.port = port;
      this.username = username;
      this.password = password;
      this.path = path;
      this.privateKeyPath = privateKeyPath;
      this.passphrase = passphrase;
      this.strictHostKeyChecking = strictHostKeyChecking;
    }
  }

  private static class SftpInputStream extends InputStream {
    private final InputStream wrapped;
    private final ChannelSftp channel;
    private final Session session;

    SftpInputStream(InputStream wrapped, ChannelSftp channel, Session session) {
      this.wrapped = wrapped;
      this.channel = channel;
      this.session = session;
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
        closeSftpChannel(channel);
        closeSession(session);
      }
    }

    private void closeSftpChannel(ChannelSftp channel) {
      if (channel != null && channel.isConnected()) {
        channel.disconnect();
      }
    }

    private void closeSession(Session session) {
      if (session != null && session.isConnected()) {
        session.disconnect();
      }
    }
  }
}
