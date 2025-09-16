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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Storage provider implementation for Amazon S3.
 */
public class S3StorageProvider implements StorageProvider {

  private final AmazonS3 s3Client;

  // Persistent cache for restart-survivable caching
  private final PersistentStorageCache persistentCache;

  public S3StorageProvider() {
    AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
        .withCredentials(new DefaultAWSCredentialsProviderChain());

    // Try to get region from default provider chain, fallback to us-west-1 if not available
    try {
      String region = new DefaultAwsRegionProviderChain().getRegion();
      builder.withRegion(region);
    } catch (Exception e) {
      // If no region is configured, use us-west-1 as default
      builder.withRegion("us-west-1");
    }

    this.s3Client = builder.build();

    // Initialize persistent cache if cache manager is available
    PersistentStorageCache cache = null;
    try {
      cache = StorageCacheManager.getInstance().getCache("s3");
    } catch (IllegalStateException e) {
      // Cache manager not initialized, persistent cache will be null
    }
    this.persistentCache = cache;
  }

  public S3StorageProvider(AmazonS3 s3Client) {
    this.s3Client = s3Client;

    // Initialize persistent cache if cache manager is available
    PersistentStorageCache cache = null;
    try {
      cache = StorageCacheManager.getInstance().getCache("s3");
    } catch (IllegalStateException e) {
      // Cache manager not initialized, persistent cache will be null
    }
    this.persistentCache = cache;
  }

  @Override public List<FileEntry> listFiles(String path, boolean recursive) throws IOException {
    S3Uri s3Uri = parseS3Uri(path);
    List<FileEntry> entries = new ArrayList<>();

    ListObjectsV2Request request = new ListObjectsV2Request()
        .withBucketName(s3Uri.bucket)
        .withPrefix(s3Uri.key);

    if (!recursive) {
      request.withDelimiter("/");
    }

    ListObjectsV2Result result;
    do {
      result = s3Client.listObjectsV2(request);

      for (S3ObjectSummary summary : result.getObjectSummaries()) {
        if (!summary.getKey().equals(s3Uri.key)) { // Skip the directory itself
          entries.add(
              new FileEntry(
              "s3://" + s3Uri.bucket + "/" + summary.getKey(),
              getFileName(summary.getKey()),
              false,
              summary.getSize(),
              summary.getLastModified().getTime()));
        }
      }

      // Add directories when not recursive
      if (!recursive && result.getCommonPrefixes() != null) {
        for (String prefix : result.getCommonPrefixes()) {
          entries.add(
              new FileEntry(
              "s3://" + s3Uri.bucket + "/" + prefix,
              getFileName(prefix.endsWith("/") ?
                  prefix.substring(0, prefix.length() - 1) : prefix),
              true,
              0,
              0));
        }
      }

      request.setContinuationToken(result.getNextContinuationToken());
    } while (result.isTruncated());

    return entries;
  }

  @Override public FileMetadata getMetadata(String path) throws IOException {
    S3Uri s3Uri = parseS3Uri(path);

    com.amazonaws.services.s3.model.ObjectMetadata metadata =
        s3Client.getObjectMetadata(s3Uri.bucket, s3Uri.key);

    return new FileMetadata(
        path,
        metadata.getContentLength(),
        metadata.getLastModified().getTime(),
        metadata.getContentType(),
        metadata.getETag());
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

    S3Uri s3Uri = parseS3Uri(path);
    GetObjectRequest request = new GetObjectRequest(s3Uri.bucket, s3Uri.key);
    S3Object object = s3Client.getObject(request);

    // If persistent cache is available, read data and cache it
    if (persistentCache != null) {
      byte[] data = readAllBytes(object.getObjectContent());
      object.close();

      // Get file metadata for caching (use S3 object metadata)
      FileMetadata metadata =
          new FileMetadata(path, object.getObjectMetadata().getContentLength(),
          object.getObjectMetadata().getLastModified().getTime(),
          object.getObjectMetadata().getContentType(),
          object.getObjectMetadata().getETag());
      persistentCache.cacheData(path, data, metadata, 0); // No TTL for S3

      return new java.io.ByteArrayInputStream(data);
    }

    return object.getObjectContent();
  }

  @Override public Reader openReader(String path) throws IOException {
    return new InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
  }

  @Override public boolean exists(String path) throws IOException {
    try {
      S3Uri s3Uri = parseS3Uri(path);
      return s3Client.doesObjectExist(s3Uri.bucket, s3Uri.key);
    } catch (Exception e) {
      return false;
    }
  }

  @Override public boolean isDirectory(String path) throws IOException {
    S3Uri s3Uri = parseS3Uri(path);

    // In S3, directories are conceptual. Check if there are objects with this prefix
    ListObjectsV2Request request = new ListObjectsV2Request()
        .withBucketName(s3Uri.bucket)
        .withPrefix(s3Uri.key.endsWith("/") ? s3Uri.key : s3Uri.key + "/")
        .withMaxKeys(1);

    ListObjectsV2Result result = s3Client.listObjectsV2(request);
    return result.getKeyCount() > 0;
  }

  @Override public String getStorageType() {
    return "s3";
  }

  @Override public String resolvePath(String basePath, String relativePath) {
    if (relativePath.startsWith("s3://")) {
      return relativePath;
    }

    // If basePath doesn't end with /, it might be a file
    // Strip the filename part to get the directory
    if (!basePath.endsWith("/")) {
      int lastSlash = basePath.lastIndexOf('/');
      if (lastSlash > "s3://".length()) {
        // Check if the part after the last slash looks like a file (has extension)
        String lastPart = basePath.substring(lastSlash + 1);
        if (lastPart.contains(".")) {
          // It's likely a file, use the directory part
          basePath = basePath.substring(0, lastSlash + 1);
        } else {
          // It's likely a directory without trailing slash, add one
          basePath = basePath + "/";
        }
      } else {
        basePath = basePath + "/";
      }
    }

    return basePath + relativePath;
  }

  private S3Uri parseS3Uri(String uri) throws IOException {
    if (!uri.startsWith("s3://")) {
      throw new IOException("Invalid S3 URI: " + uri);
    }

    try {
      URI parsed = new URI(uri);
      String bucket = parsed.getHost();
      String key = parsed.getPath();
      if (key.startsWith("/")) {
        key = key.substring(1);
      }
      return new S3Uri(bucket, key);
    } catch (Exception e) {
      throw new IOException("Failed to parse S3 URI: " + uri, e);
    }
  }

  private String getFileName(String key) {
    int lastSlash = key.lastIndexOf('/');
    if (lastSlash >= 0 && lastSlash < key.length() - 1) {
      return key.substring(lastSlash + 1);
    }
    return key;
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

  @Override public void writeFile(String path, byte[] content) throws IOException {
    S3Uri s3Uri = parseS3Uri(path);
    
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(content.length);
    
    // Set content type based on file extension
    String contentType = guessContentType(path);
    if (contentType != null) {
      metadata.setContentType(contentType);
    }
    
    try (InputStream input = new ByteArrayInputStream(content)) {
      PutObjectRequest request = new PutObjectRequest(s3Uri.bucket, s3Uri.key, input, metadata);
      s3Client.putObject(request);
    } catch (AmazonServiceException e) {
      throw new IOException("Failed to write file to S3: " + path, e);
    }
  }

  @Override public void writeFile(String path, InputStream content) throws IOException {
    S3Uri s3Uri = parseS3Uri(path);
    
    // For input streams, we need to buffer the content to determine size
    // This is required for S3 uploads unless using multipart upload
    byte[] buffer = readAllBytes(content);
    writeFile(path, buffer);
  }

  @Override public void createDirectories(String path) throws IOException {
    // S3 doesn't have real directories, they're just prefixes
    // We can create a marker object if needed, but it's often not necessary
    // For compatibility, we'll create an empty object with a trailing slash
    if (!path.endsWith("/")) {
      path = path + "/";
    }
    
    S3Uri s3Uri = parseS3Uri(path);
    
    // Create an empty marker object
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(0);
    
    try (InputStream emptyContent = new ByteArrayInputStream(new byte[0])) {
      PutObjectRequest request = new PutObjectRequest(s3Uri.bucket, s3Uri.key, emptyContent, metadata);
      s3Client.putObject(request);
    } catch (AmazonServiceException e) {
      // Ignore if it already exists
      if (e.getStatusCode() != 409) { // 409 = Conflict
        throw new IOException("Failed to create directory marker in S3: " + path, e);
      }
    }
  }

  @Override public boolean delete(String path) throws IOException {
    S3Uri s3Uri = parseS3Uri(path);
    
    try {
      // Check if object exists first
      if (!s3Client.doesObjectExist(s3Uri.bucket, s3Uri.key)) {
        return false;
      }
      
      // Delete the object
      DeleteObjectRequest request = new DeleteObjectRequest(s3Uri.bucket, s3Uri.key);
      s3Client.deleteObject(request);
      return true;
    } catch (AmazonServiceException e) {
      throw new IOException("Failed to delete S3 object: " + path, e);
    }
  }

  @Override public void copyFile(String source, String destination) throws IOException {
    S3Uri sourceUri = parseS3Uri(source);
    S3Uri destUri = parseS3Uri(destination);
    
    try {
      // Check if source exists
      if (!s3Client.doesObjectExist(sourceUri.bucket, sourceUri.key)) {
        throw new IOException("Source file does not exist in S3: " + source);
      }
      
      // Perform the copy
      CopyObjectRequest copyRequest = new CopyObjectRequest(
          sourceUri.bucket, sourceUri.key,
          destUri.bucket, destUri.key);
      
      s3Client.copyObject(copyRequest);
    } catch (AmazonServiceException e) {
      throw new IOException("Failed to copy S3 object from " + source + " to " + destination, e);
    }
  }

  /**
   * Guess content type based on file extension.
   */
  private String guessContentType(String path) {
    String lowercasePath = path.toLowerCase();
    if (lowercasePath.endsWith(".json")) {
      return "application/json";
    } else if (lowercasePath.endsWith(".csv")) {
      return "text/csv";
    } else if (lowercasePath.endsWith(".parquet")) {
      return "application/x-parquet";
    } else if (lowercasePath.endsWith(".xml")) {
      return "application/xml";
    } else if (lowercasePath.endsWith(".txt")) {
      return "text/plain";
    } else if (lowercasePath.endsWith(".yaml") || lowercasePath.endsWith(".yml")) {
      return "application/x-yaml";
    }
    return "application/octet-stream";
  }

  private static class S3Uri {
    final String bucket;
    final String key;

    S3Uri(String bucket, String key) {
      this.bucket = bucket;
      this.key = key;
    }
  }
}
