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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.DefaultAwsRegionProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

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

  public S3StorageProvider() {
    this.s3Client = AmazonS3ClientBuilder.standard()
        .withCredentials(new DefaultAWSCredentialsProviderChain())
        .withRegion(new DefaultAwsRegionProviderChain().getRegion())
        .build();
  }

  public S3StorageProvider(AmazonS3 s3Client) {
    this.s3Client = s3Client;
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
    S3Uri s3Uri = parseS3Uri(path);
    GetObjectRequest request = new GetObjectRequest(s3Uri.bucket, s3Uri.key);
    S3Object object = s3Client.getObject(request);
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

    if (basePath.endsWith("/")) {
      return basePath + relativePath;
    } else {
      return basePath + "/" + relativePath;
    }
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

  private static class S3Uri {
    final String bucket;
    final String key;

    S3Uri(String bucket, String key) {
      this.bucket = bucket;
      this.key = key;
    }
  }
}
