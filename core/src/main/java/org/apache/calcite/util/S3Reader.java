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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class S3Reader {
  public static Region getBucketRegion(String bucketName) {
    S3Client s3Client = S3Client.builder().build();
    GetBucketLocationRequest locationRequest = GetBucketLocationRequest.builder()
        .bucket(bucketName)
        .build();
    GetBucketLocationResponse locationResponse = s3Client.getBucketLocation(locationRequest);
    String regionStr = locationResponse.locationConstraintAsString();
    // Handle the case where the region is null (default region is us-east-1)
    if (regionStr == null) {
      return Region.US_EAST_1;
    }
    return Region.of(regionStr);
  }
  public static InputStream getS3ObjectStream(String s3Uri) throws IOException {
    String uriPattern = "^s3:\\/\\/(?<bucket>[^\\/]+)\\/(?<key>.+)$";
    Pattern pattern = Pattern.compile(uriPattern);
    Matcher matcher = pattern.matcher(s3Uri);
    if (!matcher.matches()) {
      throw new IOException("Invalid S3 URI: " + s3Uri);
    } else {
      String bucketName = matcher.group("bucket");
      String key = matcher.group("key");
      byte[] bytes;
      Region bucketRegion = getBucketRegion(bucketName);
      S3Client s3Client = S3Client.create();
      GetObjectRequest getObjectRequest =
          GetObjectRequest.builder().bucket(bucketName).key(key).build();
      InputStream s3ObjectStream = s3Client.getObject(getObjectRequest);
      try {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[1024];
        while ((nRead = s3ObjectStream.read(data, 0, data.length)) != -1) {
          buffer.write(data, 0, nRead);
        }
        buffer.flush();
        bytes = buffer.toByteArray();
      } catch (IOException e) {
        throw new IOException("Error reading S3 Object Stream", e);
      }
      return new ByteArrayInputStream(bytes);
    }
  }
}
