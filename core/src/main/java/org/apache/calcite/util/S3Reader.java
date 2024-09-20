package org.apache.calcite.util;


import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.core.ResponseInputStream;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.io.ByteArrayInputStream;
import java.util.function.Function;
import java.lang.RuntimeException;

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
        bytes = s3ObjectStream.readAllBytes();
      } catch (IOException e) {
        throw new IOException("Error reading S3 Object Stream", e);
      }
      return new ByteArrayInputStream(bytes);
    }
  }
}
