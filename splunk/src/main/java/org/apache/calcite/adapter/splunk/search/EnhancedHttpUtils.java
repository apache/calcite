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
package org.apache.calcite.adapter.splunk.util;

import org.apache.calcite.adapter.splunk.search.SplunkAuthenticationException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/**
 * Enhanced HTTP utilities with proper authentication error detection and handling.
 * Extends the base HttpUtils functionality with Splunk-specific error handling.
 */
public final class EnhancedHttpUtils {

  private EnhancedHttpUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Posts data to a URL with authentication error detection.
   *
   * @param url the target URL
   * @param data the POST data
   * @param headers the request headers
   * @param connectTimeout connection timeout in milliseconds
   * @param readTimeout read timeout in milliseconds
   * @return InputStream for reading the response
   * @throws SplunkAuthenticationException if authentication fails (HTTP 401)
   * @throws IOException for other network errors
   */
  public static InputStream postWithAuthDetection(String url, StringBuilder data,
      Map<String, String> headers, int connectTimeout, int readTimeout)
      throws IOException, SplunkAuthenticationException {

    HttpURLConnection connection = null;

    try {
      URI uri = URI.create(url);
      URL urlObj = uri.toURL();
      connection = (HttpURLConnection) urlObj.openConnection();

      // Configure connection
      connection.setRequestMethod("POST");
      connection.setDoOutput(true);
      connection.setConnectTimeout(connectTimeout);
      connection.setReadTimeout(readTimeout);

      // Set headers
      connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      if (headers != null) {
        for (Map.Entry<String, String> header : headers.entrySet()) {
          connection.setRequestProperty(header.getKey(), header.getValue());
        }
      }

      // Send POST data
      if (data != null && data.length() > 0) {
        try (OutputStream os = connection.getOutputStream()) {
          byte[] input = data.toString().getBytes(StandardCharsets.UTF_8);
          os.write(input, 0, input.length);
        }
      }

      // Check response code
      int responseCode = connection.getResponseCode();

      if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
        // Read error response for more details
        String errorMessage = readErrorResponse(connection);
        throw new SplunkAuthenticationException(
            "Authentication failed (HTTP 401): " + errorMessage,
            responseCode,
            true); // Assume it's session expiry if we got this far
      }

      if (responseCode < 200 || responseCode >= 300) {
        String errorMessage = readErrorResponse(connection);
        throw new IOException("HTTP " + responseCode + ": " + errorMessage);
      }

      // Return input stream for successful response
      return connection.getInputStream();

    } catch (SplunkAuthenticationException e) {
      // Re-throw authentication exceptions as-is
      if (connection != null) {
        connection.disconnect();
      }
      throw e;
    } catch (IOException e) {
      // Check if the IOException might be hiding an authentication error
      if (connection != null) {
        try {
          int responseCode = connection.getResponseCode();
          if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
            String errorMessage = readErrorResponse(connection);
            throw new SplunkAuthenticationException(
                "Authentication failed (HTTP 401): " + errorMessage,
                responseCode,
                true,
                e);
          }
        } catch (IOException ignored) {
          // If we can't read the response code, just throw the original exception
        } finally {
          connection.disconnect();
        }
      }
      throw e;
    }
    // Note: Don't disconnect the connection here as the InputStream needs it
  }

  /**
   * Reads the error response from a failed HTTP connection.
   *
   * @param connection the HTTP connection
   * @return the error message, or a default message if unable to read
   */
  private static String readErrorResponse(HttpURLConnection connection) {
    try {
      InputStream errorStream = connection.getErrorStream();
      if (errorStream != null) {
        byte[] errorBytes = errorStream.readAllBytes();
        if (errorBytes != null) {
          return StandardCharsets.UTF_8.decode(java.nio.ByteBuffer.wrap(errorBytes)).toString();
        }
        return "";
      }
    } catch (IOException e) {
      // Ignore errors reading the error response
    }

    try {
      return connection.getResponseMessage();
    } catch (IOException e) {
      return "Unknown error";
    }
  }

  /**
   * Wrapper for the existing HttpUtils.post method that adds authentication detection.
   * This allows existing code to benefit from enhanced error handling with minimal changes.
   */
  public static InputStream post(String url, StringBuilder data, Map<String, String> headers,
      int connectTimeout, int readTimeout) throws IOException, SplunkAuthenticationException {

    try {
      // Use the existing HttpUtils.post method
      return org.apache.calcite.runtime.HttpUtils.post(url, data, headers, connectTimeout,
          readTimeout);
    } catch (RuntimeException e) {
      // Check if the RuntimeException is hiding an authentication error
      if (isAuthenticationError(e)) {
        throw new SplunkAuthenticationException(
            "Authentication failed: " + e.getMessage(),
            401,
            true,
            e);
      }
      throw e;
    } catch (IOException e) {
      // Check if the IOException indicates an authentication error
      if (isAuthenticationError(e)) {
        throw new SplunkAuthenticationException(
            "Authentication failed: " + e.getMessage(),
            401,
            true,
            e);
      }
      throw e;
    }
  }

  /**
   * Checks if an exception indicates an authentication error.
   * This method examines the exception message and any nested causes
   * to determine if the error is related to authentication.
   */
  private static boolean isAuthenticationError(Throwable e) {
    if (e == null) {
      return false;
    }

    // Check the exception message
    String message = e.getMessage();
    if (message != null) {
      String lowerMessage = message.toLowerCase(Locale.ROOT);
      if (lowerMessage.contains("401")
          || lowerMessage.contains("unauthorized")
          || lowerMessage.contains("authentication")
          || lowerMessage.contains("login required")
          || lowerMessage.contains("session expired")) {
        return true;
      }
    }

    // Check nested causes
    Throwable cause = e.getCause();
    if (cause != null && cause != e) {
      return isAuthenticationError(cause);
    }

    return false;
  }
}
