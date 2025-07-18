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
package org.apache.calcite.adapter.splunk.search;

/**
 * Exception thrown when Splunk authentication fails or expires.
 * This allows for more precise error handling and retry logic.
 */
public class SplunkAuthenticationException extends RuntimeException {

  private final int httpStatusCode;
  private final boolean isSessionExpiry;

  /**
   * Creates an authentication exception.
   *
   * @param message the error message
   */
  public SplunkAuthenticationException(String message) {
    this(message, 401, false);
  }

  /**
   * Creates an authentication exception with HTTP status code.
   *
   * @param message the error message
   * @param httpStatusCode the HTTP status code (typically 401)
   */
  public SplunkAuthenticationException(String message, int httpStatusCode) {
    this(message, httpStatusCode, false);
  }

  /**
   * Creates an authentication exception with session expiry information.
   *
   * @param message the error message
   * @param httpStatusCode the HTTP status code (typically 401)
   * @param isSessionExpiry true if this is a session expiry vs initial auth failure
   */
  public SplunkAuthenticationException(String message, int httpStatusCode, boolean isSessionExpiry) {
    super(message);
    this.httpStatusCode = httpStatusCode;
    this.isSessionExpiry = isSessionExpiry;
  }

  /**
   * Creates an authentication exception with a cause.
   *
   * @param message the error message
   * @param cause the underlying cause
   */
  public SplunkAuthenticationException(String message, Throwable cause) {
    this(message, 401, false, cause);
  }

  /**
   * Creates an authentication exception with full details.
   *
   * @param message the error message
   * @param httpStatusCode the HTTP status code
   * @param isSessionExpiry true if this is a session expiry
   * @param cause the underlying cause
   */
  public SplunkAuthenticationException(String message, int httpStatusCode, boolean isSessionExpiry, Throwable cause) {
    super(message, cause);
    this.httpStatusCode = httpStatusCode;
    this.isSessionExpiry = isSessionExpiry;
  }

  /**
   * Returns the HTTP status code associated with this authentication error.
   *
   * @return the HTTP status code (typically 401)
   */
  public int getHttpStatusCode() {
    return httpStatusCode;
  }

  /**
   * Returns whether this exception represents a session expiry vs an initial authentication failure.
   *
   * @return true if this is a session expiry, false if initial authentication failure
   */
  public boolean isSessionExpiry() {
    return isSessionExpiry;
  }

  /**
   * Returns whether this exception indicates the request should be retried after re-authentication.
   *
   * @return true if retry is recommended
   */
  public boolean shouldRetry() {
    return httpStatusCode == 401 || isSessionExpiry;
  }
}
