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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.remote.Service.ErrorResponse;

import java.util.Objects;

/**
 * A {@link RuntimeException} thrown by Avatica with additional contextual information about
 * what happened to cause the Exception.
 */
public class AvaticaRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 1L;
  private final String errorMessage;
  private final int errorCode;
  private final String sqlState;
  private final AvaticaSeverity severity;

  /**
   * Constructs an {@code AvaticaRuntimeException} with no additional information.
   *
   * <p>It is strongly preferred that the caller invoke
   * {@link #AvaticaRuntimeException(String, int, String, AvaticaSeverity)}
   * with proper contextual information.
   */
  public AvaticaRuntimeException() {
    this("No additional context on exception", ErrorResponse.UNKNOWN_ERROR_CODE,
        ErrorResponse.UNKNOWN_SQL_STATE, AvaticaSeverity.UNKNOWN);
  }

  /**
   * Constructs an {@code AvaticaRuntimeException} with the given
   * contextual information surrounding the error.
   *
   * @param errorMessage A human-readable explanation about what happened
   * @param errorCode Numeric identifier for error
   * @param sqlState 5-character identifier for error
   * @param severity Severity
   */
  public AvaticaRuntimeException(String errorMessage, int errorCode, String sqlState,
      AvaticaSeverity severity) {
    this.errorMessage = Objects.requireNonNull(errorMessage);
    this.errorCode = errorCode;
    this.sqlState = Objects.requireNonNull(sqlState);
    this.severity = Objects.requireNonNull(severity);
  }

  /**
   * Returns a human-readable error message.
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Returns a numeric code for this error.
   */
  public int getErrorCode() {
    return errorCode;
  }

  /**
   * Returns the five-character identifier for this error.
   */
  public String getSqlState() {
    return sqlState;
  }

  /**
   * Returns the severity at which this exception is thrown.
   */
  public AvaticaSeverity getSeverity() {
    return severity;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder(64);
    return sb.append("AvaticaRuntimeException: [")
        .append("Messsage: '").append(errorMessage).append("', ")
        .append("Error code: '").append(errorCode).append("', ")
        .append("SQL State: '").append(sqlState).append("', ")
        .append("Severity: '").append(severity).append("']").toString();
  }
}

// End AvaticaRuntimeException.java
