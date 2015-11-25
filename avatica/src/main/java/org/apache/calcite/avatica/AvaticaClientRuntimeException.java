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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.calcite.avatica.remote.Service.ErrorResponse;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;

import java.util.Collections;
import java.util.List;

/**
 * The client-side representation of {@link AvaticaRuntimeException}. This exception is not intended
 * for consumption by clients, {@link AvaticaSqlException} serves that purpose. This exception only
 * exists to pass the original error attributes to a higher level of execution without modifying
 * existing exception-handling logic.
 */
public class AvaticaClientRuntimeException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final int errorCode;
  private final String sqlState;
  private final AvaticaSeverity severity;
  private final List<String> serverExceptions;
  private final RpcMetadataResponse metadata;

  public AvaticaClientRuntimeException(String errorMessage, int errorCode, String sqlState,
      AvaticaSeverity severity, List<String> serverExceptions, RpcMetadataResponse metadata) {
    super(errorMessage);
    this.errorCode = errorCode;
    this.sqlState = sqlState;
    this.severity = severity;
    this.serverExceptions = serverExceptions;
    this.metadata = metadata;
  }

  public AvaticaClientRuntimeException(String message, Throwable cause) {
    super(message, cause);
    errorCode = ErrorResponse.UNKNOWN_ERROR_CODE;
    sqlState = ErrorResponse.UNKNOWN_SQL_STATE;
    severity = AvaticaSeverity.UNKNOWN;
    serverExceptions = Collections.singletonList("");
    metadata = null;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getSqlState() {
    return sqlState;
  }

  public AvaticaSeverity getSeverity() {
    return severity;
  }

  public List<String> getServerExceptions() {
    return serverExceptions;
  }

  public RpcMetadataResponse getRpcMetadata() {
    return metadata;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder(64);
    sb.append(getClass().getSimpleName()).append(": ")
    .append(getMessage()).append(". Error ").append(getErrorCode())
    .append(" (").append(sqlState).append(") ").append(getSeverity()).append("\n\n");
    for (String serverException : getServerExceptions()) {
      sb.append(serverException).append("\n");
    }
    return sb.toString();
  }

}

// End AvaticaClientRuntimeException.java
