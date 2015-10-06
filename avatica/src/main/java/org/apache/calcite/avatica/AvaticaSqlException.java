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

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * A client-facing {@link SQLException} which encapsulates errors from the remote Avatica server.
 */
public class AvaticaSqlException extends SQLException {

  private static final long serialVersionUID = 1L;

  private final String errorMessage;
  private final List<String> stackTraces;

  /**
   * Construct the Exception with information from the server.
   *
   * @param errorMessage A human-readable error message.
   * @param errorCode An integer corresponding to a known error.
   * @param stackTraces Server-side stacktrace.
   */
  public AvaticaSqlException(String errorMessage, String sqlState, int errorCode,
      List<String> stackTraces) {
    super("Error " + errorCode + " (" + sqlState + ") : " + errorMessage, sqlState, errorCode);
    this.errorMessage = errorMessage;
    this.stackTraces = Objects.requireNonNull(stackTraces);
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * @return The stacktraces for exceptions thrown on the Avatica server.
   */
  public List<String> getStackTraces() {
    return stackTraces;
  }
}

// End AvaticaSqlException.java
