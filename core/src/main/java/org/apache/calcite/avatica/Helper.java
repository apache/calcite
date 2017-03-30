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

import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Utility methods, mainly concerning error-handling.
 */
public class Helper {
  public static final Helper INSTANCE = new Helper();

  private Helper() {
  }

  public RuntimeException todo() {
    return new RuntimeException("todo: implement this method");
  }

  public RuntimeException wrap(String message, Exception e) {
    return new RuntimeException(message, e);
  }

  public SQLException createException(String message, Exception e) {
    return createException(message, null, e);
  }

  public SQLException createException(String message, String sql, Exception e) {
    if (e instanceof AvaticaClientRuntimeException) {
      // The AvaticaClientRuntimeException contains extra information about what/why
      // the exception was thrown that we can pass back to the user.
      AvaticaClientRuntimeException rte = (AvaticaClientRuntimeException) e;
      String serverAddress = null;
      if (null != rte.getRpcMetadata()) {
        serverAddress = rte.getRpcMetadata().serverAddress;
      }
      return new AvaticaSqlException(message, rte.getSqlState(), rte.getErrorCode(),
          rte.getServerExceptions(), serverAddress);
    }
    return new SQLException(message, e);
  }

  public SQLException createException(String message) {
    return new SQLException(message);
  }

  public SQLException toSQLException(SQLException exception) {
    return exception;
  }

  public SQLException unsupported() {
    return new SQLFeatureNotSupportedException();
  }

  public SQLClientInfoException clientInfo() {
    return new SQLClientInfoException();
  }
}

// End Helper.java
