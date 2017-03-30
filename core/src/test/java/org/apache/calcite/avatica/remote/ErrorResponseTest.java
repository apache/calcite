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

import org.apache.calcite.avatica.AvaticaClientRuntimeException;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.remote.Service.ErrorResponse;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * A test class for ErrorResponse.
 */
public class ErrorResponseTest {

  @Test public void testEquality() {
    final String message = "There was an error";
    final int code = 23;
    final String state = "a1b2c";
    final AvaticaSeverity severity = AvaticaSeverity.ERROR;
    final List<String> exceptions = Arrays.asList("Server Stacktrace 1", "Server Stacktace 2");
    final RpcMetadataResponse metadata = new RpcMetadataResponse("localhost:8765");
    assertEquals(new ErrorResponse(message, code, state, severity, exceptions, metadata),
        new ErrorResponse(message, code, state, severity, exceptions, metadata));
  }

  @Test public void testToClientRTE() {
    final String message = "There was an error";
    final int code = 23;
    final String state = "a1b2c";
    final AvaticaSeverity severity = AvaticaSeverity.ERROR;
    final List<String> exceptions = Arrays.asList("Server Stacktrace 1", "Server Stacktace 2");
    final RpcMetadataResponse metadata = new RpcMetadataResponse("localhost:8765");
    final ErrorResponse resp = new ErrorResponse(message, code, state, severity, exceptions,
        metadata);
    AvaticaClientRuntimeException exception = resp.toException();
    assertTrue("Expected error message to end with '" + resp.errorMessage + "', but was '"
        + exception.getMessage() + "'", exception.getMessage().endsWith(resp.errorMessage));
    assertEquals(resp.errorCode, exception.getErrorCode());
    assertEquals(resp.severity, exception.getSeverity());
    assertEquals(resp.sqlState, exception.getSqlState());
    assertEquals(resp.exceptions, exception.getServerExceptions());
    assertEquals(resp.rpcMetadata, exception.getRpcMetadata());
  }
}

// End ErrorResponseTest.java
