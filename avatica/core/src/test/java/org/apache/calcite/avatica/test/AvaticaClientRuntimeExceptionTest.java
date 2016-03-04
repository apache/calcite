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
package org.apache.calcite.avatica.test;

import org.apache.calcite.avatica.AvaticaClientRuntimeException;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link AvaticaClientRuntimeException}.
 */
public class AvaticaClientRuntimeExceptionTest {

  @Test public void testGetters() {
    final String errorMsg = "My error message";
    final int errorCode = 10;
    final String sqlState = "abc12";
    final AvaticaSeverity severity = AvaticaSeverity.ERROR;
    final List<String> stacktraces = Arrays.asList("my stack trace");
    final RpcMetadataResponse metadata = new RpcMetadataResponse("localhost:8765");
    AvaticaClientRuntimeException e = new AvaticaClientRuntimeException(errorMsg, errorCode,
        sqlState, severity, stacktraces, metadata);
    assertEquals(errorMsg, e.getMessage());
    assertEquals(errorCode, e.getErrorCode());
    assertEquals(severity, e.getSeverity());
    assertEquals(stacktraces, e.getServerExceptions());
    assertEquals(metadata, e.getRpcMetadata());
  }

}

// End AvaticaClientRuntimeExceptionTest.java
