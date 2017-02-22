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
import org.apache.calcite.avatica.remote.Handler.HandlerResponse;
import org.apache.calcite.avatica.remote.Service.ErrorResponse;
import org.apache.calcite.avatica.remote.Service.Request;
import org.apache.calcite.avatica.remote.Service.Response;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for common functionality across {@link Handler} implementations.
 */
public class AbstractHandlerTest {

  private String exceptionToString(Exception e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    Objects.requireNonNull(e).printStackTrace(pw);
    return sw.toString();
  }

  @Test public void testExceptionUnwrappingWithoutContext() {
    @SuppressWarnings("unchecked")
    AbstractHandler<String> handler = Mockito.mock(AbstractHandler.class);

    Mockito.when(handler.unwrapException(Mockito.any(Exception.class))).thenCallRealMethod();

    Exception e = new RuntimeException();
    Response resp = handler.unwrapException(e);
    assertTrue("Response should be ErrorResponse, but was " + resp.getClass(),
        resp instanceof ErrorResponse);
    ErrorResponse errorResp = (ErrorResponse) resp;
    assertEquals(ErrorResponse.UNKNOWN_ERROR_CODE, errorResp.errorCode);
    assertEquals(AvaticaSeverity.UNKNOWN, errorResp.severity);
    assertEquals(Arrays.asList(exceptionToString(e)), errorResp.exceptions);

    e = new AvaticaRuntimeException();
    resp = handler.unwrapException(e);
    assertTrue("Response should be ErrorResponse, but was " + resp.getClass(),
        resp instanceof ErrorResponse);
    errorResp = (ErrorResponse) resp;
    assertEquals(ErrorResponse.UNKNOWN_ERROR_CODE, errorResp.errorCode);
    assertEquals(AvaticaSeverity.UNKNOWN, errorResp.severity);
    assertEquals(Arrays.asList(exceptionToString(e)), errorResp.exceptions);
  }

  @Test public void testExceptionUnwrappingWithContext() {
    @SuppressWarnings("unchecked")
    AbstractHandler<String> handler = Mockito.mock(AbstractHandler.class);

    Mockito.when(handler.unwrapException(Mockito.any(Exception.class))).thenCallRealMethod();

    final String msg = "Something failed!";
    AvaticaRuntimeException e = new AvaticaRuntimeException(msg,
        ErrorResponse.UNKNOWN_ERROR_CODE, ErrorResponse.UNKNOWN_SQL_STATE, AvaticaSeverity.FATAL);
    Response resp = handler.unwrapException(e);
    assertTrue("Response should be ErrorResponse, but was " + resp.getClass(),
        resp instanceof ErrorResponse);
    ErrorResponse errorResp = (ErrorResponse) resp;
    assertEquals(ErrorResponse.UNKNOWN_ERROR_CODE, errorResp.errorCode);
    assertEquals(AvaticaSeverity.FATAL, errorResp.severity);
    assertEquals(Arrays.asList(exceptionToString(e)), errorResp.exceptions);
    assertEquals(msg, errorResp.errorMessage);
  }

  @Test public void testFailedResponseSerialization() throws IOException {
    @SuppressWarnings("unchecked")
    final AbstractHandler<String> handler = Mockito.mock(AbstractHandler.class);
    final Request request = Mockito.mock(Request.class);
    final Response response = Mockito.mock(Response.class);
    final IOException exception = new IOException();
    final ErrorResponse errorResponse = Mockito.mock(ErrorResponse.class);
    final String serializedErrorResponse = "An ErrorResponse";

    // Accept a serialized request
    Mockito.when(handler.apply(Mockito.anyString())).thenCallRealMethod();
    // Deserialize it back into a POJO
    Mockito.when(handler.decode(Mockito.anyString())).thenReturn(request);
    // Construct the Response for that Request
    Mockito.when(request.accept(Mockito.nullable(Service.class))).thenReturn(response);
    // Throw an IOException when serializing the Response.
    Mockito.when(handler.encode(response)).thenThrow(exception);
    Mockito.when(handler.convertToErrorResponse(exception)).thenCallRealMethod();
    // Convert the IOException into an ErrorResponse
    Mockito.when(handler.unwrapException(exception)).thenReturn(errorResponse);
    Mockito.when(handler.encode(errorResponse)).thenReturn(serializedErrorResponse);

    HandlerResponse<String> handlerResp = handler.apply("this is mocked out");
    assertEquals(500, handlerResp.getStatusCode());
    assertEquals(serializedErrorResponse, handlerResp.getResponse());
  }

  @Test public void testFailedErrorResponseSerialization() throws IOException {
    @SuppressWarnings("unchecked")
    final AbstractHandler<String> handler = Mockito.mock(AbstractHandler.class);
    final Request request = Mockito.mock(Request.class);
    final Response response = Mockito.mock(Response.class);
    final IOException exception = new IOException();
    final ErrorResponse errorResponse = Mockito.mock(ErrorResponse.class);

    // Accept a serialized request
    Mockito.when(handler.apply(Mockito.anyString())).thenCallRealMethod();
    // Deserialize it back into a POJO
    Mockito.when(handler.decode(Mockito.anyString())).thenReturn(request);
    // Construct the Response for that Request
    Mockito.when(request.accept(Mockito.any(Service.class))).thenReturn(response);
    // Throw an IOException when serializing the Response.
    Mockito.when(handler.encode(response)).thenThrow(exception);
    // Convert the IOException into an ErrorResponse
    Mockito.when(handler.unwrapException(exception)).thenReturn(errorResponse);
    // Fail to serialize the ErrorResponse
    Mockito.when(handler.encode(errorResponse)).thenThrow(exception);

    try {
      handler.apply("this is mocked out");
    } catch (RuntimeException e) {
      assertEquals(exception, e.getCause());
    }
  }

  @Test public void testFailedRequestDeserialization() throws IOException {
    @SuppressWarnings("unchecked")
    final AbstractHandler<String> handler = Mockito.mock(AbstractHandler.class);
    final IOException exception = new IOException();
    final ErrorResponse errorResponse = new ErrorResponse();
    final String serializedErrorResponse = "Serialized ErrorResponse"; // Faked out

    // Accept a serialized request
    Mockito.when(handler.apply(Mockito.anyString())).thenCallRealMethod();
    // Throw an Exception trying to convert it back into a POJO
    Mockito.when(handler.decode(Mockito.anyString())).thenThrow(exception);
    Mockito.when(handler.convertToErrorResponse(exception)).thenCallRealMethod();
    Mockito.when(handler.unwrapException(exception)).thenReturn(errorResponse);
    Mockito.when(handler.encode(errorResponse)).thenReturn(serializedErrorResponse);

    HandlerResponse<String> response = handler.apply("this is mocked out");
    assertEquals(serializedErrorResponse, response.getResponse());
    assertEquals(500, response.getStatusCode());
  }
}

// End AbstractHandlerTest.java
