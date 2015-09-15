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

import org.apache.calcite.avatica.remote.Service.Request;
import org.apache.calcite.avatica.remote.Service.Response;

import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;

/**
 * Generic interface to support parsing of serialized protocol buffers between client and server.
 */
public interface ProtobufTranslation {

  /**
   * Serialize a Response as a protocol buffer.
   *
   * @param msg The response to serialize.
   * @throws IOException If there are errors during serialization.
   */
  byte[] serializeResponse(Response response) throws IOException;

  /**
   * Serialize a Request as a protocol buffer.
   *
   * @param request The request to serialize.
   * @throws IOException If there are errors during serialization
   */
  byte[] serializeRequest(Request request) throws IOException;

  /**
   * Parse a serialized protocol buffer request into a Request.
   * @param bytes Serialized protocol buffer request from client
   * @return A Request object for the given bytes.
   * @throws InvalidProtocolBufferException If the protocol buffer cannot be deserialized.
   */
  Request parseRequest(byte[] bytes) throws InvalidProtocolBufferException;

  /**
   * Parse a serialize protocol buffer response into a {@link Response}.
   * @param bytes Serialized protocol buffer request from server
   * @return The Response object for the given bytes
   * @throws InvalidProtocolBufferException If the protocol buffer cannot be deserialized.
   */
  Response parseResponse(byte[] bytes) throws InvalidProtocolBufferException;
}

// End ProtobufTranslation.java
