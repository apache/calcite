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

import org.apache.calcite.avatica.remote.Service.Response;

import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;

/**
 * Dispatches serialized protocol buffer messages to the provided {@link Service}
 * by converting them to the POJO Request. Returns back the serialized protocol
 * buffer response.
 */
public class ProtobufHandler implements Handler<byte[]> {

  private final Service service;
  private final ProtobufTranslation translation;

  public ProtobufHandler(Service service, ProtobufTranslation translation) {
    this.service = service;
    this.translation = translation;
  }

  @Override public byte[] apply(byte[] requestBytes) {
    // Transform the protocol buffer bytes into a POJO
    // Encapsulate the task of transforming this since
    // the bytes also contain the PB request class name.
    Service.Request requestPojo;
    try {
      requestPojo = translation.parseRequest(requestBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    // Get the response for the request
    Response response = requestPojo.accept(service);

    try {
      // Serialize it into bytes for the wire.
      return translation.serializeResponse(response);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

// End ProtobufHandler.java
