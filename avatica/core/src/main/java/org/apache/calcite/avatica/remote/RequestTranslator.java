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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

/**
 * Encapsulate the logic of transforming a protobuf Request message into the Avatica POJO request.
 */
public class RequestTranslator {

  private final Parser<? extends Message> parser;
  private final Service.Request impl;

  public RequestTranslator(Parser<? extends Message> parser, Service.Request impl) {
    this.parser = parser;
    this.impl = impl;
  }

  public Service.Request transform(ByteString serializedMessage) throws
      InvalidProtocolBufferException {
    // This should already be an aliased CodedInputStream from the WireMessage parsing.
    Message msg = parser.parseFrom(serializedMessage.newCodedInput());
    return impl.deserialize(msg);
  }
}

// End RequestTranslator.java
