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
 * Encapsulate the logic of transforming a protobuf Response message into the Avatica POJO Response.
 */
public class ResponseTranslator {

  private final Parser<? extends Message> parser;
  private final Service.Response impl;

  public ResponseTranslator(Parser<? extends Message> parser, Service.Response impl) {
    this.parser = parser;
    this.impl = impl;
  }

  public Service.Response transform(ByteString serializedMessage) throws
      InvalidProtocolBufferException {
    Message msg = parser.parseFrom(serializedMessage);
    return impl.deserialize(msg);
  }
}

// End ResponseTranslator.java
