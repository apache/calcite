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

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.proto.Common.WireMessage;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.remote.Service.Request;

import com.google.protobuf.HBaseZeroCopyByteString;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Protobuf serialization tests.
 */
public class ProtobufSerializationTest {

  private Signature getSignature() {
    return null;
  }

  private List<TypedValue> getTypedValues() {
    List<TypedValue> paramValues =
        Arrays.asList(TypedValue.create(Rep.BOOLEAN.name(), Boolean.TRUE),
            TypedValue.create(Rep.STRING.name(), "string"));
    return paramValues;
  }

  @Test public void testExecuteSerialization() throws Exception {
    Service.ExecuteRequest executeRequest = new Service.ExecuteRequest(
        new StatementHandle("connection", 12345, getSignature()), getTypedValues(), 0);

    Requests.ExecuteRequest pbExecuteRequest = executeRequest.serialize();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    pbExecuteRequest.writeTo(baos);

    byte[] serialized = baos.toByteArray();
    baos.reset();
    WireMessage wireMsg = WireMessage.newBuilder().setName(Requests.ExecuteRequest.class.getName())
        .setWrappedMessage(HBaseZeroCopyByteString.wrap(serialized)).build();
    wireMsg.writeTo(baos);
    serialized = baos.toByteArray();

    ProtobufTranslation translator = new ProtobufTranslationImpl();

    Request newRequest = translator.parseRequest(serialized);

    Assert.assertEquals(executeRequest, newRequest);
  }

}

// End ProtobufSerializationTest.java
