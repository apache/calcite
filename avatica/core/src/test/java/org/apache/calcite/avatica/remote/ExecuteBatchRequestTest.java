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
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.avatica.remote.Service.ExecuteBatchRequest;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for ExecuteBatchRequest
 */
public class ExecuteBatchRequestTest {

  private ExecuteBatchRequest identityRequest = new ExecuteBatchRequest();
  private List<TypedValue> paramValues =
      Arrays.asList(TypedValue.create(Rep.BOOLEAN.name(), Boolean.TRUE),
          TypedValue.create(Rep.STRING.name(), "string"));

  @Test public void testConversionFromProtobuf() {
    ExecuteBatchRequest request = new ExecuteBatchRequest("connectionId", 12345,
        Arrays.asList(paramValues, paramValues, paramValues));

    assertFalse("A request with the POJO TypedValue list should return false",
        request.hasProtoUpdateBatches());

    // Everything will be serialized via protobuf
    Requests.ExecuteBatchRequest protoRequest = request.serialize();

    ExecuteBatchRequest copy = identityRequest.deserialize(protoRequest);

    assertNull("Parameter values (pojo) list should be null", copy.parameterValues);
    assertTrue("hasProtoUpdateBatches() should return true", copy.hasProtoUpdateBatches());
    List<UpdateBatch> protoParameterValues = copy.getProtoUpdateBatches();
    assertNotNull("Protobuf serialized parameterValues should not be null", protoParameterValues);

    assertEquals(request.parameterValues.size(), protoParameterValues.size());

    for (int i = 0; i < request.parameterValues.size(); i++) {
      List<TypedValue> orig = request.parameterValues.get(i);
      List<Common.TypedValue> proto = protoParameterValues.get(i).getParameterValuesList();
      assertEquals("Mismatch in length of TypedValues at index " + i, orig.size(), proto.size());

      // Don't re-test TypedValue serialization
    }

    // Everything else should be equivalent.
    assertEquals(request.connectionId, copy.connectionId);
    assertEquals(request.statementId, copy.statementId);
  }
}

// End ExecuteBatchRequestTest.java
