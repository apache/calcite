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

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.remote.Service.ExecuteRequest;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Test class for ExecuteRequest
 */
public class ExecuteRequestTest {

  private static final Meta.StatementHandle STATEMENT_HANDLE =
      new Meta.StatementHandle("cnxn1", 1, null);

  @Test public void testUsesDeprecatedFieldIfPresent() {
    Requests.ExecuteRequest proto = Requests.ExecuteRequest.newBuilder()
        .setDeprecatedFirstFrameMaxSize(1).setStatementHandle(STATEMENT_HANDLE.toProto()).build();

    ExecuteRequest request = new ExecuteRequest();
    request = request.deserialize(proto);

    assertEquals(proto.getDeprecatedFirstFrameMaxSize(), request.maxRowCount);
  }

  @Test public void testNewFieldOverridesOldField() {
    Requests.ExecuteRequest proto = Requests.ExecuteRequest.newBuilder()
        .setDeprecatedFirstFrameMaxSize(1).setFirstFrameMaxSize(2)
        .setStatementHandle(STATEMENT_HANDLE.toProto()).build();

    ExecuteRequest request = new ExecuteRequest();
    request = request.deserialize(proto);

    assertEquals(proto.getFirstFrameMaxSize(), request.maxRowCount);
  }

  @Test public void testNewFieldIsUsed() {
    Requests.ExecuteRequest proto = Requests.ExecuteRequest.newBuilder()
        .setFirstFrameMaxSize(2).setStatementHandle(STATEMENT_HANDLE.toProto()).build();

    ExecuteRequest request = new ExecuteRequest();
    request = request.deserialize(proto);

    assertEquals(proto.getFirstFrameMaxSize(), request.maxRowCount);
  }

  @Test public void testBothFieldsAreSerialized() {
    ExecuteRequest request = new ExecuteRequest(STATEMENT_HANDLE,
        Collections.<TypedValue>emptyList(), 5);
    Requests.ExecuteRequest proto = request.serialize();

    assertEquals(request.maxRowCount, proto.getDeprecatedFirstFrameMaxSize());
    assertEquals(request.maxRowCount, proto.getFirstFrameMaxSize());
  }
}

// End ExecuteRequestTest.java
