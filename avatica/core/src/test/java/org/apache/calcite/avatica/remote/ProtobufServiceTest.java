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

import org.apache.calcite.avatica.proto.Requests;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test class for ProtobufService.
 */
public class ProtobufServiceTest {

  @Test public void testCastProtobufMessage() {
    final Requests.CommitRequest commitReq =
        Requests.CommitRequest.newBuilder().setConnectionId("cnxn1").build();
    final Requests.RollbackRequest rollbackReq =
        Requests.RollbackRequest.newBuilder().setConnectionId("cnxn1").build();

    assertEquals(commitReq,
        ProtobufService.castProtobufMessage(commitReq, Requests.CommitRequest.class));
    assertEquals(rollbackReq,
        ProtobufService.castProtobufMessage(rollbackReq, Requests.RollbackRequest.class));

    try {
      ProtobufService.castProtobufMessage(commitReq, Requests.RollbackRequest.class);
      fail("Should have seen IllegalArgumentException casting CommitRequest into RollbackRequest");
    } catch (IllegalArgumentException e) {
      // Expected
    }

    try {
      ProtobufService.castProtobufMessage(rollbackReq, Requests.CommitRequest.class);
      fail("Should have seen IllegalArgumentException casting RollbackRequest into CommitRequest");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}

// End ProtobufServiceTest.java
