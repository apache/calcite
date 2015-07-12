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

import org.apache.calcite.avatica.remote.JsonHandler;
import org.apache.calcite.avatica.remote.JsonService;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.TypedValue;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests JSON encoding/decoding in the remote service.
 */
public class JsonHandlerTest {

  /**
   * Implementation of {@link org.apache.calcite.avatica.remote.Service}
   * that does nothing.
   */
  public static class NoopService implements Service {
    @Override public ResultSetResponse apply(CatalogsRequest request) {
      return null;
    }

    @Override public ResultSetResponse apply(SchemasRequest request) {
      return null;
    }

    @Override public ResultSetResponse apply(TablesRequest request) {
      return null;
    }

    @Override public ResultSetResponse apply(TableTypesRequest request) {
      return null;
    }

    @Override public ResultSetResponse apply(TypeInfoRequest request) {
      return null;
    }

    @Override public ResultSetResponse apply(ColumnsRequest request) {
      return null;
    }

    @Override public PrepareResponse apply(PrepareRequest request) {
      return null;
    }

    @Override public ExecuteResponse apply(PrepareAndExecuteRequest request) {
      return null;
    }

    @Override public FetchResponse apply(FetchRequest request) {
      return null;
    }

    @Override public CreateStatementResponse apply(CreateStatementRequest request) {
      return null;
    }

    @Override public CloseStatementResponse apply(CloseStatementRequest request) {
      return null;
    }

    @Override public CloseConnectionResponse apply(CloseConnectionRequest request) {
      return null;
    }

    @Override public ConnectionSyncResponse apply(ConnectionSyncRequest request) {
      return null;
    }

    @Override public DatabasePropertyResponse apply(DatabasePropertyRequest request) {
      return null;
    }
  }


  /**
   * Instrumented subclass of {@link org.apache.calcite.avatica.test.JsonHandlerTest.NoopService}
   * that checks the parameter values passed to the "fetch" request.
   */
  public static class ParameterValuesCheckingService extends NoopService {

    final List<TypedValue> expectedParameterValues;

    public ParameterValuesCheckingService(List<TypedValue> epv) {
      expectedParameterValues = epv;
    }

    @Override public FetchResponse apply(FetchRequest request) {
      assertEquals(expectedParameterValues.size(), request.parameterValues.size());
      for (int i = 0; i < expectedParameterValues.size(); i++) {
        assertEquals(expectedParameterValues.get(i).type, request.parameterValues.get(i).type);
        assertEquals(expectedParameterValues.get(i).value, request.parameterValues.get(i).value);
      }
      expectedParameterValues.clear();
      return null;
    }
  }

  @Test public void testFetchRequestWithNumberParameter() {
    final List<TypedValue> expectedParameterValues = new ArrayList<>();
    final Service service = new ParameterValuesCheckingService(expectedParameterValues);
    final JsonService jsonService = new LocalJsonService(service);
    final JsonHandler jsonHandler = new JsonHandler(jsonService);

    expectedParameterValues.add(TypedValue.create("NUMBER", new BigDecimal("333.333")));
    jsonHandler.apply("{'request':'fetch','parameterValues':[{'type':'NUMBER','value':333.333}]}");
    assertTrue(expectedParameterValues.isEmpty());

    expectedParameterValues.add(TypedValue.create("NUMBER", new BigDecimal("333")));
    jsonHandler.apply("{'request':'fetch','parameterValues':[{'type':'NUMBER','value':333}]}");
    assertTrue(expectedParameterValues.isEmpty());
  }
}

// End JsonHandlerTest.java
