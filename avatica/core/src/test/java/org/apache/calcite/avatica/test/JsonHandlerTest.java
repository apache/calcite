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

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.remote.JsonHandler;
import org.apache.calcite.avatica.remote.JsonService;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.TypedValue;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests JSON encoding/decoding in the remote service.
 */
public class JsonHandlerTest {

  private static final Random RANDOM = new Random();

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

    @Override public OpenConnectionResponse apply(OpenConnectionRequest request) {
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

    @Override public SyncResultsResponse apply(SyncResultsRequest request) {
      return null;
    }

    @Override public ExecuteResponse apply(ExecuteRequest request) {
      return null;
    }

    @Override public void setRpcMetadata(RpcMetadataResponse metadata) {}

    @Override public CommitResponse apply(CommitRequest request) {
      return null;
    }

    @Override public RollbackResponse apply(RollbackRequest request) {
      return null;
    }

    @Override public ExecuteBatchResponse apply(ExecuteBatchRequest request) {
      return null;
    }

    @Override public ExecuteBatchResponse apply(PrepareAndExecuteBatchRequest request) {
      return null;
    }
  }

  /**
   * Instrumented subclass of {@link org.apache.calcite.avatica.test.JsonHandlerTest.NoopService}
   * that checks the parameter values passed to the "execute" request.
   *
   * <p>Note: parameter values for "fetch" request deprecated.
   */
  public static class ParameterValuesCheckingService extends NoopService {

    final List<TypedValue> expectedParameterValues;

    public ParameterValuesCheckingService(List<TypedValue> epv) {
      expectedParameterValues = epv;
    }

    @Override public ExecuteResponse apply(ExecuteRequest request) {
      expectedParameterValues.addAll(request.parameterValues);

      final Meta.Signature signature =
          new Meta.Signature(Collections.<ColumnMetaData>emptyList(),
              "SELECT 1 FROM VALUE()",
              Collections.<AvaticaParameter>emptyList(),
              Collections.<String, Object>emptyMap(),
              CursorFactory.LIST, Meta.StatementType.SELECT);

      final Service.ResultSetResponse resultSetResponse =
          new Service.ResultSetResponse(UUID.randomUUID().toString(),
              RANDOM.nextInt(), false, signature, Meta.Frame.EMPTY, -1L, null);

      return new Service.ExecuteResponse(
          Collections.singletonList(resultSetResponse), false, null);
    }
  }

  @Test public void testExecuteRequestWithNumberParameter() {
    final List<TypedValue> expectedParameterValues = new ArrayList<>();
    final Service service = new ParameterValuesCheckingService(expectedParameterValues);
    final JsonService jsonService = new LocalJsonService(service);
    final JsonHandler jsonHandler = new JsonHandler(jsonService, NoopMetricsSystem.getInstance());

    final List<TypedValue> parameterValues = Arrays.asList(
        TypedValue.create("NUMBER", new BigDecimal("123")),
        TypedValue.create("STRING", "calcite"));

    jsonHandler.apply(
        "{'request':'execute',"
        + "'parameterValues':[{'type':'NUMBER','value':123},"
        + "{'type':'STRING','value':'calcite'}]}");
    assertThat(expectedParameterValues.size(), is(2));
    assertThat(expectedParameterValues.get(0), is(parameterValues.get(0)));
    assertThat(expectedParameterValues.get(1), is(parameterValues.get(1)));
  }
}

// End JsonHandlerTest.java
