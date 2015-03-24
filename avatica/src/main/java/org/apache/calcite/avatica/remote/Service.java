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

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/**
 * API for request-response calls to an Avatica server.
 */
public interface Service {
  ResultSetResponse apply(CatalogsRequest request);
  ResultSetResponse apply(SchemasRequest request);
  ResultSetResponse apply(TablesRequest request);
  ResultSetResponse apply(TableTypesRequest request);
  ResultSetResponse apply(ColumnsRequest request);
  PrepareResponse apply(PrepareRequest request);
  ResultSetResponse apply(PrepareAndExecuteRequest request);
  FetchResponse apply(FetchRequest request);
  CreateStatementResponse apply(CreateStatementRequest request);
  CloseStatementResponse apply(CloseStatementRequest request);

  /** Factory that creates a {@code Service}. */
  interface Factory {
    Service create(AvaticaConnection connection);
  }

  /** Base class for all service request messages. */
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "request",
      defaultImpl = SchemasRequest.class)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = CatalogsRequest.class, name = "getCatalogs"),
      @JsonSubTypes.Type(value = SchemasRequest.class, name = "getSchemas"),
      @JsonSubTypes.Type(value = TablesRequest.class, name = "getTables"),
      @JsonSubTypes.Type(value = TableTypesRequest.class,
          name = "getTableTypes"),
      @JsonSubTypes.Type(value = ColumnsRequest.class, name = "getColumns"),
      @JsonSubTypes.Type(value = PrepareRequest.class, name = "prepare"),
      @JsonSubTypes.Type(value = PrepareAndExecuteRequest.class,
          name = "prepareAndExecute"),
      @JsonSubTypes.Type(value = FetchRequest.class, name = "fetch"),
      @JsonSubTypes.Type(value = CreateStatementRequest.class,
          name = "createStatement"),
      @JsonSubTypes.Type(value = CloseStatementRequest.class,
          name = "closeStatement") })
  abstract class Request {
    abstract Response accept(Service service);
  }

  /** Base class for all service response messages. */
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "response",
      defaultImpl = ResultSetResponse.class)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = ResultSetResponse.class, name = "resultSet"),
      @JsonSubTypes.Type(value = PrepareResponse.class, name = "prepare"),
      @JsonSubTypes.Type(value = FetchResponse.class, name = "fetch"),
      @JsonSubTypes.Type(value = CreateStatementResponse.class,
          name = "createStatement"),
      @JsonSubTypes.Type(value = CloseStatementResponse.class,
          name = "closeStatement") })
  abstract class Response {
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#getCatalogs()}. */
  class CatalogsRequest extends Request {
    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Request for
   * {@link Meta#getSchemas(String, org.apache.calcite.avatica.Meta.Pat)}. */
  class SchemasRequest extends Request {
    public final String catalog;
    public final String schemaPattern;

    @JsonCreator
    public SchemasRequest(@JsonProperty("catalog") String catalog,
        @JsonProperty("schemaPattern") String schemaPattern) {
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
    }

    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Request for
   * {@link Meta#getTables(String, org.apache.calcite.avatica.Meta.Pat, org.apache.calcite.avatica.Meta.Pat, java.util.List)}
   */
  class TablesRequest extends Request {
    public final String catalog;
    public final String schemaPattern;
    public final String tableNamePattern;
    public final List<String> typeList;

    @JsonCreator
    public TablesRequest(@JsonProperty("catalog") String catalog,
        @JsonProperty("schemaPattern") String schemaPattern,
        @JsonProperty("tableNamePattern") String tableNamePattern,
        @JsonProperty("typeList") List<String> typeList) {
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
      this.tableNamePattern = tableNamePattern;
      this.typeList = typeList;
    }

    @Override Response accept(Service service) {
      return service.apply(this);
    }
  }

  /** Request for
   * {@link Meta#getTableTypes()}. */
  class TableTypesRequest extends Request {

    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Request for
   * {@link Meta#getColumns(String, org.apache.calcite.avatica.Meta.Pat, org.apache.calcite.avatica.Meta.Pat, org.apache.calcite.avatica.Meta.Pat)}.
   */
  class ColumnsRequest extends Request {
    public final String catalog;
    public final String schemaPattern;
    public final String tableNamePattern;
    public final String columnNamePattern;

    @JsonCreator
    public ColumnsRequest(@JsonProperty("catalog") String catalog,
        @JsonProperty("schemaPattern") String schemaPattern,
        @JsonProperty("tableNamePattern") String tableNamePattern,
        @JsonProperty("columnNamePattern") String columnNamePattern) {
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
      this.tableNamePattern = tableNamePattern;
      this.columnNamePattern = columnNamePattern;
    }

    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Response that contains a result set.
   *
   * <p>Several types of request, including
   * {@link org.apache.calcite.avatica.Meta#getCatalogs()} and
   * {@link org.apache.calcite.avatica.Meta#getSchemas(String, org.apache.calcite.avatica.Meta.Pat)}
   * {@link Meta#getTables(String, Meta.Pat, Meta.Pat, List)}
   * {@link Meta#getTableTypes()}
   * return this response. */
  class ResultSetResponse extends Response {
    public final int statementId;
    public final boolean ownStatement;
    public final Meta.Signature signature;
    public final Meta.Frame firstFrame;

    @JsonCreator
    public ResultSetResponse(@JsonProperty("statementId") int statementId,
        @JsonProperty("ownStatement") boolean ownStatement,
        @JsonProperty("signature") Meta.Signature signature,
        @JsonProperty("firstFrame") Meta.Frame firstFrame) {
      this.statementId = statementId;
      this.ownStatement = ownStatement;
      this.signature = signature;
      this.firstFrame = firstFrame;
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#prepareAndExecute(org.apache.calcite.avatica.Meta.StatementHandle, String, int, org.apache.calcite.avatica.Meta.PrepareCallback)}. */
  class PrepareAndExecuteRequest extends Request {
    public final int statementId;
    public final String sql;
    public final int maxRowCount;

    @JsonCreator
    public PrepareAndExecuteRequest(
        @JsonProperty("statementId") int statementId,
        @JsonProperty("sql") String sql,
        @JsonProperty("maxRowCount") int maxRowCount) {
      this.statementId = statementId;
      this.sql = sql;
      this.maxRowCount = maxRowCount;
    }

    @Override ResultSetResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#prepare(org.apache.calcite.avatica.Meta.StatementHandle, String, int)}. */
  class PrepareRequest extends Request {
    public final int statementId;
    public final String sql;
    public final int maxRowCount;

    @JsonCreator
    public PrepareRequest(@JsonProperty("statementId") int statementId,
        @JsonProperty("sql") String sql,
        @JsonProperty("maxRowCount") int maxRowCount) {
      this.statementId = statementId;
      this.sql = sql;
      this.maxRowCount = maxRowCount;
    }

    @Override PrepareResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.PrepareRequest}. */
  class PrepareResponse extends Response {
    public final Meta.Signature signature;

    @JsonCreator
    public PrepareResponse(
        @JsonProperty("signature") Meta.Signature signature) {
      this.signature = signature;
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#fetch(Meta.StatementHandle, List, int, int)}. */
  class FetchRequest extends Request {
    public final int statementId;
    public final int offset;
    /** Maximum number of rows to be returned in the frame. Negative means no
     * limit. */
    public final int fetchMaxRowCount;
    /** A list of parameter values, if statement is to be executed; otherwise
     * null. */
    public final List<Object> parameterValues;

    @JsonCreator
    public FetchRequest(@JsonProperty("statementId") int statementId,
        @JsonProperty("parameterValues") List<Object> parameterValues,
        @JsonProperty("offset") int offset,
        @JsonProperty("fetchMaxRowCount") int fetchMaxRowCount) {
      this.statementId = statementId;
      this.parameterValues = parameterValues;
      this.offset = offset;
      this.fetchMaxRowCount = fetchMaxRowCount;
    }

    @Override FetchResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.FetchRequest}. */
  class FetchResponse extends Response {
    public final Meta.Frame frame;

    @JsonCreator
    public FetchResponse(@JsonProperty("frame") Meta.Frame frame) {
      this.frame = frame;
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#createStatement(org.apache.calcite.avatica.Meta.ConnectionHandle)}. */
  class CreateStatementRequest extends Request {
    public final int connectionId;

    @JsonCreator
    public CreateStatementRequest(@JsonProperty("signature") int connectionId) {
      this.connectionId = connectionId;
    }

    @Override CreateStatementResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.CreateStatementRequest}. */
  class CreateStatementResponse extends Response {
    public final int id;

    @JsonCreator
    public CreateStatementResponse(@JsonProperty("id") int id) {
      this.id = id;
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#closeStatement(org.apache.calcite.avatica.Meta.StatementHandle)}. */
  class CloseStatementRequest extends Request {
    public final int statementId;

    @JsonCreator
    public CloseStatementRequest(@JsonProperty("id") int statementId) {
      this.statementId = statementId;
    }

    @Override CloseStatementResponse accept(Service service) {
      return service.apply(this);
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.CloseStatementRequest}. */
  class CloseStatementResponse extends Response {
    @JsonCreator
    public CloseStatementResponse() {}
  }
}

// End Service.java
