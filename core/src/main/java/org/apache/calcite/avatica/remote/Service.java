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

import org.apache.calcite.avatica.AvaticaClientRuntimeException;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.proto.Responses;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * API for request-response calls to an Avatica server.
 */
public interface Service {
  ResultSetResponse apply(CatalogsRequest request);
  ResultSetResponse apply(SchemasRequest request);
  ResultSetResponse apply(TablesRequest request);
  ResultSetResponse apply(TableTypesRequest request);
  ResultSetResponse apply(TypeInfoRequest request);
  ResultSetResponse apply(ColumnsRequest request);
  PrepareResponse apply(PrepareRequest request);
  ExecuteResponse apply(ExecuteRequest request);
  ExecuteResponse apply(PrepareAndExecuteRequest request);
  SyncResultsResponse apply(SyncResultsRequest request);
  FetchResponse apply(FetchRequest request);
  CreateStatementResponse apply(CreateStatementRequest request);
  CloseStatementResponse apply(CloseStatementRequest request);
  OpenConnectionResponse apply(OpenConnectionRequest request);
  CloseConnectionResponse apply(CloseConnectionRequest request);
  ConnectionSyncResponse apply(ConnectionSyncRequest request);
  DatabasePropertyResponse apply(DatabasePropertyRequest request);
  CommitResponse apply(CommitRequest request);
  RollbackResponse apply(RollbackRequest request);
  ExecuteBatchResponse apply(PrepareAndExecuteBatchRequest request);
  ExecuteBatchResponse apply(ExecuteBatchRequest request);

  /**
   * Sets server-level metadata for RPCs. This includes information that is static across all RPCs.
   *
   * @param metadata The server-level metadata.
   */
  void setRpcMetadata(RpcMetadataResponse metadata);

  /** Factory that creates a {@code Service}. */
  interface Factory {
    Service create(AvaticaConnection connection);
  }

  /** Base class for request and response. */
  abstract class Base {
    static final int PRIME = 31;

    protected static int p(int result, Object o) {
      return PRIME * result + ((o == null) ? 0 : o.hashCode());
    }

    protected static int p(int result, boolean v) {
      return PRIME * result + (v ? 1231 : 1237);
    }

    protected static int p(int result, int v) {
      return PRIME * result + v;
    }

    protected static int p(int result, long v) {
      return PRIME * result + (int) (v ^ (v >>> 32));
    }
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
      @JsonSubTypes.Type(value = TableTypesRequest.class, name = "getTableTypes"),
      @JsonSubTypes.Type(value = TypeInfoRequest.class, name = "getTypeInfo"),
      @JsonSubTypes.Type(value = ColumnsRequest.class, name = "getColumns"),
      @JsonSubTypes.Type(value = ExecuteRequest.class, name = "execute"),
      @JsonSubTypes.Type(value = PrepareRequest.class, name = "prepare"),
      @JsonSubTypes.Type(value = PrepareAndExecuteRequest.class,
          name = "prepareAndExecute"),
      @JsonSubTypes.Type(value = FetchRequest.class, name = "fetch"),
      @JsonSubTypes.Type(value = CreateStatementRequest.class,
          name = "createStatement"),
      @JsonSubTypes.Type(value = CloseStatementRequest.class,
          name = "closeStatement"),
      @JsonSubTypes.Type(value = OpenConnectionRequest.class,
          name = "openConnection"),
      @JsonSubTypes.Type(value = CloseConnectionRequest.class,
          name = "closeConnection"),
      @JsonSubTypes.Type(value = ConnectionSyncRequest.class, name = "connectionSync"),
      @JsonSubTypes.Type(value = DatabasePropertyRequest.class, name = "databaseProperties"),
      @JsonSubTypes.Type(value = SyncResultsRequest.class, name = "syncResults"),
      @JsonSubTypes.Type(value = CommitRequest.class, name = "commit"),
      @JsonSubTypes.Type(value = RollbackRequest.class, name = "rollback"),
      @JsonSubTypes.Type(value = PrepareAndExecuteBatchRequest.class,
          name = "prepareAndExecuteBatch"),
      @JsonSubTypes.Type(value = ExecuteBatchRequest.class, name = "executeBatch") })
  abstract class Request extends Base {
    abstract Response accept(Service service);
    abstract Request deserialize(Message genericMsg);
    abstract Message serialize();
  }

  /** Base class for all service response messages. */
  @JsonTypeInfo(
      use = JsonTypeInfo.Id.NAME,
      property = "response",
      defaultImpl = ResultSetResponse.class)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = OpenConnectionResponse.class, name = "openConnection"),
      @JsonSubTypes.Type(value = ResultSetResponse.class, name = "resultSet"),
      @JsonSubTypes.Type(value = PrepareResponse.class, name = "prepare"),
      @JsonSubTypes.Type(value = FetchResponse.class, name = "fetch"),
      @JsonSubTypes.Type(value = CreateStatementResponse.class,
          name = "createStatement"),
      @JsonSubTypes.Type(value = CloseStatementResponse.class,
          name = "closeStatement"),
      @JsonSubTypes.Type(value = CloseConnectionResponse.class,
          name = "closeConnection"),
      @JsonSubTypes.Type(value = ConnectionSyncResponse.class, name = "connectionSync"),
      @JsonSubTypes.Type(value = DatabasePropertyResponse.class, name = "databaseProperties"),
      @JsonSubTypes.Type(value = ExecuteResponse.class, name = "executeResults"),
      @JsonSubTypes.Type(value = ErrorResponse.class, name = "error"),
      @JsonSubTypes.Type(value = SyncResultsResponse.class, name = "syncResults"),
      @JsonSubTypes.Type(value = RpcMetadataResponse.class, name = "rpcMetadata"),
      @JsonSubTypes.Type(value = CommitResponse.class, name = "commit"),
      @JsonSubTypes.Type(value = RollbackResponse.class, name = "rollback"),
      @JsonSubTypes.Type(value = ExecuteBatchResponse.class, name = "executeBatch") })
  abstract class Response extends Base {
    abstract Response deserialize(Message genericMsg);
    abstract Message serialize();
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#getCatalogs(Meta.ConnectionHandle)}. */
  class CatalogsRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.CatalogsRequest.
        getDescriptor().findFieldByNumber(Requests.CatalogsRequest.CONNECTION_ID_FIELD_NUMBER);
    public final String connectionId;

    public CatalogsRequest() {
      connectionId = null;
    }

    @JsonCreator
    public CatalogsRequest(@JsonProperty("connectionId") String connectionId) {
      this.connectionId = connectionId;
    }

    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }

    @Override CatalogsRequest deserialize(Message genericMsg) {
      final Requests.CatalogsRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.CatalogsRequest.class);
      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      return new CatalogsRequest(connectionId);
    }

    @Override Requests.CatalogsRequest serialize() {
      Requests.CatalogsRequest.Builder builder = Requests.CatalogsRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof CatalogsRequest
          && Objects.equals(connectionId, ((CatalogsRequest) o).connectionId);
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#getDatabaseProperties(Meta.ConnectionHandle)}. */
  class DatabasePropertyRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR =
        Requests.DatabasePropertyRequest.getDescriptor().
        findFieldByNumber(Requests.DatabasePropertyRequest.CONNECTION_ID_FIELD_NUMBER);

    public final String connectionId;

    public DatabasePropertyRequest() {
      connectionId = null;
    }

    @JsonCreator
    public DatabasePropertyRequest(@JsonProperty("connectionId") String connectionId) {
      this.connectionId = connectionId;
    }

    DatabasePropertyResponse accept(Service service) {
      return service.apply(this);
    }

    @Override DatabasePropertyRequest deserialize(Message genericMsg) {
      final Requests.DatabasePropertyRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.DatabasePropertyRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      return new DatabasePropertyRequest(connectionId);
    }

    @Override Requests.DatabasePropertyRequest serialize() {
      Requests.DatabasePropertyRequest.Builder builder =
          Requests.DatabasePropertyRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof DatabasePropertyRequest
          && Objects.equals(connectionId, ((DatabasePropertyRequest) o).connectionId);
    }
  }

  /** Request for
   * {@link Meta#getSchemas(Meta.ConnectionHandle, String, Meta.Pat)}. */
  class SchemasRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.SchemasRequest.
        getDescriptor().findFieldByNumber(Requests.SchemasRequest.CONNECTION_ID_FIELD_NUMBER);
    private static final FieldDescriptor CATALOG_DESCRIPTOR = Requests.SchemasRequest.
        getDescriptor().findFieldByNumber(Requests.SchemasRequest.CATALOG_FIELD_NUMBER);
    private static final FieldDescriptor SCHEMA_PATTERN_DESCRIPTOR = Requests.SchemasRequest.
        getDescriptor().findFieldByNumber(Requests.SchemasRequest.SCHEMA_PATTERN_FIELD_NUMBER);

    public final String connectionId;
    public final String catalog;
    public final String schemaPattern;

    SchemasRequest() {
      connectionId = null;
      catalog = null;
      schemaPattern = null;
    }

    @JsonCreator
    public SchemasRequest(@JsonProperty("connectionId") String connectionId,
        @JsonProperty("catalog") String catalog,
        @JsonProperty("schemaPattern") String schemaPattern) {
      this.connectionId = connectionId;
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
    }

    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }

    @Override SchemasRequest deserialize(Message genericMsg) {
      final Requests.SchemasRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.SchemasRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      String catalog = null;
      if (msg.hasField(CATALOG_DESCRIPTOR)) {
        catalog = msg.getCatalog();
      }

      String schemaPattern = null;
      if (msg.hasField(SCHEMA_PATTERN_DESCRIPTOR)) {
        schemaPattern = msg.getSchemaPattern();
      }

      return new SchemasRequest(connectionId, catalog, schemaPattern);
    }

    @Override Requests.SchemasRequest serialize() {
      Requests.SchemasRequest.Builder builder = Requests.SchemasRequest.newBuilder();
      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }
      if (null != catalog) {
        builder.setCatalog(catalog);
      }
      if (null != schemaPattern) {
        builder.setSchemaPattern(schemaPattern);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, catalog);
      result = p(result, schemaPattern);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof SchemasRequest
          && Objects.equals(connectionId, ((SchemasRequest) o).connectionId)
          && Objects.equals(catalog, ((SchemasRequest) o).catalog)
          && Objects.equals(schemaPattern, ((SchemasRequest) o).schemaPattern);
    }
  }

  /** Request for
   * {@link Meta#getTables(Meta.ConnectionHandle, String, org.apache.calcite.avatica.Meta.Pat, org.apache.calcite.avatica.Meta.Pat, java.util.List)}
   */
  class TablesRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.TablesRequest.
        getDescriptor().findFieldByNumber(Requests.TablesRequest.CONNECTION_ID_FIELD_NUMBER);
    private static final FieldDescriptor CATALOG_DESCRIPTOR = Requests.TablesRequest.
        getDescriptor().findFieldByNumber(Requests.TablesRequest.CATALOG_FIELD_NUMBER);
    private static final FieldDescriptor SCHEMA_PATTERN_DESCRIPTOR = Requests.TablesRequest.
        getDescriptor().findFieldByNumber(Requests.TablesRequest.SCHEMA_PATTERN_FIELD_NUMBER);
    private static final FieldDescriptor TABLE_NAME_PATTERN_DESCRIPTOR = Requests.TablesRequest.
        getDescriptor().findFieldByNumber(Requests.TablesRequest.TABLE_NAME_PATTERN_FIELD_NUMBER);

    public final String connectionId;
    public final String catalog;
    public final String schemaPattern;
    public final String tableNamePattern;
    public final List<String> typeList;

    TablesRequest() {
      connectionId = null;
      catalog = null;
      schemaPattern = null;
      tableNamePattern = null;
      typeList = null;
    }

    @JsonCreator
    public TablesRequest(@JsonProperty("connectionId") String connectionId,
        @JsonProperty("catalog") String catalog,
        @JsonProperty("schemaPattern") String schemaPattern,
        @JsonProperty("tableNamePattern") String tableNamePattern,
        @JsonProperty("typeList") List<String> typeList) {
      this.connectionId = connectionId;
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
      this.tableNamePattern = tableNamePattern;
      this.typeList = typeList;
    }

    @Override Response accept(Service service) {
      return service.apply(this);
    }

    @Override Request deserialize(Message genericMsg) {
      final Requests.TablesRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.TablesRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      String catalog = null;
      if (msg.hasField(CATALOG_DESCRIPTOR)) {
        catalog = msg.getCatalog();
      }

      String schemaPattern = null;
      if (msg.hasField(SCHEMA_PATTERN_DESCRIPTOR)) {
        schemaPattern = msg.getSchemaPattern();
      }

      String tableNamePattern = null;
      if (msg.hasField(TABLE_NAME_PATTERN_DESCRIPTOR)) {
        tableNamePattern = msg.getTableNamePattern();
      }

      // Cannot determine if a value was set for a repeated field. Must use an extra boolean
      // parameter to distinguish an empty and null typeList.
      List<String> typeList = null;
      if (msg.getHasTypeList()) {
        typeList = msg.getTypeListList();
      }

      return new TablesRequest(connectionId, catalog, schemaPattern, tableNamePattern, typeList);
    }

    @Override Requests.TablesRequest serialize() {
      Requests.TablesRequest.Builder builder = Requests.TablesRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }
      if (null != catalog) {
        builder.setCatalog(catalog);
      }
      if (null != schemaPattern) {
        builder.setSchemaPattern(schemaPattern);
      }
      if (null != tableNamePattern) {
        builder.setTableNamePattern(tableNamePattern);
      }
      if (null != typeList) {
        builder.setHasTypeList(true);
        builder.addAllTypeList(typeList);
      } else {
        builder.setHasTypeList(false);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, catalog);
      result = p(result, schemaPattern);
      result = p(result, tableNamePattern);
      result = p(result, typeList);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof TablesRequest
          && Objects.equals(connectionId, ((TablesRequest) o).connectionId)
          && Objects.equals(catalog, ((TablesRequest) o).catalog)
          && Objects.equals(schemaPattern, ((TablesRequest) o).schemaPattern)
          && Objects.equals(tableNamePattern, ((TablesRequest) o).tableNamePattern)
          && Objects.equals(typeList, ((TablesRequest) o).typeList);
    }
  }

  /**
   * Request for {@link Meta#getTableTypes(Meta.ConnectionHandle)}.
   */
  class TableTypesRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.TableTypesRequest.
        getDescriptor().findFieldByNumber(Requests.TableTypesRequest.CONNECTION_ID_FIELD_NUMBER);
    public final String connectionId;

    public TableTypesRequest() {
      this.connectionId = null;
    }

    @JsonCreator
    public TableTypesRequest(@JsonProperty("connectionId") String connectionId) {
      this.connectionId = connectionId;
    }

    @Override ResultSetResponse accept(Service service) {
      return service.apply(this);
    }

    @Override TableTypesRequest deserialize(Message genericMsg) {
      final Requests.TableTypesRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.TableTypesRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      return new TableTypesRequest(connectionId);
    }

    @Override Requests.TableTypesRequest serialize() {
      Requests.TableTypesRequest.Builder builder = Requests.TableTypesRequest.newBuilder();
      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof TableTypesRequest
          && Objects.equals(connectionId, ((TableTypesRequest) o).connectionId);
    }
  }

  /** Request for
   * {@link Meta#getColumns(Meta.ConnectionHandle, String, org.apache.calcite.avatica.Meta.Pat, org.apache.calcite.avatica.Meta.Pat, org.apache.calcite.avatica.Meta.Pat)}.
   */
  class ColumnsRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.ColumnsRequest.
        getDescriptor().findFieldByNumber(Requests.ColumnsRequest.CONNECTION_ID_FIELD_NUMBER);
    private static final FieldDescriptor CATALOG_DESCRIPTOR = Requests.ColumnsRequest.
        getDescriptor().findFieldByNumber(Requests.ColumnsRequest.CATALOG_FIELD_NUMBER);
    private static final FieldDescriptor SCHEMA_PATTERN_DESCRIPTOR = Requests.ColumnsRequest.
        getDescriptor().findFieldByNumber(Requests.ColumnsRequest.SCHEMA_PATTERN_FIELD_NUMBER);
    private static final FieldDescriptor TABLE_NAME_PATTERN_DESCRIPTOR = Requests.ColumnsRequest.
        getDescriptor().findFieldByNumber(Requests.ColumnsRequest.TABLE_NAME_PATTERN_FIELD_NUMBER);
    private static final FieldDescriptor COLUMN_NAME_PATTERN_DESCRIPTOR = Requests.ColumnsRequest.
        getDescriptor().findFieldByNumber(Requests.ColumnsRequest.COLUMN_NAME_PATTERN_FIELD_NUMBER);

    public final String connectionId;
    public final String catalog;
    public final String schemaPattern;
    public final String tableNamePattern;
    public final String columnNamePattern;

    ColumnsRequest() {
      connectionId = null;
      catalog = null;
      schemaPattern = null;
      tableNamePattern = null;
      columnNamePattern = null;
    }

    @JsonCreator
    public ColumnsRequest(@JsonProperty("connectionId") String connectionId,
        @JsonProperty("catalog") String catalog,
        @JsonProperty("schemaPattern") String schemaPattern,
        @JsonProperty("tableNamePattern") String tableNamePattern,
        @JsonProperty("columnNamePattern") String columnNamePattern) {
      this.connectionId = connectionId;
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
      this.tableNamePattern = tableNamePattern;
      this.columnNamePattern = columnNamePattern;
    }

    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }

    @Override ColumnsRequest deserialize(Message genericMsg) {
      final Requests.ColumnsRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.ColumnsRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      String catalog = null;
      if (msg.hasField(CATALOG_DESCRIPTOR)) {
        catalog = msg.getCatalog();
      }

      String schemaPattern = null;
      if (msg.hasField(SCHEMA_PATTERN_DESCRIPTOR)) {
        schemaPattern = msg.getSchemaPattern();
      }

      String tableNamePattern = null;
      if (msg.hasField(TABLE_NAME_PATTERN_DESCRIPTOR)) {
        tableNamePattern = msg.getTableNamePattern();
      }

      String columnNamePattern = null;
      if (msg.hasField(COLUMN_NAME_PATTERN_DESCRIPTOR)) {
        columnNamePattern = msg.getColumnNamePattern();
      }

      return new ColumnsRequest(connectionId, catalog, schemaPattern, tableNamePattern,
          columnNamePattern);
    }

    @Override Requests.ColumnsRequest serialize() {
      Requests.ColumnsRequest.Builder builder = Requests.ColumnsRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }
      if (null != catalog) {
        builder.setCatalog(catalog);
      }
      if (null != schemaPattern) {
        builder.setSchemaPattern(schemaPattern);
      }
      if (null != tableNamePattern) {
        builder.setTableNamePattern(tableNamePattern);
      }
      if (null != columnNamePattern) {
        builder.setColumnNamePattern(columnNamePattern);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, catalog);
      result = p(result, columnNamePattern);
      result = p(result, schemaPattern);
      result = p(result, tableNamePattern);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof ColumnsRequest
          && Objects.equals(connectionId, ((ColumnsRequest) o).connectionId)
          && Objects.equals(catalog, ((ColumnsRequest) o).catalog)
          && Objects.equals(schemaPattern, ((ColumnsRequest) o).schemaPattern)
          && Objects.equals(tableNamePattern, ((ColumnsRequest) o).tableNamePattern)
          && Objects.equals(columnNamePattern, ((ColumnsRequest) o).columnNamePattern);
    }
  }

  /** Request for
   * {@link Meta#getTypeInfo(Meta.ConnectionHandle)}. */
  class TypeInfoRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.TypeInfoRequest.
        getDescriptor().findFieldByNumber(Requests.TypeInfoRequest.CONNECTION_ID_FIELD_NUMBER);
    public final String connectionId;

    public TypeInfoRequest() {
      connectionId = null;
    }

    @JsonCreator
    public TypeInfoRequest(@JsonProperty("connectionId") String connectionId) {
      this.connectionId = connectionId;
    }

    @Override ResultSetResponse accept(Service service) {
      return service.apply(this);
    }

    @Override TypeInfoRequest deserialize(Message genericMsg) {
      final Requests.TypeInfoRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.TypeInfoRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      return new TypeInfoRequest(connectionId);
    }

    @Override Requests.TypeInfoRequest serialize() {
      Requests.TypeInfoRequest.Builder builder = Requests.TypeInfoRequest.newBuilder();
      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof TypeInfoRequest
          && Objects.equals(connectionId, ((TypeInfoRequest) o).connectionId);
    }
  }

  /** Response that contains a result set.
   *
   * <p>Regular result sets have {@code updateCount} -1;
   * any other value means a dummy result set that is just a count, and has
   * no signature and no other data.
   *
   * <p>Several types of request, including
   * {@link org.apache.calcite.avatica.Meta#getCatalogs(Meta.ConnectionHandle)} and
   * {@link org.apache.calcite.avatica.Meta#getSchemas(Meta.ConnectionHandle, String, org.apache.calcite.avatica.Meta.Pat)}
   * {@link Meta#getTables(Meta.ConnectionHandle, String, Meta.Pat, Meta.Pat, List)}
   * {@link Meta#getTableTypes(Meta.ConnectionHandle)}
   * return this response. */
  class ResultSetResponse extends Response {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Responses.ResultSetResponse.
        getDescriptor().findFieldByNumber(Responses.ResultSetResponse.CONNECTION_ID_FIELD_NUMBER);
    private static final FieldDescriptor SIGNATURE_DESCRIPTOR = Responses.ResultSetResponse.
        getDescriptor().findFieldByNumber(Responses.ResultSetResponse.SIGNATURE_FIELD_NUMBER);
    private static final FieldDescriptor FIRST_FRAME_DESCRIPTOR = Responses.ResultSetResponse.
        getDescriptor().findFieldByNumber(Responses.ResultSetResponse.FIRST_FRAME_FIELD_NUMBER);
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.ResultSetResponse.
        getDescriptor().findFieldByNumber(Responses.ResultSetResponse.METADATA_FIELD_NUMBER);

    public final String connectionId;
    public final int statementId;
    public final boolean ownStatement;
    public final Meta.Signature signature;
    public final Meta.Frame firstFrame;
    public final long updateCount;
    public final RpcMetadataResponse rpcMetadata;

    ResultSetResponse() {
      connectionId = null;
      statementId = 0;
      ownStatement = false;
      signature = null;
      firstFrame = null;
      updateCount = 0;
      rpcMetadata = null;
    }

    @JsonCreator
    public ResultSetResponse(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId,
        @JsonProperty("ownStatement") boolean ownStatement,
        @JsonProperty("signature") Meta.Signature signature,
        @JsonProperty("firstFrame") Meta.Frame firstFrame,
        @JsonProperty("updateCount") long updateCount,
        @JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.ownStatement = ownStatement;
      this.signature = signature;
      this.firstFrame = firstFrame;
      this.updateCount = updateCount;
      this.rpcMetadata = rpcMetadata;
    }

    @Override ResultSetResponse deserialize(Message genericMsg) {
      final Responses.ResultSetResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.ResultSetResponse.class);

      return fromProto(msg);
    }

    static ResultSetResponse fromProto(Responses.ResultSetResponse msg) {
      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      Meta.Signature signature = null;
      if (msg.hasField(SIGNATURE_DESCRIPTOR)) {
        signature = Meta.Signature.fromProto(msg.getSignature());
      }

      Meta.Frame frame = null;
      if (msg.hasField(FIRST_FRAME_DESCRIPTOR)) {
        frame = Meta.Frame.fromProto(msg.getFirstFrame());
      }

      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new ResultSetResponse(connectionId, msg.getStatementId(), msg.getOwnStatement(),
          signature, frame, msg.getUpdateCount(), metadata);
    }

    @Override Responses.ResultSetResponse serialize() {
      Responses.ResultSetResponse.Builder builder = Responses.ResultSetResponse.newBuilder();

      builder.setStatementId(statementId).setOwnStatement(ownStatement).setUpdateCount(updateCount);

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      if (null != signature) {
        builder.setSignature(signature.toProto());
      }

      if (null != firstFrame) {
        builder.setFirstFrame(firstFrame.toProto());
      }

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, firstFrame);
      result = p(result, ownStatement);
      result = p(result, signature);
      result = p(result, statementId);
      result = p(result, updateCount);
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof ResultSetResponse
          && statementId == ((ResultSetResponse) o).statementId
          && ownStatement == ((ResultSetResponse) o).ownStatement
          && updateCount == ((ResultSetResponse) o).updateCount
          && Objects.equals(connectionId, ((ResultSetResponse) o).connectionId)
          && Objects.equals(firstFrame, ((ResultSetResponse) o).firstFrame)
          && Objects.equals(signature, ((ResultSetResponse) o).signature)
          && Objects.equals(rpcMetadata, ((ResultSetResponse) o).rpcMetadata);
    }
  }

  /** Request for
   * {@link Meta#prepareAndExecute(Meta.StatementHandle, String, long, int, Meta.PrepareCallback)}.
   */
  class PrepareAndExecuteRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.
        PrepareAndExecuteRequest.getDescriptor().findFieldByNumber(
            Requests.PrepareAndExecuteRequest.CONNECTION_ID_FIELD_NUMBER);
    private static final FieldDescriptor SQL_DESCRIPTOR = Requests.
        PrepareAndExecuteRequest.getDescriptor().findFieldByNumber(
            Requests.PrepareAndExecuteRequest.SQL_FIELD_NUMBER);
    private static final FieldDescriptor MAX_ROWS_TOTAL_DESCRIPTOR = Requests.
        PrepareAndExecuteRequest.getDescriptor().findFieldByNumber(
            Requests.PrepareAndExecuteRequest.MAX_ROWS_TOTAL_FIELD_NUMBER);
    private static final FieldDescriptor FIRST_FRAME_MAX_SIZE_DESCRIPTOR = Requests.
        PrepareAndExecuteRequest.getDescriptor().findFieldByNumber(
            Requests.PrepareAndExecuteRequest.FIRST_FRAME_MAX_SIZE_FIELD_NUMBER);

    public final String connectionId;
    public final String sql;
    public final long maxRowCount;
    public final int maxRowsInFirstFrame;
    public final int statementId;

    PrepareAndExecuteRequest() {
      connectionId = null;
      sql = null;
      maxRowCount = 0;
      maxRowsInFirstFrame = 0;
      statementId = 0;
    }

    public PrepareAndExecuteRequest(String connectionId, int statementId, String sql,
        long maxRowCount) {
      this(connectionId, statementId, sql, maxRowCount, AvaticaUtils.toSaturatedInt(maxRowCount));
    }

    @JsonCreator
    public PrepareAndExecuteRequest(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId,
        @JsonProperty("sql") String sql,
        @JsonProperty("maxRowsTotal") long maxRowCount,
        @JsonProperty("maxRowsInFirstFrame") int maxRowsInFirstFrame) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.sql = sql;
      this.maxRowCount = maxRowCount;
      this.maxRowsInFirstFrame = maxRowsInFirstFrame;
    }

    @Override ExecuteResponse accept(Service service) {
      return service.apply(this);
    }

    @Override PrepareAndExecuteRequest deserialize(Message genericMsg) {
      final Requests.PrepareAndExecuteRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.PrepareAndExecuteRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      String sql = null;
      if (msg.hasField(SQL_DESCRIPTOR)) {
        sql = msg.getSql();
      }

      // Use the old attribute, unless the new is set
      long maxRowsTotal = msg.getMaxRowCount();
      if (msg.hasField(MAX_ROWS_TOTAL_DESCRIPTOR)) {
        maxRowsTotal = msg.getMaxRowsTotal();
      }

      // Use maxRowCount (cast to an integer) if firstFrameMaxSize isn't set
      int maxRowsInFirstFrame = (int) maxRowsTotal;
      if (msg.hasField(FIRST_FRAME_MAX_SIZE_DESCRIPTOR)) {
        maxRowsInFirstFrame = msg.getFirstFrameMaxSize();
      }

      return new PrepareAndExecuteRequest(connectionId, msg.getStatementId(), sql,
          maxRowsTotal, maxRowsInFirstFrame);
    }

    @Override Requests.PrepareAndExecuteRequest serialize() {
      Requests.PrepareAndExecuteRequest.Builder builder = Requests.PrepareAndExecuteRequest
          .newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }
      if (null != sql) {
        builder.setSql(sql);
      }
      builder.setStatementId(statementId);
      // Set both attributes for backwards compat
      builder.setMaxRowCount(maxRowCount).setMaxRowsTotal(maxRowCount);
      builder.setFirstFrameMaxSize(maxRowsInFirstFrame);

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, maxRowCount);
      result = p(result, maxRowsInFirstFrame);
      result = p(result, sql);
      result = p(result, statementId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof PrepareAndExecuteRequest
          && statementId == ((PrepareAndExecuteRequest) o).statementId
          && maxRowCount == ((PrepareAndExecuteRequest) o).maxRowCount
          && maxRowsInFirstFrame == ((PrepareAndExecuteRequest) o).maxRowsInFirstFrame
          && Objects.equals(connectionId, ((PrepareAndExecuteRequest) o).connectionId)
          && Objects.equals(sql, ((PrepareAndExecuteRequest) o).sql);
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#execute}. */
  class ExecuteRequest extends Request {
    private static final FieldDescriptor STATEMENT_HANDLE_DESCRIPTOR = Requests.ExecuteRequest.
        getDescriptor().findFieldByNumber(Requests.ExecuteRequest.STATEMENTHANDLE_FIELD_NUMBER);
    public final Meta.StatementHandle statementHandle;
    public final List<TypedValue> parameterValues;
    public final long maxRowCount;

    ExecuteRequest() {
      statementHandle = null;
      parameterValues = null;
      maxRowCount = 0;
    }

    @JsonCreator
    public ExecuteRequest(
        @JsonProperty("statementHandle") Meta.StatementHandle statementHandle,
        @JsonProperty("parameterValues") List<TypedValue> parameterValues,
        @JsonProperty("maxRowCount") long maxRowCount) {
      this.statementHandle = statementHandle;
      this.parameterValues = parameterValues;
      this.maxRowCount = maxRowCount;
    }

    @Override ExecuteResponse accept(Service service) {
      return service.apply(this);
    }

    @Override ExecuteRequest deserialize(Message genericMsg) {
      final Requests.ExecuteRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.ExecuteRequest.class);

      Meta.StatementHandle statementHandle = null;
      if (msg.hasField(STATEMENT_HANDLE_DESCRIPTOR)) {
        statementHandle = Meta.StatementHandle.fromProto(msg.getStatementHandle());
      }

      List<TypedValue> values = null;
      if (msg.getHasParameterValues()) {
        values = new ArrayList<>(msg.getParameterValuesCount());
        for (Common.TypedValue valueProto : msg.getParameterValuesList()) {
          values.add(TypedValue.fromProto(valueProto));
        }
      }

      return new ExecuteRequest(statementHandle, values, msg.getFirstFrameMaxSize());
    }

    @Override Requests.ExecuteRequest serialize() {
      Requests.ExecuteRequest.Builder builder = Requests.ExecuteRequest.newBuilder();

      if (null != statementHandle) {
        builder.setStatementHandle(statementHandle.toProto());
      }

      if (null != parameterValues) {
        builder.setHasParameterValues(true);
        for (TypedValue paramValue : parameterValues) {
          if (paramValue == null) {
            builder.addParameterValues(TypedValue.NULL.toProto());
          } else {
            builder.addParameterValues(paramValue.toProto());
          }
        }
      } else {
        builder.setHasParameterValues(false);
      }

      builder.setFirstFrameMaxSize(maxRowCount);

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, statementHandle);
      result = p(result, parameterValues);
      result = p(result, maxRowCount);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof ExecuteRequest
          && maxRowCount == ((ExecuteRequest) o).maxRowCount
          && Objects.equals(statementHandle, ((ExecuteRequest) o).statementHandle)
          && Objects.equals(parameterValues, ((ExecuteRequest) o).parameterValues);
    }
  }

  /** Response to a
   * {@link org.apache.calcite.avatica.remote.Service.PrepareAndExecuteRequest}. */
  class ExecuteResponse extends Response {
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.ExecuteResponse.
        getDescriptor().findFieldByNumber(Responses.ExecuteResponse.METADATA_FIELD_NUMBER);
    public final List<ResultSetResponse> results;
    public boolean missingStatement = false;
    public final RpcMetadataResponse rpcMetadata;

    ExecuteResponse() {
      results = null;
      rpcMetadata = null;
    }

    @JsonCreator
    public ExecuteResponse(@JsonProperty("resultSets") List<ResultSetResponse> results,
        @JsonProperty("missingStatement") boolean missingStatement,
        @JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.results = results;
      this.missingStatement = missingStatement;
      this.rpcMetadata = rpcMetadata;
    }

    @Override ExecuteResponse deserialize(Message genericMsg) {
      final Responses.ExecuteResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.ExecuteResponse.class);

      List<Responses.ResultSetResponse> msgResults = msg.getResultsList();
      List<ResultSetResponse> copiedResults = new ArrayList<>(msgResults.size());

      for (Responses.ResultSetResponse msgResult : msgResults) {
        copiedResults.add(ResultSetResponse.fromProto(msgResult));
      }

      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new ExecuteResponse(copiedResults, msg.getMissingStatement(), metadata);
    }

    @Override Responses.ExecuteResponse serialize() {
      Responses.ExecuteResponse.Builder builder = Responses.ExecuteResponse.newBuilder();

      if (null != results) {
        for (ResultSetResponse result : results) {
          builder.addResults(result.serialize());
        }
      }

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.setMissingStatement(missingStatement).build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, results);
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof ExecuteResponse
          && Objects.equals(results, ((ExecuteResponse) o).results)
          && Objects.equals(rpcMetadata, ((ExecuteResponse) o).rpcMetadata);
    }
  }

  /** Request for
   * {@link Meta#prepare(Meta.ConnectionHandle, String, long)}. */
  class PrepareRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.PrepareRequest.
        getDescriptor().findFieldByNumber(Requests.PrepareRequest.CONNECTION_ID_FIELD_NUMBER);
    private static final FieldDescriptor SQL_DESCRIPTOR = Requests.PrepareRequest.
        getDescriptor().findFieldByNumber(Requests.PrepareRequest.SQL_FIELD_NUMBER);
    private static final FieldDescriptor MAX_ROWS_TOTAL_DESCRIPTOR = Requests.PrepareRequest.
        getDescriptor().findFieldByNumber(Requests.PrepareRequest.MAX_ROWS_TOTAL_FIELD_NUMBER);

    public final String connectionId;
    public final String sql;
    public final long maxRowCount;

    PrepareRequest() {
      connectionId = null;
      sql = null;
      maxRowCount = 0;
    }

    @JsonCreator
    public PrepareRequest(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("sql") String sql,
        @JsonProperty("maxRowCount") long maxRowCount) {
      this.connectionId = connectionId;
      this.sql = sql;
      this.maxRowCount = maxRowCount;
    }

    @Override PrepareResponse accept(Service service) {
      return service.apply(this);
    }

    @Override PrepareRequest deserialize(Message genericMsg) {
      final Requests.PrepareRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.PrepareRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      String sql = null;
      if (msg.hasField(SQL_DESCRIPTOR)) {
        sql = msg.getSql();
      }

      // Use the old field unless the new field is provided
      long totalRowsForStatement = msg.getMaxRowCount();
      if (msg.hasField(MAX_ROWS_TOTAL_DESCRIPTOR)) {
        totalRowsForStatement = msg.getMaxRowsTotal();
      }

      return new PrepareRequest(connectionId, sql, totalRowsForStatement);
    }

    @Override Requests.PrepareRequest serialize() {
      Requests.PrepareRequest.Builder builder = Requests.PrepareRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      if (null != sql) {
        builder.setSql(sql);
      }

      // Set both field for backwards compatibility
      return builder.setMaxRowCount(maxRowCount).setMaxRowsTotal(maxRowCount).build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, maxRowCount);
      result = p(result, sql);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof PrepareRequest
          && maxRowCount == ((PrepareRequest) o).maxRowCount
          && Objects.equals(connectionId, ((PrepareRequest) o).connectionId)
          && Objects.equals(sql, ((PrepareRequest) o).sql);
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.PrepareRequest}. */
  class PrepareResponse extends Response {
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.PrepareResponse.
        getDescriptor().findFieldByNumber(Responses.PrepareResponse.METADATA_FIELD_NUMBER);
    public final Meta.StatementHandle statement;
    public final RpcMetadataResponse rpcMetadata;

    PrepareResponse() {
      statement = null;
      rpcMetadata = null;
    }

    @JsonCreator
    public PrepareResponse(
        @JsonProperty("statement") Meta.StatementHandle statement,
        @JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.statement = statement;
      this.rpcMetadata = rpcMetadata;
    }

    @Override PrepareResponse deserialize(Message genericMsg) {
      final Responses.PrepareResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.PrepareResponse.class);

      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new PrepareResponse(Meta.StatementHandle.fromProto(msg.getStatement()), metadata);
    }

    @Override Responses.PrepareResponse serialize() {
      Responses.PrepareResponse.Builder builder = Responses.PrepareResponse.newBuilder();

      if (null != statement) {
        builder.setStatement(statement.toProto());
      }

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, statement);
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof PrepareResponse
          && Objects.equals(statement, ((PrepareResponse) o).statement)
          && Objects.equals(rpcMetadata, ((PrepareResponse) o).rpcMetadata);
    }
  }

  /** Request for
   * {@link Meta#fetch}. */
  class FetchRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.FetchRequest.
        getDescriptor().findFieldByNumber(Requests.FetchRequest.CONNECTION_ID_FIELD_NUMBER);
    private static final FieldDescriptor FRAME_MAX_SIZE_DESCRIPTOR = Requests.FetchRequest.
        getDescriptor().findFieldByNumber(Requests.FetchRequest.FRAME_MAX_SIZE_FIELD_NUMBER);

    public final String connectionId;
    public final int statementId;
    public final long offset;
    /** Maximum number of rows to be returned in the frame. Negative means no
     * limit. */
    public final int fetchMaxRowCount;

    FetchRequest() {
      connectionId = null;
      statementId = 0;
      offset = 0;
      fetchMaxRowCount = 0;
    }

    @JsonCreator
    public FetchRequest(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId,
        @JsonProperty("offset") long offset,
        @JsonProperty("fetchMaxRowCount") int fetchMaxRowCount) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.offset = offset;
      this.fetchMaxRowCount = fetchMaxRowCount;
    }

    @Override FetchResponse accept(Service service) {
      return service.apply(this);
    }

    @Override FetchRequest deserialize(Message genericMsg) {
      final Requests.FetchRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.FetchRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      int fetchMaxRowCount = msg.getFetchMaxRowCount();
      if (msg.hasField(FRAME_MAX_SIZE_DESCRIPTOR)) {
        fetchMaxRowCount = msg.getFrameMaxSize();
      }

      return new FetchRequest(connectionId, msg.getStatementId(), msg.getOffset(),
          fetchMaxRowCount);
    }

    @Override Requests.FetchRequest serialize() {
      Requests.FetchRequest.Builder builder = Requests.FetchRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      builder.setStatementId(statementId);
      builder.setOffset(offset);
      // Both fields for backwards compat
      builder.setFetchMaxRowCount(fetchMaxRowCount).setFrameMaxSize(fetchMaxRowCount);

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, fetchMaxRowCount);
      result = p(result, offset);
      result = p(result, statementId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof FetchRequest
          && statementId == ((FetchRequest) o).statementId
          && offset == ((FetchRequest) o).offset
          && fetchMaxRowCount == ((FetchRequest) o).fetchMaxRowCount
          && Objects.equals(connectionId, ((FetchRequest) o).connectionId);
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.FetchRequest}. */
  class FetchResponse extends Response {
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.FetchResponse.
        getDescriptor().findFieldByNumber(Responses.FetchResponse.METADATA_FIELD_NUMBER);
    public final Meta.Frame frame;
    public boolean missingStatement = false;
    public boolean missingResults = false;
    public final RpcMetadataResponse rpcMetadata;

    FetchResponse() {
      frame = null;
      rpcMetadata = null;
    }

    @JsonCreator
    public FetchResponse(@JsonProperty("frame") Meta.Frame frame,
        @JsonProperty("missingStatement") boolean missingStatement,
        @JsonProperty("missingResults") boolean missingResults,
        @JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.frame = frame;
      this.missingStatement = missingStatement;
      this.missingResults = missingResults;
      this.rpcMetadata = rpcMetadata;
    }

    @Override FetchResponse deserialize(Message genericMsg) {
      final Responses.FetchResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.FetchResponse.class);

      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new FetchResponse(Meta.Frame.fromProto(msg.getFrame()), msg.getMissingStatement(),
          msg.getMissingResults(), metadata);
    }

    @Override Responses.FetchResponse serialize() {
      Responses.FetchResponse.Builder builder = Responses.FetchResponse.newBuilder();

      if (null != frame) {
        builder.setFrame(frame.toProto());
      }

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.setMissingStatement(missingStatement)
          .setMissingResults(missingResults).build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, frame);
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof FetchResponse
          && Objects.equals(frame, ((FetchResponse) o).frame)
          && Objects.equals(rpcMetadata, ((FetchResponse) o).rpcMetadata)
          && missingStatement == ((FetchResponse) o).missingStatement;
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#createStatement(org.apache.calcite.avatica.Meta.ConnectionHandle)}. */
  class CreateStatementRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.CreateStatementRequest.
        getDescriptor().findFieldByNumber(
            Requests.CreateStatementRequest.CONNECTION_ID_FIELD_NUMBER);
    public final String connectionId;

    CreateStatementRequest() {
      connectionId = null;
    }

    @JsonCreator
    public CreateStatementRequest(
        @JsonProperty("signature") String connectionId) {
      this.connectionId = connectionId;
    }

    @Override CreateStatementResponse accept(Service service) {
      return service.apply(this);
    }

    @Override CreateStatementRequest deserialize(Message genericMsg) {
      final Requests.CreateStatementRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.CreateStatementRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      return new CreateStatementRequest(connectionId);
    }

    @Override Requests.CreateStatementRequest serialize() {
      Requests.CreateStatementRequest.Builder builder = Requests.CreateStatementRequest
          .newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof CreateStatementRequest
          && Objects.equals(connectionId, ((CreateStatementRequest) o).connectionId);
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.CreateStatementRequest}. */
  class CreateStatementResponse extends Response {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Responses.
        CreateStatementResponse.getDescriptor().findFieldByNumber(
            Responses.CreateStatementResponse.CONNECTION_ID_FIELD_NUMBER);
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.
        CreateStatementResponse.getDescriptor().findFieldByNumber(
            Responses.CreateStatementResponse.METADATA_FIELD_NUMBER);
    public final String connectionId;
    public final int statementId;
    public final RpcMetadataResponse rpcMetadata;

    CreateStatementResponse() {
      connectionId = null;
      statementId = 0;
      rpcMetadata = null;
    }

    @JsonCreator
    public CreateStatementResponse(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId,
        @JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.rpcMetadata = rpcMetadata;
    }

    @Override CreateStatementResponse deserialize(Message genericMsg) {
      final Responses.CreateStatementResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.CreateStatementResponse.class);
      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new CreateStatementResponse(connectionId, msg.getStatementId(), metadata);
    }

    @Override Responses.CreateStatementResponse serialize() {
      Responses.CreateStatementResponse.Builder builder = Responses.CreateStatementResponse
          .newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      builder.setStatementId(statementId);

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, statementId);
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof CreateStatementResponse
          && statementId == ((CreateStatementResponse) o).statementId
          && Objects.equals(connectionId, ((CreateStatementResponse) o).connectionId)
          && Objects.equals(rpcMetadata, ((CreateStatementResponse) o).rpcMetadata);
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#closeStatement(org.apache.calcite.avatica.Meta.StatementHandle)}. */
  class CloseStatementRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.CloseStatementRequest.
        getDescriptor().findFieldByNumber(
            Requests.CloseStatementRequest.CONNECTION_ID_FIELD_NUMBER);
    public final String connectionId;
    public final int statementId;

    CloseStatementRequest() {
      connectionId = null;
      statementId = 0;
    }

    @JsonCreator
    public CloseStatementRequest(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId) {
      this.connectionId = connectionId;
      this.statementId = statementId;
    }

    @Override CloseStatementResponse accept(Service service) {
      return service.apply(this);
    }

    @Override CloseStatementRequest deserialize(Message genericMsg) {
      final Requests.CloseStatementRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.CloseStatementRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      return new CloseStatementRequest(connectionId, msg.getStatementId());
    }

    @Override Requests.CloseStatementRequest serialize() {
      Requests.CloseStatementRequest.Builder builder = Requests.CloseStatementRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      return builder.setStatementId(statementId).build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, statementId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof CloseStatementRequest
          && statementId == ((CloseStatementRequest) o).statementId
          && Objects.equals(connectionId, ((CloseStatementRequest) o).connectionId);
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.CloseStatementRequest}. */
  class CloseStatementResponse extends Response {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Responses.
        CloseStatementResponse.getDescriptor().findFieldByNumber(
            Responses.CloseStatementResponse.METADATA_FIELD_NUMBER);

    public final RpcMetadataResponse rpcMetadata;

    public CloseStatementResponse() {
      rpcMetadata = null;
    }

    @JsonCreator
    public CloseStatementResponse(@JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.rpcMetadata = rpcMetadata;
    }

    @Override CloseStatementResponse deserialize(Message genericMsg) {
      final Responses.CloseStatementResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.CloseStatementResponse.class);
      RpcMetadataResponse metadata = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new CloseStatementResponse(metadata);
    }

    @Override Responses.CloseStatementResponse serialize() {
      Responses.CloseStatementResponse.Builder builder =
          Responses.CloseStatementResponse.newBuilder();

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof CloseStatementResponse
          && Objects.equals(rpcMetadata, ((CloseStatementResponse) o).rpcMetadata);
    }
  }

  /** Request for
   * {@link Meta#openConnection}. */
  class OpenConnectionRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.OpenConnectionRequest
        .getDescriptor().findFieldByNumber(
            Requests.OpenConnectionRequest.CONNECTION_ID_FIELD_NUMBER);
    public final String connectionId;
    public final Map<String, String> info;

    public OpenConnectionRequest() {
      connectionId = null;
      info = null;
    }

    @JsonCreator
    public OpenConnectionRequest(@JsonProperty("connectionId") String connectionId,
        @JsonProperty("info") Map<String, String> info) {
      this.connectionId = connectionId;
      this.info = info;
    }

    @Override OpenConnectionResponse accept(Service service) {
      return service.apply(this);
    }

    /**
     * Serializes the necessary properties into a Map.
     *
     * @param props The properties to serialize.
     * @return A representation of the Properties as a Map.
     */
    public static Map<String, String> serializeProperties(Properties props) {
      Map<String, String> infoAsString = new HashMap<>();
      for (Map.Entry<Object, Object> entry : props.entrySet()) {
        // Determine if this is a property we want to forward to the server
        if (!BuiltInConnectionProperty.isLocalProperty(entry.getKey())) {
          infoAsString.put(entry.getKey().toString(), entry.getValue().toString());
        }
      }
      return infoAsString;
    }

    @Override Request deserialize(Message genericMsg) {
      final Requests.OpenConnectionRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.OpenConnectionRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      Map<String, String> info = msg.getInfoMap();
      if (info.isEmpty()) {
        info = null;
      }

      return new OpenConnectionRequest(connectionId, info);
    }

    @Override Message serialize() {
      Requests.OpenConnectionRequest.Builder builder = Requests.OpenConnectionRequest.newBuilder();
      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }
      if (null != info) {
        builder.putAllInfo(info);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, info);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof OpenConnectionRequest
          && Objects.equals(connectionId, ((OpenConnectionRequest) o).connectionId)
          && Objects.equals(info, ((OpenConnectionRequest) o).info);
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.OpenConnectionRequest}. */
  class OpenConnectionResponse extends Response {
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.OpenConnectionResponse
        .getDescriptor().findFieldByNumber(
            Responses.OpenConnectionResponse.METADATA_FIELD_NUMBER);
    public final RpcMetadataResponse rpcMetadata;

    public OpenConnectionResponse() {
      rpcMetadata = null;
    }

    @JsonCreator
    public OpenConnectionResponse(@JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.rpcMetadata = rpcMetadata;
    }

    @Override OpenConnectionResponse deserialize(Message genericMsg) {
      final Responses.OpenConnectionResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.OpenConnectionResponse.class);

      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new OpenConnectionResponse(metadata);
    }

    @Override Responses.OpenConnectionResponse serialize() {
      Responses.OpenConnectionResponse.Builder builder =
          Responses.OpenConnectionResponse.newBuilder();

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof OpenConnectionResponse
          && Objects.equals(rpcMetadata, ((OpenConnectionResponse) o).rpcMetadata);
    }
  }

  /** Request for
   * {@link Meta#closeConnection(org.apache.calcite.avatica.Meta.ConnectionHandle)}. */
  class CloseConnectionRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.CloseConnectionRequest
        .getDescriptor().findFieldByNumber(
            Requests.CloseConnectionRequest.CONNECTION_ID_FIELD_NUMBER);
    public final String connectionId;

    CloseConnectionRequest() {
      connectionId = null;
    }

    @JsonCreator
    public CloseConnectionRequest(
        @JsonProperty("connectionId") String connectionId) {
      this.connectionId = connectionId;
    }

    @Override CloseConnectionResponse accept(Service service) {
      return service.apply(this);
    }

    @Override CloseConnectionRequest deserialize(Message genericMsg) {
      final Requests.CloseConnectionRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.CloseConnectionRequest.class);
      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      return new CloseConnectionRequest(connectionId);
    }

    @Override Requests.CloseConnectionRequest serialize() {
      Requests.CloseConnectionRequest.Builder builder = Requests.CloseConnectionRequest
          .newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof CloseConnectionRequest
          && Objects.equals(connectionId, ((CloseConnectionRequest) o).connectionId);
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.CloseConnectionRequest}. */
  class CloseConnectionResponse extends Response {
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.CloseConnectionResponse
        .getDescriptor().findFieldByNumber(
            Responses.CloseConnectionResponse.METADATA_FIELD_NUMBER);

    public final RpcMetadataResponse rpcMetadata;

    public CloseConnectionResponse() {
      rpcMetadata = null;
    }

    @JsonCreator
    public CloseConnectionResponse(@JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.rpcMetadata = rpcMetadata;
    }

    @Override CloseConnectionResponse deserialize(Message genericMsg) {
      final Responses.CloseConnectionResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.CloseConnectionResponse.class);

      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new CloseConnectionResponse(metadata);
    }

    @Override Responses.CloseConnectionResponse serialize() {
      Responses.CloseConnectionResponse.Builder builder =
          Responses.CloseConnectionResponse.newBuilder();

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof CloseConnectionResponse
          && Objects.equals(rpcMetadata, ((CloseConnectionResponse) o).rpcMetadata);
    }
  }

  /** Request for {@link Meta#connectionSync(Meta.ConnectionHandle, Meta.ConnectionProperties)}. */
  class ConnectionSyncRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.ConnectionSyncRequest
        .getDescriptor().findFieldByNumber(
            Requests.ConnectionSyncRequest.CONNECTION_ID_FIELD_NUMBER);
    private static final FieldDescriptor CONN_PROPS_DESCRIPTOR = Requests.ConnectionSyncRequest
        .getDescriptor().findFieldByNumber(Requests.ConnectionSyncRequest.CONN_PROPS_FIELD_NUMBER);

    public final String connectionId;
    public final Meta.ConnectionProperties connProps;

    ConnectionSyncRequest() {
      connectionId = null;
      connProps = null;
    }

    @JsonCreator
    public ConnectionSyncRequest(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("connProps") Meta.ConnectionProperties connProps) {
      this.connectionId = connectionId;
      this.connProps = connProps;
    }

    @Override ConnectionSyncResponse accept(Service service) {
      return service.apply(this);
    }

    @Override ConnectionSyncRequest deserialize(Message genericMsg) {
      final Requests.ConnectionSyncRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.ConnectionSyncRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      Meta.ConnectionProperties connProps = null;
      if (msg.hasField(CONN_PROPS_DESCRIPTOR)) {
        connProps = ConnectionPropertiesImpl.fromProto(msg.getConnProps());
      }

      return new ConnectionSyncRequest(connectionId, connProps);
    }

    @Override Requests.ConnectionSyncRequest serialize() {
      Requests.ConnectionSyncRequest.Builder builder = Requests.ConnectionSyncRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      if (null != connProps) {
        builder.setConnProps(connProps.toProto());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connProps);
      result = p(result, connectionId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof ConnectionSyncRequest
          && Objects.equals(connectionId, ((ConnectionSyncRequest) o).connectionId)
          && Objects.equals(connProps, ((ConnectionSyncRequest) o).connProps);
    }
  }

  /** Response for
   * {@link Meta#connectionSync(Meta.ConnectionHandle, Meta.ConnectionProperties)}. */
  class ConnectionSyncResponse extends Response {
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.ConnectionSyncResponse
        .getDescriptor().findFieldByNumber(Responses.ConnectionSyncResponse.METADATA_FIELD_NUMBER);
    public final Meta.ConnectionProperties connProps;
    public final RpcMetadataResponse rpcMetadata;

    ConnectionSyncResponse() {
      connProps = null;
      rpcMetadata = null;
    }

    @JsonCreator
    public ConnectionSyncResponse(@JsonProperty("connProps") Meta.ConnectionProperties connProps,
        @JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.connProps = connProps;
      this.rpcMetadata = rpcMetadata;
    }

    @Override ConnectionSyncResponse deserialize(Message genericMsg) {
      final Responses.ConnectionSyncResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.ConnectionSyncResponse.class);
      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new ConnectionSyncResponse(ConnectionPropertiesImpl.fromProto(msg.getConnProps()),
          metadata);
    }

    @Override Responses.ConnectionSyncResponse serialize() {
      Responses.ConnectionSyncResponse.Builder builder = Responses.ConnectionSyncResponse
          .newBuilder();

      if (null != connProps) {
        builder.setConnProps(connProps.toProto());
      }

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connProps);
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof ConnectionSyncResponse
          && Objects.equals(connProps, ((ConnectionSyncResponse) o).connProps)
          && Objects.equals(rpcMetadata, ((ConnectionSyncResponse) o).rpcMetadata);
    }
  }

  /** Response for
   * {@link Meta#getDatabaseProperties(Meta.ConnectionHandle)}. */
  class DatabasePropertyResponse extends Response {
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.DatabasePropertyResponse
        .getDescriptor().findFieldByNumber(
            Responses.DatabasePropertyResponse.METADATA_FIELD_NUMBER);
    public final Map<Meta.DatabaseProperty, Object> map;
    public final RpcMetadataResponse rpcMetadata;

    DatabasePropertyResponse() {
      map = null;
      rpcMetadata = null;
    }

    @JsonCreator
    public DatabasePropertyResponse(@JsonProperty("map") Map<Meta.DatabaseProperty, Object> map,
        @JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.map = map;
      this.rpcMetadata = rpcMetadata;
    }

    @Override DatabasePropertyResponse deserialize(Message genericMsg) {
      final Responses.DatabasePropertyResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.DatabasePropertyResponse.class);
      HashMap<Meta.DatabaseProperty, Object> properties = new HashMap<>();
      for (Responses.DatabasePropertyElement property : msg.getPropsList()) {
        final Meta.DatabaseProperty dbProp = Meta.DatabaseProperty.fromProto(property.getKey());
        final Common.TypedValue value = property.getValue();

        Object obj;
        switch (dbProp) {
        // Just need to keep parity with the exposed values on DatabaseProperty
        case GET_NUMERIC_FUNCTIONS:
        case GET_STRING_FUNCTIONS:
        case GET_SYSTEM_FUNCTIONS:
        case GET_TIME_DATE_FUNCTIONS:
        case GET_S_Q_L_KEYWORDS:
          // String
          if (Common.Rep.STRING != value.getType()) {
            throw new IllegalArgumentException("Expected STRING, but got " + value.getType());
          }

          obj = value.getStringValue();
          break;
        case GET_DEFAULT_TRANSACTION_ISOLATION:
          // int
          if (Common.Rep.INTEGER != value.getType()) {
            throw new IllegalArgumentException("Expected INTEGER, but got " + value.getType());
          }

          obj = (int) value.getNumberValue();
          break;
        default:
          switch (value.getType()) {
          case INTEGER:
            obj = Long.valueOf(value.getNumberValue()).intValue();
            break;
          case STRING:
            obj = value.getStringValue();
            break;
          default:
            throw new IllegalArgumentException("Unhandled value type, " + value.getType());
          }

          break;
        }

        properties.put(dbProp, obj);
      }

      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new DatabasePropertyResponse(properties, metadata);
    }

    @Override Responses.DatabasePropertyResponse serialize() {
      Responses.DatabasePropertyResponse.Builder builder = Responses.DatabasePropertyResponse
          .newBuilder();

      if (null != map) {
        for (Entry<Meta.DatabaseProperty, Object> entry : map.entrySet()) {
          Object obj = entry.getValue();

          Common.TypedValue.Builder valueBuilder = Common.TypedValue.newBuilder();
          switch (entry.getKey()) {
          // Just need to keep parity with the exposed values on DatabaseProperty
          case GET_NUMERIC_FUNCTIONS:
          case GET_STRING_FUNCTIONS:
          case GET_SYSTEM_FUNCTIONS:
          case GET_TIME_DATE_FUNCTIONS:
          case GET_S_Q_L_KEYWORDS:
            // String
            if (!(obj instanceof String)) {
              throw new RuntimeException("Expected a String, but got " + obj.getClass());
            }

            valueBuilder.setType(Common.Rep.STRING).setStringValue((String) obj);
            break;
          case GET_DEFAULT_TRANSACTION_ISOLATION:
            // int
            if (!(obj instanceof Integer)) {
              throw new RuntimeException("Expected an Integer, but got " + obj.getClass());
            }

            valueBuilder.setType(Common.Rep.INTEGER).setNumberValue(((Integer) obj).longValue());
            break;
          default:
            if (obj instanceof Integer) {
              valueBuilder.setType(Common.Rep.INTEGER).setNumberValue((Integer) obj);
            } else {
              String value;
              if (obj instanceof String) {
                value = (String) obj;
              } else {
                value = obj.toString();
              }
              valueBuilder.setType(Common.Rep.STRING).setStringValue(value);
            }

            break;
          }

          builder.addProps(Responses.DatabasePropertyElement.newBuilder()
              .setKey(entry.getKey().toProto()).setValue(valueBuilder.build()));
        }
      }

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, map);
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof DatabasePropertyResponse
          && Objects.equals(map, ((DatabasePropertyResponse) o).map)
          && Objects.equals(rpcMetadata, ((DatabasePropertyResponse) o).rpcMetadata);
    }
  }

  /**
   * Response for any request that the server failed to successfully perform.
   * It is used internally by the transport layers to format errors for
   * transport over the wire. Thus, {@link Service#apply} will never return
   * an ErrorResponse.
   */
  public class ErrorResponse extends Response {
    private static final FieldDescriptor ERROR_MESSAGE_DESCRIPTOR = Responses.ErrorResponse
        .getDescriptor().findFieldByNumber(
            Responses.ErrorResponse.ERROR_MESSAGE_FIELD_NUMBER);
    private static final FieldDescriptor SQL_DESCRIPTOR = Responses.ErrorResponse
        .getDescriptor().findFieldByNumber(
            Responses.ErrorResponse.SQL_STATE_FIELD_NUMBER);
    private static final FieldDescriptor SEVERITY_DESCRIPTOR = Responses.ErrorResponse
        .getDescriptor().findFieldByNumber(
            Responses.ErrorResponse.SEVERITY_FIELD_NUMBER);
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.ErrorResponse
        .getDescriptor().findFieldByNumber(
            Responses.ErrorResponse.METADATA_FIELD_NUMBER);

    public static final int UNKNOWN_ERROR_CODE = -1;
    public static final int MISSING_CONNECTION_ERROR_CODE = 1;
    public static final int UNAUTHORIZED_ERROR_CODE = 2;

    public static final String UNKNOWN_SQL_STATE = "00000";
    public static final String UNAUTHORIZED_SQL_STATE = "00002";

    public final List<String> exceptions;
    public final String errorMessage;
    public final int errorCode;
    public final String sqlState;
    public final AvaticaSeverity severity;
    public final RpcMetadataResponse rpcMetadata;

    ErrorResponse() {
      exceptions = Collections.singletonList("Unhandled exception");
      errorMessage = "Unknown message";
      errorCode = -1;
      sqlState = UNKNOWN_SQL_STATE;
      severity = AvaticaSeverity.UNKNOWN;
      rpcMetadata = null;
    }

    @JsonCreator
    public ErrorResponse(@JsonProperty("exceptions") List<String> exceptions,
        @JsonProperty("errorMessage") String errorMessage,
        @JsonProperty("errorCode") int errorCode,
        @JsonProperty("sqlState") String sqlState,
        @JsonProperty("severity") AvaticaSeverity severity,
        @JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.exceptions = exceptions;
      this.errorMessage = errorMessage;
      this.errorCode = errorCode;
      this.sqlState = sqlState;
      this.severity = severity;
      this.rpcMetadata = rpcMetadata;
    }

    protected ErrorResponse(Exception e, String errorMessage, int code, String sqlState,
        AvaticaSeverity severity, RpcMetadataResponse rpcMetadata) {
      this(errorMessage, code, sqlState, severity, toStackTraces(e), rpcMetadata);
    }

    protected ErrorResponse(String errorMessage, int code, String sqlState,
        AvaticaSeverity severity, List<String> exceptions, RpcMetadataResponse rpcMetadata) {
      this.exceptions = exceptions;
      this.errorMessage = errorMessage;
      this.errorCode = code;
      this.sqlState = sqlState;
      this.severity = severity;
      this.rpcMetadata = rpcMetadata;
    }

    static List<String> toStackTraces(Exception e) {
      List<String> stackTraces = new ArrayList<>();
      stackTraces.add(toString(e));
      if (e instanceof SQLException) {
        SQLException next = ((SQLException) e).getNextException();
        while (null != next) {
          stackTraces.add(toString(next));
          next = next.getNextException();
        }
      }
      return stackTraces;
    }

    static String toString(Exception e) {
      //noinspection ThrowableResultOfMethodCallIgnored
      Objects.requireNonNull(e);
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      return sw.toString();
    }

    @Override ErrorResponse deserialize(Message genericMsg) {
      final Responses.ErrorResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.ErrorResponse.class);
      List<String> exceptions = null;
      if (msg.getHasExceptions()) {
        exceptions = msg.getExceptionsList();
      }

      String errorMessage = null;
      if (msg.hasField(ERROR_MESSAGE_DESCRIPTOR)) {
        errorMessage = msg.getErrorMessage();
      }

      String sqlState = null;
      if (msg.hasField(SQL_DESCRIPTOR)) {
        sqlState = msg.getSqlState();
      }

      AvaticaSeverity severity = null;
      if (msg.hasField(SEVERITY_DESCRIPTOR)) {
        severity = AvaticaSeverity.fromProto(msg.getSeverity());
      }

      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new ErrorResponse(exceptions, errorMessage, msg.getErrorCode(), sqlState, severity,
          metadata);
    }

    // Public so the Jetty handler implementations can use it
    @Override public Responses.ErrorResponse serialize() {
      Responses.ErrorResponse.Builder builder = Responses.ErrorResponse.newBuilder();

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      if (null != exceptions) {
        builder.setHasExceptions(true);
        builder.addAllExceptions(exceptions);
      } else {
        builder.setHasExceptions(false);
      }

      if (null != errorMessage) {
        builder.setErrorMessage(errorMessage);
      }

      if (null != sqlState) {
        builder.setSqlState(sqlState);
      }

      if (null != severity) {
        builder.setSeverity(severity.toProto());
      }

      return builder.setErrorCode(errorCode).build();
    }

    @Override public String toString() {
      StringBuilder sb = new StringBuilder(32);
      sb.append("ErrorResponse[errorCode=").append(errorCode)
          .append(", sqlState=").append(sqlState)
          .append(", severity=").append(severity)
          .append(", errorMessage=").append(errorMessage)
          .append(", exceptions=").append(exceptions);
      return sb.toString();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, exceptions);
      result = p(result, errorMessage);
      result = p(result, errorCode);
      result = p(result, sqlState);
      result = p(result, severity);
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof ErrorResponse
          && errorCode == ((ErrorResponse) o).errorCode
          && severity == ((ErrorResponse) o).severity
          && Objects.equals(exceptions, ((ErrorResponse) o).exceptions)
          && Objects.equals(errorMessage, ((ErrorResponse) o).errorMessage)
          && Objects.equals(sqlState, ((ErrorResponse) o).sqlState)
          && Objects.equals(rpcMetadata, ((ErrorResponse) o).rpcMetadata);
    }

    public AvaticaClientRuntimeException toException() {
      return new AvaticaClientRuntimeException("Remote driver error: " + errorMessage, errorCode,
          sqlState, severity, exceptions, rpcMetadata);
    }
  }

  /**
   * Request for {@link Service#apply(SyncResultsRequest)}
   */
  class SyncResultsRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.SyncResultsRequest
        .getDescriptor().findFieldByNumber(Requests.SyncResultsRequest.CONNECTION_ID_FIELD_NUMBER);
    private static final FieldDescriptor STATEMENT_ID_DESCRIPTOR = Requests.SyncResultsRequest
        .getDescriptor().findFieldByNumber(Requests.SyncResultsRequest.STATEMENT_ID_FIELD_NUMBER);
    private static final FieldDescriptor STATE_DESCRIPTOR = Requests.SyncResultsRequest
        .getDescriptor().findFieldByNumber(Requests.SyncResultsRequest.STATE_FIELD_NUMBER);
    private static final FieldDescriptor OFFSET_DESCRIPTOR = Requests.SyncResultsRequest
        .getDescriptor().findFieldByNumber(Requests.SyncResultsRequest.OFFSET_FIELD_NUMBER);
    public final String connectionId;
    public final int statementId;
    public final QueryState state;
    public final long offset;

    SyncResultsRequest() {
      this.connectionId = null;
      this.statementId = 0;
      this.state = null;
      this.offset = 0L;
    }

    public SyncResultsRequest(@JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId, @JsonProperty("state") QueryState state,
        @JsonProperty("offset") long offset) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.state = state;
      this.offset = offset;
    }

    SyncResultsResponse accept(Service service) {
      return service.apply(this);
    }

    Request deserialize(Message genericMsg) {
      final Requests.SyncResultsRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.SyncResultsRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      int statementId = 0;
      if (msg.hasField(STATEMENT_ID_DESCRIPTOR)) {
        statementId = msg.getStatementId();
      }

      Common.QueryState state = null;
      if (msg.hasField(STATE_DESCRIPTOR)) {
        state = msg.getState();
      }

      long offset = 0;
      if (msg.hasField(OFFSET_DESCRIPTOR)) {
        offset = msg.getOffset();
      }

      return new SyncResultsRequest(connectionId, statementId,
          null == state ? null : QueryState.fromProto(msg.getState()), offset);
    }

    Requests.SyncResultsRequest serialize() {
      Requests.SyncResultsRequest.Builder builder = Requests.SyncResultsRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      if (null != state) {
        builder.setState(state.toProto());
      }

      builder.setStatementId(statementId);
      builder.setOffset(offset);

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, offset);
      result = p(result, state);
      result = p(result, statementId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof SyncResultsRequest
          && statementId == ((SyncResultsRequest) o).statementId
          && offset == ((SyncResultsRequest) o).offset
          && Objects.equals(connectionId, ((SyncResultsRequest) o).connectionId)
          && Objects.equals(state, ((SyncResultsRequest) o).state);
    }
  }

  /**
   * Response for {@link Service#apply(SyncResultsRequest)}.
   */
  class SyncResultsResponse extends Response {
    private static final FieldDescriptor METADATA_DESCRIPTOR = Responses.SyncResultsResponse
        .getDescriptor().findFieldByNumber(Responses.SyncResultsResponse.METADATA_FIELD_NUMBER);
    public boolean missingStatement = false;
    public final boolean moreResults;
    public final RpcMetadataResponse rpcMetadata;

    SyncResultsResponse() {
      this.moreResults = false;
      this.rpcMetadata = null;
    }

    public SyncResultsResponse(@JsonProperty("moreResults") boolean moreResults,
        @JsonProperty("missingStatement") boolean missingStatement,
        @JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.moreResults = moreResults;
      this.missingStatement = missingStatement;
      this.rpcMetadata = rpcMetadata;
    }

    SyncResultsResponse deserialize(Message genericMsg) {
      final Responses.SyncResultsResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.SyncResultsResponse.class);

      RpcMetadataResponse metadata = null;
      if (msg.hasField(METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new SyncResultsResponse(msg.getMoreResults(), msg.getMissingStatement(), metadata);
    }

    Responses.SyncResultsResponse serialize() {
      Responses.SyncResultsResponse.Builder builder = Responses.SyncResultsResponse.newBuilder();

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.setMoreResults(moreResults).setMissingStatement(missingStatement).build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, missingStatement);
      result = p(result, moreResults);
      result = p(result, rpcMetadata);
      return result;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof SyncResultsResponse
          && missingStatement == ((SyncResultsResponse) o).missingStatement
          && moreResults == ((SyncResultsResponse) o).moreResults
          && Objects.equals(rpcMetadata, ((SyncResultsResponse) o).rpcMetadata);
    }
  }

  /**
   * Response that includes information about the server that handled an RPC.
   *
   * This isn't really a "response", but we want to be able to be able to convert it to protobuf
   * and back again, so ignore that there isn't an explicit endpoint for it.
   */
  public class RpcMetadataResponse extends Response {
    private static final FieldDescriptor SERVER_ADDRESS_DESCRIPTOR = Responses.RpcMetadata
        .getDescriptor().findFieldByNumber(Responses.RpcMetadata.SERVER_ADDRESS_FIELD_NUMBER);
    public final String serverAddress;
    private final ByteString serverAddressAsBytes;

    public RpcMetadataResponse() {
      this.serverAddress = null;
      this.serverAddressAsBytes = null;
    }

    public RpcMetadataResponse(@JsonProperty("serverAddress") String serverAddress) {
      this.serverAddress = serverAddress;
      this.serverAddressAsBytes = UnsafeByteOperations.unsafeWrap(serverAddress.getBytes(UTF_8));
    }

    @Override RpcMetadataResponse deserialize(Message genericMsg) {
      final Responses.RpcMetadata msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.RpcMetadata.class);

      return fromProto(msg);
    }

    @Override Responses.RpcMetadata serialize() {
      return Responses.RpcMetadata.newBuilder().setServerAddressBytes(serverAddressAsBytes).build();
    }

    static RpcMetadataResponse fromProto(Responses.RpcMetadata msg) {
      String serverAddress = null;
      if (msg.hasField(SERVER_ADDRESS_DESCRIPTOR)) {
        serverAddress = msg.getServerAddress();
      }

      return new RpcMetadataResponse(serverAddress);
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, serverAddress);
      return result;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof RpcMetadataResponse
          && Objects.equals(serverAddress, ((RpcMetadataResponse) o).serverAddress);
    }
  }

  /**
   * An RPC request to invoke a commit on a Connection.
   */
  class CommitRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.CommitRequest
        .getDescriptor().findFieldByNumber(Requests.CommitRequest.CONNECTION_ID_FIELD_NUMBER);
    public final String connectionId;

    CommitRequest() {
      this.connectionId = null;
    }

    public CommitRequest(@JsonProperty("connectionId") String connectionId) {
      this.connectionId = connectionId;
    }

    @Override CommitResponse accept(Service service) {
      return service.apply(this);
    }

    @Override CommitRequest deserialize(Message genericMsg) {
      final Requests.CommitRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.CommitRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      return new CommitRequest(connectionId);
    }

    @Override Requests.CommitRequest serialize() {
      Requests.CommitRequest.Builder builder = Requests.CommitRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof CommitRequest
          && Objects.equals(connectionId, ((CommitRequest) o).connectionId);
    }
  }

  /**
   * An RPC response from invoking commit on a Connection.
   */
  class CommitResponse extends Response {
    private static final CommitResponse INSTANCE = new CommitResponse();
    private static final Responses.CommitResponse PB_INSTANCE =
        Responses.CommitResponse.getDefaultInstance();

    CommitResponse() {}

    @Override CommitResponse deserialize(Message genericMsg) {
      // Checks the type of genericMsg
      ProtobufService.castProtobufMessage(genericMsg, Responses.CommitResponse.class);

      return INSTANCE;
    }

    @Override Responses.CommitResponse serialize() {
      return PB_INSTANCE;
    }

    @Override public int hashCode() {
      return 1;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof CommitResponse;
    }
  }

  /**
   * An RPC request to invoke a rollback on a Connection.
   */
  class RollbackRequest extends Request {
    private static final FieldDescriptor CONNECTION_ID_DESCRIPTOR = Requests.RollbackRequest
        .getDescriptor().findFieldByNumber(Requests.RollbackRequest.CONNECTION_ID_FIELD_NUMBER);
    public final String connectionId;

    RollbackRequest() {
      this.connectionId = null;
    }

    public RollbackRequest(@JsonProperty("connectionId") String connectionId) {
      this.connectionId = connectionId;
    }

    @Override RollbackResponse accept(Service service) {
      return service.apply(this);
    }

    @Override RollbackRequest deserialize(Message genericMsg) {
      final Requests.RollbackRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.RollbackRequest.class);

      String connectionId = null;
      if (msg.hasField(CONNECTION_ID_DESCRIPTOR)) {
        connectionId = msg.getConnectionId();
      }

      return new RollbackRequest(connectionId);
    }

    @Override Requests.RollbackRequest serialize() {
      Requests.RollbackRequest.Builder builder = Requests.RollbackRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      return result;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof RollbackRequest
          && Objects.equals(connectionId, ((RollbackRequest) o).connectionId);
    }
  }

  /**
   * An RPC response from invoking rollback on a Connection.
   */
  class RollbackResponse extends Response {
    private static final RollbackResponse INSTANCE = new RollbackResponse();
    private static final Responses.RollbackResponse PB_INSTANCE =
        Responses.RollbackResponse.getDefaultInstance();

    RollbackResponse() {}

    @Override RollbackResponse deserialize(Message genericMsg) {
      // Check that genericMsg is the expected type
      ProtobufService.castProtobufMessage(genericMsg, Responses.RollbackResponse.class);
      return INSTANCE;
    }

    @Override Responses.RollbackResponse serialize() {
      return PB_INSTANCE;
    }

    @Override public int hashCode() {
      return 1;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof RollbackResponse;
    }
  }

  /**
   * Request to prepare a statement and execute a series of batch commands in one call.
   */
  class PrepareAndExecuteBatchRequest extends Request {
    public final String connectionId;
    public final List<String> sqlCommands;
    public final int statementId;

    PrepareAndExecuteBatchRequest() {
      connectionId = null;
      statementId = 0;
      sqlCommands = null;
    }

    @JsonCreator
    public PrepareAndExecuteBatchRequest(@JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId, @JsonProperty("sqlCommands") List<String>
        sqlCommands) {
      this.connectionId = connectionId;
      this.sqlCommands = sqlCommands;
      this.statementId = statementId;
    }

    @Override public ExecuteBatchResponse accept(Service service) {
      return service.apply(this);
    }

    @Override public Requests.PrepareAndExecuteBatchRequest serialize() {
      Requests.PrepareAndExecuteBatchRequest.Builder builder =
          Requests.PrepareAndExecuteBatchRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      if (null != sqlCommands) {
        builder.addAllSqlCommands(sqlCommands);
      }

      return builder.setStatementId(statementId).build();
    }

    @Override public PrepareAndExecuteBatchRequest deserialize(Message genericMsg) {
      final Requests.PrepareAndExecuteBatchRequest msg =
          ProtobufService.castProtobufMessage(genericMsg,
              Requests.PrepareAndExecuteBatchRequest.class);

      List<String> sqlCommands = new ArrayList<>(msg.getSqlCommandsList());

      return new PrepareAndExecuteBatchRequest(msg.getConnectionId(), msg.getStatementId(),
          sqlCommands);
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, statementId);
      result = p(result, sqlCommands);
      return result;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof PrepareAndExecuteBatchRequest
          && Objects.equals(connectionId, ((PrepareAndExecuteBatchRequest) o).connectionId)
          && statementId == ((PrepareAndExecuteBatchRequest) o).statementId
          && Objects.equals(sqlCommands, ((PrepareAndExecuteBatchRequest) o).sqlCommands);
    }
  }

  /**
   * Request object to execute a batch of commands.
   */
  class ExecuteBatchRequest extends Request {
    public final String connectionId;
    public final int statementId;
    // Each update in a batch has a list of TypedValue's
    public final List<List<TypedValue>> parameterValues;
    // Avoid deserializing every parameter list from pb to pojo
    @JsonIgnore
    private List<Requests.UpdateBatch> protoParameterValues = null;

    ExecuteBatchRequest() {
      this.connectionId = null;
      this.statementId = 0;
      this.parameterValues = null;
    }

    @JsonCreator
    public ExecuteBatchRequest(@JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId,
        @JsonProperty("parameterValues") List<List<TypedValue>> parameterValues) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.parameterValues = parameterValues;
    }

    ExecuteBatchRequest(String connectionId, int statementId) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.parameterValues = null;
    }

    /**
     * Does this instance contain protobuf update batches.
     * @return True if <code>protoUpdateBatches</code> is non-null.
     */
    public boolean hasProtoUpdateBatches() {
      return null != protoParameterValues;
    }

    /**
     * @return The protobuf update batches.
     */
    // JsonIgnore on the getter, otherwise Jackson will try to serialize it
    @JsonIgnore
    public List<Requests.UpdateBatch> getProtoUpdateBatches() {
      return protoParameterValues;
    }

    @Override public ExecuteBatchResponse accept(Service service) {
      return service.apply(this);
    }

    @Override ExecuteBatchRequest deserialize(Message genericMsg) {
      Requests.ExecuteBatchRequest msg = ProtobufService.castProtobufMessage(genericMsg,
          Requests.ExecuteBatchRequest.class);

      List<Requests.UpdateBatch> updateBatches = msg.getUpdatesList();

      ExecuteBatchRequest pojo =
          new ExecuteBatchRequest(msg.getConnectionId(), msg.getStatementId());
      pojo.protoParameterValues = updateBatches;
      return pojo;
    }

    @Override Requests.ExecuteBatchRequest serialize() {
      Requests.ExecuteBatchRequest.Builder builder = Requests.ExecuteBatchRequest.newBuilder();

      if (hasProtoUpdateBatches()) {
        builder.addAllUpdates(protoParameterValues);
      } else if (null != parameterValues) {
        for (List<TypedValue> updateBatch : parameterValues) {
          Requests.UpdateBatch.Builder batchBuilder = Requests.UpdateBatch.newBuilder();
          for (TypedValue update : updateBatch) {
            batchBuilder.addParameterValues(update.toProto());
          }
          builder.addUpdates(batchBuilder.build());
        }
      }

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      return builder.setStatementId(statementId).build();
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, statementId);
      result = p(result, parameterValues);
      return result;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof ExecuteBatchRequest
          && Objects.equals(connectionId, ((ExecuteBatchRequest) o).connectionId)
          && statementId == ((ExecuteBatchRequest) o).statementId
          && Objects.equals(protoParameterValues, ((ExecuteBatchRequest) o).protoParameterValues)
          && Objects.equals(parameterValues, ((ExecuteBatchRequest) o).parameterValues);
    }
  }

  /**
   * Response object for executing a batch of commands.
   */
  class ExecuteBatchResponse extends Response {
    private static final FieldDescriptor RPC_METADATA_DESCRIPTOR = Responses.ExecuteBatchResponse
        .getDescriptor().findFieldByNumber(Responses.ExecuteBatchResponse.METADATA_FIELD_NUMBER);

    public final String connectionId;
    public final int statementId;
    public final long[] updateCounts;
    public final boolean missingStatement;
    public final RpcMetadataResponse rpcMetadata;

    ExecuteBatchResponse() {
      connectionId = null;
      statementId = 0;
      updateCounts = null;
      missingStatement = false;
      rpcMetadata = null;
    }

    @JsonCreator
    public ExecuteBatchResponse(@JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId,
        @JsonProperty("updateCounts") long[] updateCounts,
        @JsonProperty("missingStatement") boolean missingStatement,
        @JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.updateCounts = updateCounts;
      this.missingStatement = missingStatement;
      this.rpcMetadata = rpcMetadata;
    }

    @Override public int hashCode() {
      int result = 1;
      result = p(result, connectionId);
      result = p(result, statementId);
      result = p(result, updateCounts);
      result = p(result, missingStatement);
      return result;
    }

    @Override public boolean equals(Object o) {
      return this == o
          || o instanceof ExecuteBatchResponse
          && Arrays.equals(updateCounts, ((ExecuteBatchResponse) o).updateCounts)
          && Objects.equals(connectionId, ((ExecuteBatchResponse) o).connectionId)
          && statementId == ((ExecuteBatchResponse) o).statementId
          && missingStatement == ((ExecuteBatchResponse) o).missingStatement;
    }

    @Override ExecuteBatchResponse deserialize(Message genericMsg) {
      Responses.ExecuteBatchResponse msg = ProtobufService.castProtobufMessage(genericMsg,
          Responses.ExecuteBatchResponse.class);

      long[] updateCounts = new long[msg.getUpdateCountsCount()];
      int i = 0;
      for (Long updateCount : msg.getUpdateCountsList()) {
        updateCounts[i++] = updateCount;
      }

      RpcMetadataResponse metadata = null;
      if (msg.hasField(RPC_METADATA_DESCRIPTOR)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new ExecuteBatchResponse(msg.getConnectionId(), msg.getStatementId(), updateCounts,
          msg.getMissingStatement(), metadata);
    }

    @Override Responses.ExecuteBatchResponse serialize() {
      Responses.ExecuteBatchResponse.Builder builder = Responses.ExecuteBatchResponse.newBuilder();

      if (null != updateCounts) {
        for (int i = 0; i < updateCounts.length; i++) {
          builder.addUpdateCounts(updateCounts[i]);
        }
      }

      if (null != rpcMetadata) {
        builder.setMetadata(rpcMetadata.serialize());
      }

      return builder.setConnectionId(connectionId).setStatementId(statementId).build();
    }
  }
}

// End Service.java
