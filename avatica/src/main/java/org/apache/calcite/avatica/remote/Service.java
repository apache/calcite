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
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.proto.Responses;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
  ExecuteResponse apply(PrepareAndExecuteRequest request);
  FetchResponse apply(FetchRequest request);
  CreateStatementResponse apply(CreateStatementRequest request);
  CloseStatementResponse apply(CloseStatementRequest request);
  CloseConnectionResponse apply(CloseConnectionRequest request);
  ConnectionSyncResponse apply(ConnectionSyncRequest request);
  DatabasePropertyResponse apply(DatabasePropertyRequest request);

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
      @JsonSubTypes.Type(value = TableTypesRequest.class, name = "getTableTypes"),
      @JsonSubTypes.Type(value = TypeInfoRequest.class, name = "getTypeInfo"),
      @JsonSubTypes.Type(value = ColumnsRequest.class, name = "getColumns"),
      @JsonSubTypes.Type(value = PrepareRequest.class, name = "prepare"),
      @JsonSubTypes.Type(value = PrepareAndExecuteRequest.class,
          name = "prepareAndExecute"),
      @JsonSubTypes.Type(value = FetchRequest.class, name = "fetch"),
      @JsonSubTypes.Type(value = CreateStatementRequest.class,
          name = "createStatement"),
      @JsonSubTypes.Type(value = CloseStatementRequest.class,
          name = "closeStatement"),
      @JsonSubTypes.Type(value = CloseConnectionRequest.class,
          name = "closeConnection"),
      @JsonSubTypes.Type(value = ConnectionSyncRequest.class, name = "connectionSync"),
      @JsonSubTypes.Type(value = DatabasePropertyRequest.class, name = "databaseProperties") })
  abstract class Request {
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
      @JsonSubTypes.Type(value = DatabasePropertyResponse.class, name = "databaseProperties") })
  abstract class Response {
    abstract Response deserialize(Message genericMsg);
    abstract Message serialize();
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#getCatalogs()}. */
  class CatalogsRequest extends Request {
    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }

    @Override CatalogsRequest deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Requests.CatalogsRequest)) {
        throw new IllegalArgumentException(
            "Expected CatalogsRequest, but got " + genericMsg.getClass().getName());
      }

      // No state to set
      return new CatalogsRequest();
    }

    @Override Requests.CatalogsRequest serialize() {
      return Requests.CatalogsRequest.newBuilder().build();
    }

    @Override public int hashCode() {
      return 0;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      return o instanceof CatalogsRequest;
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#getDatabaseProperties()}. */
  class DatabasePropertyRequest extends Request {
    @JsonCreator
    public DatabasePropertyRequest() {
    }

    DatabasePropertyResponse accept(Service service) {
      return service.apply(this);
    }

    @Override DatabasePropertyRequest deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Requests.DatabasePropertyRequest)) {
        throw new IllegalArgumentException(
            "Expected DatabasePropertyRequest, but got " + genericMsg.getClass().getName());
      }

      return new DatabasePropertyRequest();
    }

    @Override Requests.DatabasePropertyRequest serialize() {
      return Requests.DatabasePropertyRequest.newBuilder().build();
    }

    @Override public int hashCode() {
      return 0;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      return o instanceof DatabasePropertyRequest;
    }
  }
  /** Request for
   * {@link Meta#getSchemas(String, org.apache.calcite.avatica.Meta.Pat)}. */
  class SchemasRequest extends Request {
    public final String catalog;
    public final String schemaPattern;

    SchemasRequest() {
      catalog = null;
      schemaPattern = null;
    }

    @JsonCreator
    public SchemasRequest(@JsonProperty("catalog") String catalog,
        @JsonProperty("schemaPattern") String schemaPattern) {
      this.catalog = catalog;
      this.schemaPattern = schemaPattern;
    }

    ResultSetResponse accept(Service service) {
      return service.apply(this);
    }

    @Override SchemasRequest deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Requests.SchemasRequest)) {
        throw new IllegalArgumentException(
            "Expected SchemasRequest, but got" + genericMsg.getClass().getName());
      }

      final Requests.SchemasRequest msg = (Requests.SchemasRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String catalog = null;
      if (ProtobufService.hasField(msg, desc, Requests.SchemasRequest.CATALOG_FIELD_NUMBER)) {
        catalog = msg.getCatalog();
      }

      String schemaPattern = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.SchemasRequest.SCHEMA_PATTERN_FIELD_NUMBER)) {
        schemaPattern = msg.getSchemaPattern();
      }

      return new SchemasRequest(catalog, schemaPattern);
    }

    @Override Requests.SchemasRequest serialize() {
      Requests.SchemasRequest.Builder builder = Requests.SchemasRequest.newBuilder();
      if (null != catalog) {
        builder.setCatalog(catalog);
      }
      if (null != schemaPattern) {
        builder.setSchemaPattern(schemaPattern);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
      result = prime * result + ((schemaPattern == null) ? 0 : schemaPattern.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof SchemasRequest) {
        SchemasRequest other = (SchemasRequest) o;

        if (null == catalog) {
          // We're null, other is not
          if (null != other.catalog) {
            return false;
          }
        } else if (!catalog.equals(other.catalog)) {
          return false;
        }

        if (null == schemaPattern) {
          // We're null, they're not
          if (null != other.schemaPattern) {
            return false;
          }
        } else if (!catalog.equals(other.catalog)) {
          return false;
        }

        return true;
      }

      return false;
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

    TablesRequest() {
      catalog = null;
      schemaPattern = null;
      tableNamePattern = null;
      typeList = null;
    }

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

    @Override Request deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Requests.TablesRequest)) {
        throw new IllegalArgumentException(
            "Expected TablesRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.TablesRequest msg = (Requests.TablesRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String catalog = null;
      if (ProtobufService.hasField(msg, desc, Requests.TablesRequest.CATALOG_FIELD_NUMBER)) {
        catalog = msg.getCatalog();
      }

      String schemaPattern = null;
      if (ProtobufService.hasField(msg, desc, Requests.TablesRequest.SCHEMA_PATTERN_FIELD_NUMBER)) {
        schemaPattern = msg.getSchemaPattern();
      }

      String tableNamePattern = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.TablesRequest.TABLE_NAME_PATTERN_FIELD_NUMBER)) {
        tableNamePattern = msg.getTableNamePattern();
      }

      // Cannot determine if a value was set for a repeated field. Must use an extra boolean
      // parameter to distinguish an empty and null typeList.
      List<String> typeList = null;
      if (msg.getHasTypeList()) {
        typeList = msg.getTypeListList();
      }

      return new TablesRequest(catalog, schemaPattern, tableNamePattern, typeList);
    }

    @Override Requests.TablesRequest serialize() {
      Requests.TablesRequest.Builder builder = Requests.TablesRequest.newBuilder();

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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
      result = prime * result + ((schemaPattern == null) ? 0 : schemaPattern.hashCode());
      result = prime * result + ((tableNamePattern == null) ? 0 : tableNamePattern.hashCode());
      result = prime * result + ((typeList == null) ? 0 : typeList.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof TablesRequest) {
        TablesRequest other = (TablesRequest) o;

        if (null == catalog) {
          if (null != catalog) {
            return false;
          }
        } else if (!catalog.equals(other.catalog)) {
          return false;
        }

        if (null == schemaPattern) {
          if (null != other.schemaPattern) {
            return false;
          }
        } else if (!schemaPattern.equals(other.schemaPattern)) {
          return false;
        }

        if (null == tableNamePattern) {
          if (null != other.tableNamePattern) {
            return false;
          }
        } else if (!tableNamePattern.equals(other.tableNamePattern)) {
          return false;
        }

        if (null == typeList) {
          if (null != other.typeList) {
            return false;
          }
        } else if (null == other.typeList || !typeList.equals(other.typeList)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /**
   * Request for {@link Meta#getTableTypes()}.
   */
  class TableTypesRequest extends Request {
    @Override ResultSetResponse accept(Service service) {
      return service.apply(this);
    }

    @Override TableTypesRequest deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Requests.TableTypesRequest)) {
        throw new IllegalArgumentException(
            "Expected TableTypesRequest, but got " + genericMsg.getClass().getName());
      }

      return new TableTypesRequest();
    }

    @Override Requests.TableTypesRequest serialize() {
      return Requests.TableTypesRequest.newBuilder().build();
    }

    @Override public int hashCode() {
      return 0;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      return o instanceof TableTypesRequest;
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

    ColumnsRequest() {
      catalog = null;
      schemaPattern = null;
      tableNamePattern = null;
      columnNamePattern = null;
    }

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

    @Override ColumnsRequest deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Requests.ColumnsRequest)) {
        throw new IllegalArgumentException(
            "Expected ColumnsRequest, but got" + genericMsg.getClass().getName());
      }

      final Requests.ColumnsRequest msg = (Requests.ColumnsRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String catalog = null;
      if (ProtobufService.hasField(msg, desc, Requests.ColumnsRequest.CATALOG_FIELD_NUMBER)) {
        catalog = msg.getCatalog();
      }

      String schemaPattern = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.ColumnsRequest.SCHEMA_PATTERN_FIELD_NUMBER)) {
        schemaPattern = msg.getSchemaPattern();
      }

      String tableNamePattern = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.ColumnsRequest.TABLE_NAME_PATTERN_FIELD_NUMBER)) {
        tableNamePattern = msg.getTableNamePattern();
      }

      String columnNamePattern = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.ColumnsRequest.COLUMN_NAME_PATTERN_FIELD_NUMBER)) {
        columnNamePattern = msg.getColumnNamePattern();
      }

      return new ColumnsRequest(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    @Override Requests.ColumnsRequest serialize() {
      Requests.ColumnsRequest.Builder builder = Requests.ColumnsRequest.newBuilder();

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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
      result = prime * result + ((columnNamePattern == null) ? 0 : columnNamePattern.hashCode());
      result = prime * result + ((schemaPattern == null) ? 0 : schemaPattern.hashCode());
      result = prime * result + ((tableNamePattern == null) ? 0 : tableNamePattern.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof ColumnsRequest) {
        ColumnsRequest other = (ColumnsRequest) o;

        if (null == catalog) {
          if (null != other.catalog) {
            return false;
          }
        } else if (!catalog.equals(other.catalog)) {
          return false;
        }

        if (null == schemaPattern) {
          if (null != other.schemaPattern) {
            return false;
          }
        } else if (!schemaPattern.equals(other.schemaPattern)) {
          return false;
        }

        if (null == tableNamePattern) {
          if (null != other.tableNamePattern) {
            return false;
          }
        } else if (!tableNamePattern.equals(other.tableNamePattern)) {
          return false;
        }

        if (null == columnNamePattern) {
          if (null != other.columnNamePattern) {
            return false;
          }
        } else if (!columnNamePattern.equals(other.columnNamePattern)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Request for
   * {@link Meta#getTypeInfo()}. */
  class TypeInfoRequest extends Request {
    @Override ResultSetResponse accept(Service service) {
      return service.apply(this);
    }

    @Override TypeInfoRequest deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Requests.TypeInfoRequest)) {
        throw new IllegalArgumentException(
            "Expected TypeInfoRequest, but got " + genericMsg.getClass().getName());
      }

      return new TypeInfoRequest();
    }

    @Override Requests.TypeInfoRequest serialize() {
      return Requests.TypeInfoRequest.newBuilder().build();
    }

    @Override public int hashCode() {
      return 0;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      return o instanceof TypeInfoRequest;
    }
  }

  /** Response that contains a result set.
   *
   * <p>Regular result sets have {@code updateCount} -1;
   * any other value means a dummy result set that is just a count, and has
   * no signature and no other data.
   *
   * <p>Several types of request, including
   * {@link org.apache.calcite.avatica.Meta#getCatalogs()} and
   * {@link org.apache.calcite.avatica.Meta#getSchemas(String, org.apache.calcite.avatica.Meta.Pat)}
   * {@link Meta#getTables(String, Meta.Pat, Meta.Pat, List)}
   * {@link Meta#getTableTypes()}
   * return this response. */
  class ResultSetResponse extends Response {
    public final String connectionId;
    public final int statementId;
    public final boolean ownStatement;
    public final Meta.Signature signature;
    public final Meta.Frame firstFrame;
    public final long updateCount;

    ResultSetResponse() {
      connectionId = null;
      statementId = 0;
      ownStatement = false;
      signature = null;
      firstFrame = null;
      updateCount = 0;
    }

    @JsonCreator
    public ResultSetResponse(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId,
        @JsonProperty("ownStatement") boolean ownStatement,
        @JsonProperty("signature") Meta.Signature signature,
        @JsonProperty("firstFrame") Meta.Frame firstFrame,
        @JsonProperty("updateCount") long updateCount) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.ownStatement = ownStatement;
      this.signature = signature;
      this.firstFrame = firstFrame;
      this.updateCount = updateCount;
    }

    @Override ResultSetResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.ResultSetResponse)) {
        throw new IllegalArgumentException(
            "Expected ResultSetResponse, but got " + genericMsg.getClass().getName());
      }

      return fromProto((Responses.ResultSetResponse) genericMsg);
    }

    static ResultSetResponse fromProto(Responses.ResultSetResponse msg) {
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Responses.ResultSetResponse.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

      Meta.Signature signature = null;
      if (ProtobufService.hasField(msg, desc, Responses.ResultSetResponse.SIGNATURE_FIELD_NUMBER)) {
        signature = Meta.Signature.fromProto(msg.getSignature());
      }

      Meta.Frame frame = null;
      if (ProtobufService.hasField(msg, desc,
          Responses.ResultSetResponse.FIRST_FRAME_FIELD_NUMBER)) {
        frame = Meta.Frame.fromProto(msg.getFirstFrame());
      }

      return new ResultSetResponse(connectionId, msg.getStatementId(), msg.getOwnStatement(),
          signature, frame, msg.getUpdateCount());
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

      return builder.build();
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + ((firstFrame == null) ? 0 : firstFrame.hashCode());
      result = prime * result + (ownStatement ? 1231 : 1237);
      result = prime * result + ((signature == null) ? 0 : signature.hashCode());
      result = prime * result + statementId;
      result = prime * result + (int) (updateCount ^ (updateCount >>> 32));
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof ResultSetResponse) {
        ResultSetResponse other = (ResultSetResponse) o;

        if (connectionId == null) {
          if (other.connectionId != null) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

        if (firstFrame == null) {
          if (other.firstFrame != null) {
            return false;
          }
        } else if (!firstFrame.equals(other.firstFrame)) {
          return false;
        }

        if (signature == null) {
          if (other.signature != null) {
            return false;
          }
        } else if (!signature.equals(other.signature)) {
          return false;
        }

        return ownStatement == other.ownStatement && statementId == other.statementId
            && updateCount == other.updateCount;
      }

      return false;
    }
  }

  /** Request for
   * {@link Meta#prepareAndExecute(Meta.StatementHandle, String, long, Meta.PrepareCallback)}. */
  class PrepareAndExecuteRequest extends Request {
    public final String connectionId;
    public final String sql;
    public final long maxRowCount;
    public final int statementId;

    PrepareAndExecuteRequest() {
      connectionId = null;
      sql = null;
      maxRowCount = 0;
      statementId = 0;
    }

    @JsonCreator
    public PrepareAndExecuteRequest(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId,
        @JsonProperty("sql") String sql,
        @JsonProperty("maxRowCount") long maxRowCount) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.sql = sql;
      this.maxRowCount = maxRowCount;
    }

    @Override ExecuteResponse accept(Service service) {
      return service.apply(this);
    }

    @Override PrepareAndExecuteRequest deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Requests.PrepareAndExecuteRequest)) {
        throw new IllegalArgumentException(
            "Expected PrepareAndExecuteRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.PrepareAndExecuteRequest msg = (Requests.PrepareAndExecuteRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.PrepareAndExecuteRequest.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

      String sql = null;
      if (ProtobufService.hasField(msg, desc, Requests.PrepareAndExecuteRequest.SQL_FIELD_NUMBER)) {
        sql = msg.getSql();
      }

      return new PrepareAndExecuteRequest(connectionId, msg.getStatementId(), sql,
          msg.getMaxRowCount());
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
      builder.setMaxRowCount(maxRowCount);

      return builder.build();
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + (int) (maxRowCount ^ (maxRowCount >>> 32));
      result = prime * result + ((sql == null) ? 0 : sql.hashCode());
      result = prime * result + statementId;
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof PrepareAndExecuteRequest) {
        PrepareAndExecuteRequest other = (PrepareAndExecuteRequest) o;

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

        if (null == sql) {
          if (null != other.sql) {
            return false;
          }
        } else if (!sql.equals(other.sql)) {
          return false;
        }

        return statementId == other.statementId && maxRowCount == other.maxRowCount;
      }

      return false;
    }
  }

  /** Response to a
   * {@link org.apache.calcite.avatica.remote.Service.PrepareAndExecuteRequest}. */
  class ExecuteResponse extends Response {
    public final List<ResultSetResponse> results;

    ExecuteResponse() {
      results = null;
    }

    @JsonCreator
    public ExecuteResponse(
        @JsonProperty("resultSets") List<ResultSetResponse> results) {
      this.results = results;
    }

    @Override ExecuteResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.ExecuteResponse)) {
        throw new IllegalArgumentException(
            "Expected ExecuteResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.ExecuteResponse msg = (Responses.ExecuteResponse) genericMsg;

      List<Responses.ResultSetResponse> msgResults = msg.getResultsList();
      List<ResultSetResponse> copiedResults = new ArrayList<>(msgResults.size());

      for (Responses.ResultSetResponse msgResult : msgResults) {
        copiedResults.add(ResultSetResponse.fromProto(msgResult));
      }

      return new ExecuteResponse(copiedResults);
    }

    @Override Responses.ExecuteResponse serialize() {
      Responses.ExecuteResponse.Builder builder = Responses.ExecuteResponse.newBuilder();

      for (ResultSetResponse result : results) {
        builder.addResults(result.serialize());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      if (null == results) {
        return 0;
      }

      return results.hashCode();
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof ExecuteResponse) {
        ExecuteResponse other = (ExecuteResponse) o;

        if (null == results) {
          if (null != other.results) {
            return false;
          }
        } else if (!results.equals(other.results)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Request for
   * {@link Meta#prepare(Meta.ConnectionHandle, String, long)}. */
  class PrepareRequest extends Request {
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
      if (!(genericMsg instanceof Requests.PrepareRequest)) {
        throw new IllegalArgumentException(
            "Expected PrepareRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.PrepareRequest msg = (Requests.PrepareRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc, Requests.PrepareRequest.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

      String sql = null;
      if (ProtobufService.hasField(msg, desc, Requests.PrepareRequest.SQL_FIELD_NUMBER)) {
        sql = msg.getSql();
      }

      return new PrepareRequest(connectionId, sql, msg.getMaxRowCount());
    }

    @Override Requests.PrepareRequest serialize() {
      Requests.PrepareRequest.Builder builder = Requests.PrepareRequest.newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      if (null != sql) {
        builder.setSql(sql);
      }

      return builder.setMaxRowCount(maxRowCount).build();
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + (int) (maxRowCount ^ (maxRowCount >>> 32));
      result = prime * result + ((sql == null) ? 0 : sql.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof PrepareRequest) {
        PrepareRequest other = (PrepareRequest) o;

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

        if (null == sql) {
          if (null != other.sql) {
            return false;
          }
        } else if (!sql.equals(other.sql)) {
          return false;
        }

        return maxRowCount == other.maxRowCount;
      }

      return false;
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.PrepareRequest}. */
  class PrepareResponse extends Response {
    public final Meta.StatementHandle statement;

    PrepareResponse() {
      statement = null;
    }

    @JsonCreator
    public PrepareResponse(
        @JsonProperty("statement") Meta.StatementHandle statement) {
      this.statement = statement;
    }

    @Override PrepareResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.PrepareResponse)) {
        throw new IllegalArgumentException(
            "Expected PrepareResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.PrepareResponse msg = (Responses.PrepareResponse) genericMsg;

      return new PrepareResponse(Meta.StatementHandle.fromProto(msg.getStatement()));
    }

    @Override Responses.PrepareResponse serialize() {
      Responses.PrepareResponse.Builder builder = Responses.PrepareResponse.newBuilder();

      if (null != statement) {
        builder.setStatement(statement.toProto());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((statement == null) ? 0 : statement.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof PrepareResponse) {
        PrepareResponse other = (PrepareResponse) o;

        if (statement == null) {
          if (other.statement != null) {
            return false;
          }
        } else if (!statement.equals(other.statement)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Request for
   * {@link Meta#fetch(Meta.StatementHandle, List, long, int)}. */
  class FetchRequest extends Request {
    public final String connectionId;
    public final int statementId;
    public final long offset;
    /** Maximum number of rows to be returned in the frame. Negative means no
     * limit. */
    public final int fetchMaxRowCount;
    /** A list of parameter values, if statement is to be executed; otherwise
     * null. */
    public final List<TypedValue> parameterValues;

    FetchRequest() {
      connectionId = null;
      statementId = 0;
      offset = 0;
      fetchMaxRowCount = 0;
      parameterValues = null;
    }

    @JsonCreator
    public FetchRequest(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId,
        @JsonProperty("parameterValues") List<TypedValue> parameterValues,
        @JsonProperty("offset") long offset,
        @JsonProperty("fetchMaxRowCount") int fetchMaxRowCount) {
      this.connectionId = connectionId;
      this.statementId = statementId;
      this.parameterValues = parameterValues;
      this.offset = offset;
      this.fetchMaxRowCount = fetchMaxRowCount;
    }

    @Override FetchResponse accept(Service service) {
      return service.apply(this);
    }

    @Override FetchRequest deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Requests.FetchRequest)) {
        throw new IllegalArgumentException(
            "Expected FetchRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.FetchRequest msg = (Requests.FetchRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      // Cannot determine if a value was set for a repeated field. Must use an extra boolean
      // parameter to distinguish an empty list and a null list of ParameterValues.
      List<TypedValue> values = null;
      if (msg.getHasParameterValues()) {
        values = new ArrayList<>(msg.getParameterValuesCount());
        for (Common.TypedValue valueProto : msg.getParameterValuesList()) {
          values.add(TypedValue.fromProto(valueProto));
        }
      }

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc, Requests.FetchRequest.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

      return new FetchRequest(connectionId, msg.getStatementId(), values, msg.getOffset(),
          msg.getFetchMaxRowCount());
    }

    @Override Requests.FetchRequest serialize() {
      Requests.FetchRequest.Builder builder = Requests.FetchRequest.newBuilder();

      if (null != parameterValues) {
        builder.setHasParameterValues(true);
        for (TypedValue paramValue : parameterValues) {
          builder.addParameterValues(paramValue.toProto());
        }
      } else {
        builder.setHasParameterValues(false);
      }

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      builder.setStatementId(statementId);
      builder.setOffset(offset);
      builder.setFetchMaxRowCount(fetchMaxRowCount);

      return builder.build();
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + fetchMaxRowCount;
      result = prime * result + (int) (offset ^ (offset >>> 32));
      result = prime * result + ((parameterValues == null) ? 0 : parameterValues.hashCode());
      result = prime * result + statementId;
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof FetchRequest) {
        FetchRequest other = (FetchRequest) o;

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else {
          if (!connectionId.equals(other.connectionId)) {
            return false;
          }
        }

        if (null == parameterValues) {
          if (null != other.parameterValues) {
            return false;
          }
        } else if (!parameterValues.equals(other.parameterValues)) {
          return false;
        }

        return offset == other.offset && fetchMaxRowCount == other.fetchMaxRowCount;
      }

      return false;
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.FetchRequest}. */
  class FetchResponse extends Response {
    public final Meta.Frame frame;

    FetchResponse() {
      frame = null;
    }

    @JsonCreator
    public FetchResponse(@JsonProperty("frame") Meta.Frame frame) {
      this.frame = frame;
    }

    @Override FetchResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.FetchResponse)) {
        throw new IllegalArgumentException(
            "Expected FetchResponse, but got" + genericMsg.getClass().getName());
      }

      Responses.FetchResponse msg = (Responses.FetchResponse) genericMsg;

      return new FetchResponse(Meta.Frame.fromProto(msg.getFrame()));
    }

    @Override Responses.FetchResponse serialize() {
      Responses.FetchResponse.Builder builder = Responses.FetchResponse.newBuilder();

      if (null != frame) {
        builder.setFrame(frame.toProto());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((frame == null) ? 0 : frame.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof FetchResponse) {
        FetchResponse other = (FetchResponse) o;

        if (frame == null) {
          if (other.frame != null) {
            return false;
          }
        } else if (!frame.equals(other.frame)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#createStatement(org.apache.calcite.avatica.Meta.ConnectionHandle)}. */
  class CreateStatementRequest extends Request {
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
      if (!(genericMsg instanceof Requests.CreateStatementRequest)) {
        throw new IllegalArgumentException(
            "Expected CreateStatementRequest, but got" + genericMsg.getClass().getName());
      }

      final Requests.CreateStatementRequest msg = (Requests.CreateStatementRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.CreateStatementRequest.CONNECTION_ID_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof CreateStatementRequest) {
        CreateStatementRequest other = (CreateStatementRequest) o;

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.CreateStatementRequest}. */
  class CreateStatementResponse extends Response {
    public final String connectionId;
    public final int statementId;

    CreateStatementResponse() {
      connectionId = null;
      statementId = 0;
    }

    @JsonCreator
    public CreateStatementResponse(
        @JsonProperty("connectionId") String connectionId,
        @JsonProperty("statementId") int statementId) {
      this.connectionId = connectionId;
      this.statementId = statementId;
    }

    @Override CreateStatementResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.CreateStatementResponse)) {
        throw new IllegalArgumentException(
            "Expected CreateStatementResponse, but got " + genericMsg.getClass().getName());
      }

      final Responses.CreateStatementResponse msg = (Responses.CreateStatementResponse) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Responses.CreateStatementResponse.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

      return new CreateStatementResponse(connectionId, msg.getStatementId());
    }

    @Override Responses.CreateStatementResponse serialize() {
      Responses.CreateStatementResponse.Builder builder = Responses.CreateStatementResponse
          .newBuilder();

      if (null != connectionId) {
        builder.setConnectionId(connectionId);
      }

      builder.setStatementId(statementId);

      return builder.build();
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + statementId;
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof CreateStatementResponse) {
        CreateStatementResponse other = (CreateStatementResponse) o;

        if (connectionId == null) {
          if (other.connectionId != null) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

        return statementId == other.statementId;
      }

      return false;
    }
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#closeStatement(org.apache.calcite.avatica.Meta.StatementHandle)}. */
  class CloseStatementRequest extends Request {
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
      if (!(genericMsg instanceof Requests.CloseStatementRequest)) {
        throw new IllegalArgumentException(
            "Expected CloseStatementRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.CloseStatementRequest msg = (Requests.CloseStatementRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.CloseStatementRequest.CONNECTION_ID_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + statementId;
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof CloseStatementRequest) {
        CloseStatementRequest other = (CloseStatementRequest) o;

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

        return statementId == other.statementId;
      }

      return false;
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.CloseStatementRequest}. */
  class CloseStatementResponse extends Response {
    @JsonCreator
    public CloseStatementResponse() {}

    @Override CloseStatementResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.CloseStatementResponse)) {
        throw new IllegalArgumentException(
            "Expected CloseStatementResponse, but got " + genericMsg.getClass().getName());
      }

      return new CloseStatementResponse();
    }

    @Override Responses.CloseStatementResponse serialize() {
      return Responses.CloseStatementResponse.newBuilder().build();
    }

    @Override public int hashCode() {
      return 0;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      return o instanceof CloseStatementResponse;
    }
  }

  /** Request for
   * {@link Meta#closeConnection(org.apache.calcite.avatica.Meta.ConnectionHandle)}. */
  class CloseConnectionRequest extends Request {
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
      if (!(genericMsg instanceof Requests.CloseConnectionRequest)) {
        throw new IllegalArgumentException(
            "Expected CloseConnectionRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.CloseConnectionRequest msg = (Requests.CloseConnectionRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.CloseConnectionRequest.CONNECTION_ID_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof CloseConnectionRequest) {
        CloseConnectionRequest other = (CloseConnectionRequest) o;

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.CloseConnectionRequest}. */
  class CloseConnectionResponse extends Response {
    @JsonCreator
    public CloseConnectionResponse() {}

    @Override CloseConnectionResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.CloseConnectionResponse)) {
        throw new IllegalArgumentException(
            "Expected CloseConnectionResponse, but got " + genericMsg.getClass().getName());
      }

      return new CloseConnectionResponse();
    }

    @Override Responses.CloseConnectionResponse serialize() {
      return Responses.CloseConnectionResponse.newBuilder().build();
    }

    @Override public int hashCode() {
      return 0;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      return o instanceof CloseConnectionResponse;
    }
  }

  /** Request for {@link Meta#connectionSync(Meta.ConnectionHandle, Meta.ConnectionProperties)}. */
  class ConnectionSyncRequest extends Request {
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
      if (!(genericMsg instanceof Requests.ConnectionSyncRequest)) {
        throw new IllegalArgumentException(
            "Expected ConnectionSyncRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.ConnectionSyncRequest msg = (Requests.ConnectionSyncRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.ConnectionSyncRequest.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

      Meta.ConnectionProperties connProps = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.ConnectionSyncRequest.CONN_PROPS_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connProps == null) ? 0 : connProps.hashCode());
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof ConnectionSyncRequest) {
        ConnectionSyncRequest other = (ConnectionSyncRequest) o;

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

        if (null == connProps) {
          if (null != other.connProps) {
            return false;
          }
        } else if (!connProps.equals(other.connProps)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Response for
   * {@link Meta#connectionSync(Meta.ConnectionHandle, Meta.ConnectionProperties)}. */
  class ConnectionSyncResponse extends Response {
    public final Meta.ConnectionProperties connProps;

    ConnectionSyncResponse() {
      connProps = null;
    }

    @JsonCreator
    public ConnectionSyncResponse(@JsonProperty("connProps") Meta.ConnectionProperties connProps) {
      this.connProps = connProps;
    }

    @Override ConnectionSyncResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.ConnectionSyncResponse)) {
        throw new IllegalArgumentException(
            "Expected ConnectionSyncResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.ConnectionSyncResponse msg = (Responses.ConnectionSyncResponse) genericMsg;

      return new ConnectionSyncResponse(ConnectionPropertiesImpl.fromProto(msg.getConnProps()));
    }

    @Override Responses.ConnectionSyncResponse serialize() {
      Responses.ConnectionSyncResponse.Builder builder = Responses.ConnectionSyncResponse
          .newBuilder();

      if (null != connProps) {
        builder.setConnProps(connProps.toProto());
      }

      return builder.build();
    }

    @Override public int hashCode() {
      if (null == connProps) {
        return 0;
      }

      return connProps.hashCode();
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof ConnectionSyncResponse) {
        ConnectionSyncResponse other = (ConnectionSyncResponse) o;

        if (null == connProps) {
          if (null != other.connProps) {
            return false;
          }
        } else if (!connProps.equals(other.connProps)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Response for
   * {@link Meta#getDatabaseProperties()}. */
  class DatabasePropertyResponse extends Response {
    public final Map<Meta.DatabaseProperty, Object> map;

    DatabasePropertyResponse() {
      map = null;
    }

    @JsonCreator
    public DatabasePropertyResponse(@JsonProperty("map") Map<Meta.DatabaseProperty, Object> map) {
      this.map = map;
    }

    @Override DatabasePropertyResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.DatabasePropertyResponse)) {
        throw new IllegalArgumentException(
            "Expected DatabasePropertyResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.DatabasePropertyResponse msg = (Responses.DatabasePropertyResponse) genericMsg;

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

          obj = Integer.valueOf((int) value.getNumberValue());
          break;
        default:
          throw new RuntimeException("Unhandled DatabaseProperty");
        }

        properties.put(dbProp, obj);
      }

      return new DatabasePropertyResponse(properties);
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
            throw new RuntimeException("Unhandled DatabaseProperty");
          }

          builder.addProps(Responses.DatabasePropertyElement.newBuilder()
              .setKey(entry.getKey().toProto()).setValue(valueBuilder.build()));
        }
      }

      return builder.build();
    }

    @Override public int hashCode() {
      if (null == map) {
        return 0;
      }

      return map.hashCode();
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof DatabasePropertyResponse) {
        DatabasePropertyResponse other = (DatabasePropertyResponse) o;

        if (null == map) {
          if (null != other.map) {
            return false;
          }
        } else if (!map.equals(other.map)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }
}

// End Service.java
