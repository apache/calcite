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
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests;
import org.apache.calcite.avatica.proto.Responses;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;

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
      @JsonSubTypes.Type(value = SyncResultsRequest.class, name = "syncResults") })
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
      @JsonSubTypes.Type(value = RpcMetadataResponse.class, name = "rpcMetadata") })
  abstract class Response {
    abstract Response deserialize(Message genericMsg);
    abstract Message serialize();
  }

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#getCatalogs(Meta.ConnectionHandle)}. */
  class CatalogsRequest extends Request {
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
      if (!(genericMsg instanceof Requests.CatalogsRequest)) {
        throw new IllegalArgumentException(
            "Expected CatalogsRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.CatalogsRequest msg = (Requests.CatalogsRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.CatalogsRequest.CONNECTION_ID_FIELD_NUMBER)) {
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
      return connectionId == null ? 0 : connectionId.hashCode();
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }

      if (o instanceof CatalogsRequest) {
        CatalogsRequest other = (CatalogsRequest) o;

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

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#getDatabaseProperties(Meta.ConnectionHandle)}. */
  class DatabasePropertyRequest extends Request {
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
      if (!(genericMsg instanceof Requests.DatabasePropertyRequest)) {
        throw new IllegalArgumentException(
            "Expected DatabasePropertyRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.DatabasePropertyRequest msg = (Requests.DatabasePropertyRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.DatabasePropertyRequest.CONNECTION_ID_FIELD_NUMBER)) {
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
      return connectionId == null ? 0 : connectionId.hashCode();
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }

      if (o instanceof DatabasePropertyRequest) {
        DatabasePropertyRequest other = (DatabasePropertyRequest) o;

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
  /** Request for
   * {@link Meta#getSchemas(Meta.ConnectionHandle, String, Meta.Pat)}. */
  class SchemasRequest extends Request {
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
      if (!(genericMsg instanceof Requests.SchemasRequest)) {
        throw new IllegalArgumentException(
            "Expected SchemasRequest, but got" + genericMsg.getClass().getName());
      }

      final Requests.SchemasRequest msg = (Requests.SchemasRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc, Requests.SchemasRequest.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

      String catalog = null;
      if (ProtobufService.hasField(msg, desc, Requests.SchemasRequest.CATALOG_FIELD_NUMBER)) {
        catalog = msg.getCatalog();
      }

      String schemaPattern = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.SchemasRequest.SCHEMA_PATTERN_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
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

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

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
   * {@link Meta#getTables(Meta.ConnectionHandle, String, org.apache.calcite.avatica.Meta.Pat, org.apache.calcite.avatica.Meta.Pat, java.util.List)}
   */
  class TablesRequest extends Request {
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
      if (!(genericMsg instanceof Requests.TablesRequest)) {
        throw new IllegalArgumentException(
            "Expected TablesRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.TablesRequest msg = (Requests.TablesRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc, Requests.TablesRequest.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
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

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

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
   * Request for {@link Meta#getTableTypes(Meta.ConnectionHandle)}.
   */
  class TableTypesRequest extends Request {
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
      if (!(genericMsg instanceof Requests.TableTypesRequest)) {
        throw new IllegalArgumentException(
            "Expected TableTypesRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.TableTypesRequest msg = (Requests.TableTypesRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.TableTypesRequest.CONNECTION_ID_FIELD_NUMBER)) {
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
      return connectionId == null ? 0 : connectionId.hashCode();
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof TableTypesRequest) {
        TableTypesRequest other = (TableTypesRequest) o;

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

  /** Request for
   * {@link Meta#getColumns(Meta.ConnectionHandle, String, org.apache.calcite.avatica.Meta.Pat, org.apache.calcite.avatica.Meta.Pat, org.apache.calcite.avatica.Meta.Pat)}.
   */
  class ColumnsRequest extends Request {
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
      if (!(genericMsg instanceof Requests.ColumnsRequest)) {
        throw new IllegalArgumentException(
            "Expected ColumnsRequest, but got" + genericMsg.getClass().getName());
      }

      final Requests.ColumnsRequest msg = (Requests.ColumnsRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc, Requests.ColumnsRequest.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
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

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

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
   * {@link Meta#getTypeInfo(Meta.ConnectionHandle)}. */
  class TypeInfoRequest extends Request {
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
      if (!(genericMsg instanceof Requests.TypeInfoRequest)) {
        throw new IllegalArgumentException(
            "Expected TypeInfoRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.TypeInfoRequest msg = (Requests.TypeInfoRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.TypeInfoRequest.CONNECTION_ID_FIELD_NUMBER)) {
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
      return connectionId == null ? 0 : connectionId.hashCode();
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof TypeInfoRequest) {
        TypeInfoRequest other = (TypeInfoRequest) o;

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

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc, Responses.ResultSetResponse.METADATA_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + ((firstFrame == null) ? 0 : firstFrame.hashCode());
      result = prime * result + (ownStatement ? 1231 : 1237);
      result = prime * result + ((signature == null) ? 0 : signature.hashCode());
      result = prime * result + statementId;
      result = prime * result + (int) (updateCount ^ (updateCount >>> 32));
      result = prime * result + ((rpcMetadata == null) ? 0 : rpcMetadata.hashCode());
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

        if (null == rpcMetadata) {
          if (null != rpcMetadata) {
            return false;
          }
        } else if (!rpcMetadata.equals(other.rpcMetadata)) {
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

  /** Request for
   * {@link org.apache.calcite.avatica.Meta#execute}. */
  class ExecuteRequest extends Request {
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
      if (!(genericMsg instanceof Requests.ExecuteRequest)) {
        throw new IllegalArgumentException(
            "Expected ExecuteRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.ExecuteRequest msg = (Requests.ExecuteRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      Meta.StatementHandle statemetnHandle = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.ExecuteRequest.STATEMENTHANDLE_FIELD_NUMBER)) {
        statemetnHandle = Meta.StatementHandle.fromProto(msg.getStatementHandle());
      }

      List<TypedValue> values = null;
      if (msg.getHasParameterValues()) {
        values = new ArrayList<>(msg.getParameterValuesCount());
        for (Common.TypedValue valueProto : msg.getParameterValuesList()) {
          values.add(TypedValue.fromProto(valueProto));
        }
      }

      return new ExecuteRequest(statemetnHandle, values, msg.getMaxRowCount());
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

      builder.setMaxRowCount(maxRowCount);

      return builder.build();
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((statementHandle == null) ? 0 : statementHandle.hashCode());
      result = prime * result + ((parameterValues == null) ? 0 : parameterValues.hashCode());
      result = prime * result + (int) (maxRowCount ^ (maxRowCount >>> 32));
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof ExecuteRequest) {
        ExecuteRequest other = (ExecuteRequest) o;

        if (statementHandle == null) {
          if (other.statementHandle != null) {
            return false;
          }
        } else if (!statementHandle.equals(other.statementHandle)) {
          return false;
        }

        if (null == parameterValues) {
          if (null != other.parameterValues) {
            return false;
          }
        } else if (!parameterValues.equals(other.parameterValues)) {
          return false;
        }

        return maxRowCount == other.maxRowCount;
      }
      return false;
    }
  }

  /** Response to a
   * {@link org.apache.calcite.avatica.remote.Service.PrepareAndExecuteRequest}. */
  class ExecuteResponse extends Response {
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
      if (!(genericMsg instanceof Responses.ExecuteResponse)) {
        throw new IllegalArgumentException(
            "Expected ExecuteResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.ExecuteResponse msg = (Responses.ExecuteResponse) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      List<Responses.ResultSetResponse> msgResults = msg.getResultsList();
      List<ResultSetResponse> copiedResults = new ArrayList<>(msgResults.size());

      for (Responses.ResultSetResponse msgResult : msgResults) {
        copiedResults.add(ResultSetResponse.fromProto(msgResult));
      }

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc, Responses.ExecuteResponse.METADATA_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((results == null) ? 0 : results.hashCode());
      result = prime * result + ((rpcMetadata == null) ? 0 : rpcMetadata.hashCode());
      return result;
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

        if (null == rpcMetadata) {
          if (null != other.rpcMetadata) {
            return false;
          }
        } else if (!rpcMetadata.equals(other.rpcMetadata)) {
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
      if (!(genericMsg instanceof Responses.PrepareResponse)) {
        throw new IllegalArgumentException(
            "Expected PrepareResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.PrepareResponse msg = (Responses.PrepareResponse) genericMsg;
      Descriptor desc = msg.getDescriptorForType();

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc, Responses.PrepareResponse.METADATA_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((statement == null) ? 0 : statement.hashCode());
      result = prime * result + ((null == rpcMetadata) ? 0 : rpcMetadata.hashCode());
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

        if (null == rpcMetadata) {
          if (null != other.rpcMetadata) {
            return false;
          }
        } else if (!rpcMetadata.equals(other.rpcMetadata)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Request for
   * {@link Meta#fetch}. */
  class FetchRequest extends Request {
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
      if (!(genericMsg instanceof Requests.FetchRequest)) {
        throw new IllegalArgumentException(
            "Expected FetchRequest, but got " + genericMsg.getClass().getName());
      }

      final Requests.FetchRequest msg = (Requests.FetchRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc, Requests.FetchRequest.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

      return new FetchRequest(connectionId, msg.getStatementId(), msg.getOffset(),
          msg.getFetchMaxRowCount());
    }

    @Override Requests.FetchRequest serialize() {
      Requests.FetchRequest.Builder builder = Requests.FetchRequest.newBuilder();

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

        return offset == other.offset && fetchMaxRowCount == other.fetchMaxRowCount;
      }

      return false;
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.FetchRequest}. */
  class FetchResponse extends Response {
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
      if (!(genericMsg instanceof Responses.FetchResponse)) {
        throw new IllegalArgumentException(
            "Expected FetchResponse, but got" + genericMsg.getClass().getName());
      }

      Responses.FetchResponse msg = (Responses.FetchResponse) genericMsg;
      Descriptor desc = msg.getDescriptorForType();

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc, Responses.FetchResponse.METADATA_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((frame == null) ? 0 : frame.hashCode());
      result = prime * result + ((null == rpcMetadata) ? 0 : rpcMetadata.hashCode());
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

        if (null == rpcMetadata) {
          if (null != other.rpcMetadata) {
            return false;
          }
        } else if (!rpcMetadata.equals(other.rpcMetadata)) {
          return false;
        }

        return missingStatement == other.missingStatement;
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

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc,
          Responses.CreateStatementResponse.METADATA_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + statementId;
      result = prime * result + ((null == rpcMetadata) ? 0 : rpcMetadata.hashCode());
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

        if (null == rpcMetadata) {
          if (null != other.rpcMetadata) {
            return false;
          }
        } else if (!rpcMetadata.equals(other.rpcMetadata)) {
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
    public final RpcMetadataResponse rpcMetadata;

    public CloseStatementResponse() {
      rpcMetadata = null;
    }

    @JsonCreator
    public CloseStatementResponse(@JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.rpcMetadata = rpcMetadata;
    }

    @Override CloseStatementResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.CloseStatementResponse)) {
        throw new IllegalArgumentException(
            "Expected CloseStatementResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.CloseStatementResponse msg = (Responses.CloseStatementResponse) genericMsg;
      Descriptor desc = msg.getDescriptorForType();

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc,
          Responses.CloseStatementResponse.METADATA_FIELD_NUMBER)) {
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
      return (null == rpcMetadata) ? 0 : rpcMetadata.hashCode();
    }

    @Override public boolean equals(Object o) {
      return o == this || (o instanceof CloseStatementResponse
          && Objects.equals(rpcMetadata, ((CloseStatementResponse) o).rpcMetadata));
    }
  }

  /** Request for
   * {@link Meta#openConnection}. */
  class OpenConnectionRequest extends Request {
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
        boolean localProperty = false;
        for (BuiltInConnectionProperty prop : BuiltInConnectionProperty.values()) {
          if (prop.camelName().equals(entry.getKey())) {
            localProperty = true;
            break;
          }
        }

        if (!localProperty) {
          infoAsString.put(entry.getKey().toString(), entry.getValue().toString());
        }
      }
      return infoAsString;
    }

    @Override Request deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Requests.OpenConnectionRequest)) {
        throw new IllegalArgumentException(
            "Expected OpenConnectionRequest, but got" + genericMsg.getClass().getName());
      }

      final Requests.OpenConnectionRequest msg = (Requests.OpenConnectionRequest) genericMsg;
      final Descriptor desc = msg.getDescriptorForType();

      String connectionId = null;
      if (ProtobufService.hasField(msg, desc,
          Requests.OpenConnectionRequest.CONNECTION_ID_FIELD_NUMBER)) {
        connectionId = msg.getConnectionId();
      }

      Map<String, String> info = msg.getInfo();
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
        builder.getMutableInfo().putAll(info);
      }

      return builder.build();
    }

    @Override public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + ((info == null) ? 0 : info.hashCode());
      return result;
    }

    @Override public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (o instanceof OpenConnectionRequest) {
        OpenConnectionRequest other = (OpenConnectionRequest) o;

        if (null == connectionId) {
          if (null != other.connectionId) {
            return false;
          }
        } else if (!connectionId.equals(other.connectionId)) {
          return false;
        }

        if (null == info) {
          if (null != other.info) {
            return false;
          }
        } else if (!info.equals(other.info)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.OpenConnectionRequest}. */
  class OpenConnectionResponse extends Response {
    public final RpcMetadataResponse rpcMetadata;

    public OpenConnectionResponse() {
      rpcMetadata = null;
    }

    @JsonCreator
    public OpenConnectionResponse(@JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.rpcMetadata = rpcMetadata;
    }

    @Override OpenConnectionResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.OpenConnectionResponse)) {
        throw new IllegalArgumentException(
            "Expected OpenConnectionResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.OpenConnectionResponse msg = (Responses.OpenConnectionResponse) genericMsg;
      Descriptor desc = msg.getDescriptorForType();

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc,
          Responses.OpenConnectionResponse.METADATA_FIELD_NUMBER)) {
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
      return (null == rpcMetadata) ? 0 : rpcMetadata.hashCode();
    }

    @Override public boolean equals(Object o) {
      return o == this || ((o instanceof OpenConnectionResponse)
          && Objects.equals(rpcMetadata, ((OpenConnectionResponse) o).rpcMetadata));
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
      return o == this || ((o instanceof CloseConnectionRequest)
          && Objects.equals(connectionId, ((CloseConnectionRequest) o).connectionId));
    }
  }

  /** Response from
   * {@link org.apache.calcite.avatica.remote.Service.CloseConnectionRequest}. */
  class CloseConnectionResponse extends Response {
    public final RpcMetadataResponse rpcMetadata;

    public CloseConnectionResponse() {
      rpcMetadata = null;
    }

    @JsonCreator
    public CloseConnectionResponse(@JsonProperty("rpcMetadata") RpcMetadataResponse rpcMetadata) {
      this.rpcMetadata = rpcMetadata;
    }

    @Override CloseConnectionResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.CloseConnectionResponse)) {
        throw new IllegalArgumentException(
            "Expected CloseConnectionResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.CloseConnectionResponse msg = (Responses.CloseConnectionResponse) genericMsg;
      Descriptor desc = msg.getDescriptorForType();

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc,
          Responses.CloseConnectionResponse.METADATA_FIELD_NUMBER)) {
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
      return (null == rpcMetadata) ? 0 : rpcMetadata.hashCode();
    }

    @Override public boolean equals(Object o) {
      return o == this || ((o instanceof CloseConnectionResponse)
          && Objects.equals(rpcMetadata, ((CloseConnectionResponse) o).rpcMetadata));
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
      if (!(genericMsg instanceof Responses.ConnectionSyncResponse)) {
        throw new IllegalArgumentException(
            "Expected ConnectionSyncResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.ConnectionSyncResponse msg = (Responses.ConnectionSyncResponse) genericMsg;
      Descriptor desc = msg.getDescriptorForType();

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc,
          Responses.ConnectionSyncResponse.METADATA_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connProps == null) ? 0 : connProps.hashCode());
      result = prime * result + ((rpcMetadata == null) ? 0 : rpcMetadata.hashCode());
      return result;
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

        if (null == rpcMetadata) {
          if (null != other.rpcMetadata) {
            return false;
          }
        } else if (!rpcMetadata.equals(other.rpcMetadata)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /** Response for
   * {@link Meta#getDatabaseProperties(Meta.ConnectionHandle)}. */
  class DatabasePropertyResponse extends Response {
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
      if (!(genericMsg instanceof Responses.DatabasePropertyResponse)) {
        throw new IllegalArgumentException(
            "Expected DatabasePropertyResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.DatabasePropertyResponse msg = (Responses.DatabasePropertyResponse) genericMsg;
      Descriptor desc = msg.getDescriptorForType();

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

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc,
          Responses.DatabasePropertyResponse.METADATA_FIELD_NUMBER)) {
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
            throw new RuntimeException("Unhandled DatabaseProperty");
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((map == null) ? 0 : map.hashCode());
      result = prime * result + ((rpcMetadata == null) ? 0 : rpcMetadata.hashCode());
      return result;
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

        if (null == rpcMetadata) {
          if (null != other.rpcMetadata) {
            return false;
          }
        } else if (!rpcMetadata.equals(other.rpcMetadata)) {
          return false;
        }

        return true;
      }

      return false;
    }
  }

  /**
   * Response for any request that the server failed to successfully perform.
   * It is used internally by the transport layers to format errors for
   * transport over the wire. Thus, {@link Service#apply} will never return
   * an ErrorResponse.
   */
  public class ErrorResponse extends Response {
    public static final int UNKNOWN_ERROR_CODE = -1;
    public static final int MISSING_CONNECTION_ERROR_CODE = 1;

    public static final String UNKNOWN_SQL_STATE = "00000";

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
      StringWriter sw = new StringWriter();
      Objects.requireNonNull(e).printStackTrace(new PrintWriter(sw));
      return sw.toString();
    }

    @Override ErrorResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.ErrorResponse)) {
        throw new IllegalArgumentException("Expected ErrorResponse, but got "
          + genericMsg.getClass());
      }

      Responses.ErrorResponse msg = (Responses.ErrorResponse) genericMsg;
      Descriptor desc = msg.getDescriptorForType();

      List<String> exceptions = null;
      if (msg.getHasExceptions()) {
        exceptions = msg.getExceptionsList();
      }

      String errorMessage = null;
      if (ProtobufService.hasField(msg, desc, Responses.ErrorResponse.ERROR_MESSAGE_FIELD_NUMBER)) {
        errorMessage = msg.getErrorMessage();
      }

      String sqlState = null;
      if (ProtobufService.hasField(msg, desc, Responses.ErrorResponse.SQL_STATE_FIELD_NUMBER)) {
        sqlState = msg.getSqlState();
      }

      AvaticaSeverity severity = null;
      if (ProtobufService.hasField(msg, desc, Responses.ErrorResponse.SEVERITY_FIELD_NUMBER)) {
        severity = AvaticaSeverity.fromProto(msg.getSeverity());
      }

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc, Responses.ErrorResponse.METADATA_FIELD_NUMBER)) {
        metadata = RpcMetadataResponse.fromProto(msg.getMetadata());
      }

      return new ErrorResponse(exceptions, errorMessage, msg.getErrorCode(), sqlState, severity,
          metadata);
    }

    @Override Responses.ErrorResponse serialize() {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((exceptions == null) ? 0 : exceptions.hashCode());
      result = prime * result + ((errorMessage == null) ? 0 : errorMessage.hashCode());
      result = prime * result + errorCode;
      result = prime * result + ((sqlState == null) ? 0 : sqlState.hashCode());
      result = prime * result + ((severity == null) ? 0 : severity.hashCode());
      result = prime * result + ((rpcMetadata == null) ? 0 : rpcMetadata.hashCode());
      return result;
    }

    @Override public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ErrorResponse)) {
        return false;
      }

      ErrorResponse other = (ErrorResponse) obj;
      if (exceptions == null) {
        if (other.exceptions != null) {
          return false;
        }
      } else if (!exceptions.equals(other.exceptions)) {
        return false;
      }

      if (errorMessage == null) {
        if (other.errorMessage != null) {
          return false;
        }
      } else if (!errorMessage.equals(other.errorMessage)) {
        return false;
      }

      if (errorCode != other.errorCode) {
        return false;
      }

      if (sqlState == null) {
        if (other.sqlState != null) {
          return false;
        }
      } else if (!sqlState.equals(other.sqlState)) {
        return false;
      }

      if (severity != other.severity) {
        return false;
      }

      if (null == rpcMetadata) {
        if (null != other.rpcMetadata) {
          return false;
        }
      } else if (!rpcMetadata.equals(other.rpcMetadata)) {
        return false;
      }

      return true;
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
      if (!(genericMsg instanceof Requests.SyncResultsRequest)) {
        throw new IllegalArgumentException(
            "Expected SyncResultsRequest, but got " + genericMsg.getClass().getName());
      }

      Requests.SyncResultsRequest msg = (Requests.SyncResultsRequest) genericMsg;

      return new SyncResultsRequest(msg.getConnectionId(), msg.getStatementId(),
          QueryState.fromProto(msg.getState()), msg.getOffset());
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
      final int prime = 31;
      int result = 1;
      result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
      result = prime * result + (int) (offset ^ (offset >>> 32));
      result = prime * result + ((state == null) ? 0 : state.hashCode());
      result = prime * result + statementId;
      return result;
    }

    @Override public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }

      if (null == obj || !(obj instanceof SyncResultsRequest)) {
        return false;
      }

      SyncResultsRequest other = (SyncResultsRequest) obj;

      if (connectionId == null) {
        if (other.connectionId != null) {
          return false;
        }
      } else if (!connectionId.equals(other.connectionId)) {
        return false;
      }

      if (offset != other.offset) {
        return false;
      }

      if (state == null) {
        if (other.state != null) {
          return false;
        }
      } else if (!state.equals(other.state)) {
        return false;
      }

      if (statementId != other.statementId) {
        return false;
      }

      return true;
    }
  }

  /**
   * Response for {@link Service#apply(SyncResultsRequest)}.
   */
  class SyncResultsResponse extends Response {
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
      if (!(genericMsg instanceof Responses.SyncResultsResponse)) {
        throw new IllegalArgumentException(
            "Expected SyncResultsResponse, but got " + genericMsg.getClass().getName());
      }

      Responses.SyncResultsResponse msg = (Responses.SyncResultsResponse) genericMsg;
      Descriptor desc = msg.getDescriptorForType();

      RpcMetadataResponse metadata = null;
      if (ProtobufService.hasField(msg, desc,
          Responses.SyncResultsResponse.METADATA_FIELD_NUMBER)) {
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
      final int prime = 31;
      int result = 1;
      result = prime * result + (missingStatement ? 1231 : 1237);
      result = prime * result + (moreResults ? 1231 : 1237);
      result = prime * result + ((rpcMetadata == null) ? 0 : rpcMetadata.hashCode());
      return result;
    }

    @Override public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || !(obj instanceof SyncResultsResponse)) {
        return false;
      }

      SyncResultsResponse other = (SyncResultsResponse) obj;

      if (null == rpcMetadata) {
        if (null != other.rpcMetadata) {
          return false;
        }
      } else if (!rpcMetadata.equals(other.rpcMetadata)) {
        return false;
      }

      return missingStatement == other.missingStatement && moreResults == other.moreResults;
    }
  }

  /**
   * Response that includes information about the server that handled an RPC.
   *
   * This isn't really a "response", but we want to be able to be able to convert it to protobuf
   * and back again, so ignore that there isn't an explicit endpoint for it.
   */
  public class RpcMetadataResponse extends Response {
    public final String serverAddress;

    public RpcMetadataResponse() {
      this.serverAddress = null;
    }

    public RpcMetadataResponse(@JsonProperty("serverAddress") String serverAddress) {
      this.serverAddress = serverAddress;
    }

    @Override
    RpcMetadataResponse deserialize(Message genericMsg) {
      if (!(genericMsg instanceof Responses.RpcMetadata)) {
        throw new IllegalArgumentException("Expected RpcMetadata, but got "
            + genericMsg.getClass().getName());
      }

      return fromProto((Responses.RpcMetadata) genericMsg);
    }

    @Override
    Responses.RpcMetadata serialize() {
      return Responses.RpcMetadata.newBuilder().setServerAddress(serverAddress).build();
    }

    static RpcMetadataResponse fromProto(Responses.RpcMetadata msg) {
      Descriptor desc = msg.getDescriptorForType();

      String serverAddress = null;
      if (ProtobufService.hasField(msg, desc, Responses.RpcMetadata.SERVER_ADDRESS_FIELD_NUMBER)) {
        serverAddress = msg.getServerAddress();
      }

      return new RpcMetadataResponse(serverAddress);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((serverAddress == null) ? 0 : serverAddress.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      return this == obj || (obj instanceof RpcMetadataResponse
          && Objects.equals(serverAddress, ((RpcMetadataResponse) obj).serverAddress));
    }
  }
}

// End Service.java
