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
package org.apache.calcite.avatica;

import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Common.MetaDataOperationArgument;
import org.apache.calcite.avatica.proto.Common.MetaDataOperationArgument.ArgumentType;
import org.apache.calcite.avatica.remote.MetaDataOperation;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Objects;

/**
 * A struct used to encapsulate the necessary information to reconstitute a ResultSet in the
 * Avatica server.
 */
public class QueryState {

  /**
   * An enumeration that represents how a ResultSet was created.
   */
  public enum StateType {
    SQL,
    METADATA;

    public Common.StateType toProto() {
      switch (this) {
      case SQL:
        return Common.StateType.SQL;
      case METADATA:
        return Common.StateType.METADATA;
      default:
        return Common.StateType.UNRECOGNIZED;
      }
    }

    public static StateType fromProto(Common.StateType protoType) {
      switch (protoType) {
      case SQL:
        return StateType.SQL;
      case METADATA:
        return StateType.METADATA;
      default:
        throw new IllegalArgumentException("Unhandled StateType " + protoType);
      }
    }
  }

  @JsonProperty("type")
  public final StateType type;

  @JsonProperty("sql")
  public final String sql;

  @JsonProperty("metaDataOperation")
  public final MetaDataOperation metaDataOperation;
  @JsonProperty("operationArgs")
  public final Object[] operationArgs;

  /**
   * Constructor encapsulating a SQL query used to create a result set.
   *
   * @param sql The SQL query.
   */
  public QueryState(String sql) {
    // This doesn't to be non-null
    this.sql = sql;
    this.type = StateType.SQL;

    // Null out the members we don't use.
    this.metaDataOperation = null;
    this.operationArgs = null;
  }

  /**
   * Constructor encapsulating a metadata operation's result set.
   *
   * @param op A pointer to the {@link DatabaseMetaData} operation being invoked.
   * @param args The arguments to the method being invoked.
   */
  public QueryState(MetaDataOperation op, Object... args) {
    this.metaDataOperation = Objects.requireNonNull(op);
    this.operationArgs = Arrays.copyOf(Objects.requireNonNull(args), args.length);
    this.type = StateType.METADATA;

    // Null out the members we won't use
    this.sql = null;
  }

  /**
   * Not intended for external use. For Jackson-databind only.
   */
  public QueryState(StateType type, String sql, MetaDataOperation op, Object... args) {
    this.type = Objects.requireNonNull(type);
    switch (type) {
    case SQL:
      this.sql = Objects.requireNonNull(sql);
      if (null != op) {
        throw new IllegalArgumentException("Expected null MetaDataOperation, but got " + op);
      }
      this.metaDataOperation = null;
      if (null != args) {
        throw new IllegalArgumentException("Expected null arguments, but got "
            + Arrays.toString(args));
      }
      this.operationArgs = null;
      break;
    case METADATA:
      this.metaDataOperation = Objects.requireNonNull(op);
      this.operationArgs = Objects.requireNonNull(args);
      if (null != sql) {
        throw new IllegalArgumentException("Expected null SQl but got " + sql);
      }
      this.sql = null;
      break;
    default:
      throw new IllegalArgumentException("Unable to handle StateType " + type);
    }
  }

  /**
   * Not intended for external use. For Jackson-databind only.
   */
  public QueryState() {
    this.sql = null;
    this.metaDataOperation = null;
    this.type = null;
    this.operationArgs = null;
  }

  /**
   * @return The {@link StateType} for this encapsulated state.
   */
  public StateType getType() {
    return type;
  }

  /**
   * @return The SQL expression to invoke.
   */
  public String getSql() {
    assert type == StateType.SQL;
    return sql;
  }

  /**
   * @return The metadata operation to invoke.
   */
  public MetaDataOperation getMetaDataOperation() {
    assert type == StateType.METADATA;
    return metaDataOperation;
  }

  /**
   * @return The Arguments for the given metadata operation.
   */
  public Object[] getOperationArgs() {
    assert type == StateType.METADATA;
    return operationArgs;
  }

  public ResultSet invoke(Connection conn, Statement statement) throws SQLException {
    switch (type) {
    case SQL:
      boolean ret = Objects.requireNonNull(statement).execute(sql);
      ResultSet results = statement.getResultSet();

      // Either execute(sql) returned true or the resultSet was null
      assert ret || null == results;

      return results;
    case METADATA:
      DatabaseMetaData metadata = Objects.requireNonNull(conn).getMetaData();
      switch (metaDataOperation) {
      case GET_ATTRIBUTES:
        verifyOpArgs(4);
        return metadata.getAttributes((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (String) operationArgs[3]);
      case GET_BEST_ROW_IDENTIFIER:
        verifyOpArgs(5);
        return metadata.getBestRowIdentifier((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (int) operationArgs[3],
            (boolean) operationArgs[4]);
      case GET_CATALOGS:
        verifyOpArgs(0);
        return metadata.getCatalogs();
      case GET_COLUMNS:
        verifyOpArgs(4);
        return metadata.getColumns((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (String) operationArgs[3]);
      case GET_COLUMN_PRIVILEGES:
        verifyOpArgs(4);
        return metadata.getColumnPrivileges((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (String) operationArgs[3]);
      case GET_CROSS_REFERENCE:
        verifyOpArgs(6);
        return metadata.getCrossReference((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (String) operationArgs[3],
            (String) operationArgs[4],
            (String) operationArgs[5]);
      case GET_EXPORTED_KEYS:
        verifyOpArgs(3);
        return metadata.getExportedKeys((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2]);
      case GET_FUNCTIONS:
        verifyOpArgs(3);
        return metadata.getFunctions((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2]);
      case GET_FUNCTION_COLUMNS:
        verifyOpArgs(4);
        return metadata.getFunctionColumns((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (String) operationArgs[3]);
      case GET_IMPORTED_KEYS:
        verifyOpArgs(3);
        return metadata.getImportedKeys((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2]);
      case GET_INDEX_INFO:
        verifyOpArgs(5);
        return metadata.getIndexInfo((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (boolean) operationArgs[3],
            (boolean) operationArgs[4]);
      case GET_PRIMARY_KEYS:
        verifyOpArgs(3);
        return metadata.getPrimaryKeys((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2]);
      case GET_PROCEDURES:
        verifyOpArgs(3);
        return metadata.getProcedures((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2]);
      case GET_PROCEDURE_COLUMNS:
        verifyOpArgs(4);
        return metadata.getProcedureColumns((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (String) operationArgs[3]);
      case GET_PSEUDO_COLUMNS:
        verifyOpArgs(4);
        return metadata.getPseudoColumns((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (String) operationArgs[3]);
      case GET_SCHEMAS:
        verifyOpArgs(0);
        return metadata.getSchemas();
      case GET_SCHEMAS_WITH_ARGS:
        verifyOpArgs(2);
        return metadata.getSchemas((String) operationArgs[0],
            (String) operationArgs[1]);
      case GET_SUPER_TABLES:
        verifyOpArgs(3);
        return metadata.getSuperTables((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2]);
      case GET_SUPER_TYPES:
        verifyOpArgs(3);
        return metadata.getSuperTypes((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2]);
      case GET_TABLES:
        verifyOpArgs(4);
        return metadata.getTables((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (String[]) operationArgs[3]);
      case GET_TABLE_PRIVILEGES:
        verifyOpArgs(3);
        return metadata.getTablePrivileges((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2]);
      case GET_TABLE_TYPES:
        verifyOpArgs(0);
        return metadata.getTableTypes();
      case GET_TYPE_INFO:
        verifyOpArgs(0);
        return metadata.getTypeInfo();
      case GET_UDTS:
        verifyOpArgs(4);
        return metadata.getUDTs((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2],
            (int[]) operationArgs[3]);
      case GET_VERSION_COLUMNS:
        verifyOpArgs(3);
        return metadata.getVersionColumns((String) operationArgs[0],
            (String) operationArgs[1],
            (String) operationArgs[2]);
      default:
        throw new IllegalArgumentException("Unhandled Metadata operation: " + metaDataOperation);
      }
    default:
      throw new IllegalArgumentException("Unable to process QueryState of type " + type);
    }
  }

  private void verifyOpArgs(int expectedArgs) {
    if (expectedArgs != operationArgs.length) {
      throw new RuntimeException("Expected " + expectedArgs + " arguments, but got "
          + Arrays.toString(operationArgs));
    }
  }

  public Common.QueryState toProto() {
    Common.QueryState.Builder builder = Common.QueryState.newBuilder();

    // Required
    switch (type) {
    case SQL:
      builder.setType(Common.StateType.SQL);
      break;
    case METADATA:
      builder.setType(Common.StateType.METADATA);
      break;
    default:
      throw new IllegalStateException("Unhandled type: " + type);
    }

    // Optional SQL
    if (null != sql) {
      builder.setSql(sql).setHasSql(true);
    }

    // Optional metaDataOperation
    if (null != metaDataOperation) {
      builder.setOp(metaDataOperation.toProto()).setHasOp(true);
    }

    // Optional operationArgs
    if (null != operationArgs) {
      builder.setHasArgs(true);
      for (Object arg : operationArgs) {
        MetaDataOperationArgument.Builder argBuilder = MetaDataOperationArgument.newBuilder();

        if (null == arg) {
          builder.addArgs(argBuilder.setType(ArgumentType.NULL).build());
        } else if (arg instanceof String) {
          builder.addArgs(argBuilder.setType(ArgumentType.STRING)
              .setStringValue((String) arg).build());
        } else if (arg instanceof Integer) {
          builder.addArgs(argBuilder.setType(ArgumentType.INT).setIntValue((int) arg).build());
        } else if (arg instanceof Boolean) {
          builder.addArgs(
              argBuilder.setType(ArgumentType.BOOL).setBoolValue((boolean) arg).build());
        } else if (arg instanceof String[]) {
          argBuilder.setType(ArgumentType.REPEATED_STRING);
          for (String strArg : (String[]) arg) {
            argBuilder.addStringArrayValues(strArg);
          }
          builder.addArgs(argBuilder.build());
        } else if (arg instanceof int[]) {
          argBuilder.setType(ArgumentType.REPEATED_INT);
          for (int intArg : (int[]) arg) {
            argBuilder.addIntArrayValues(intArg);
          }
          builder.addArgs(argBuilder.build());
        } else {
          throw new RuntimeException("Unexpected operation argument: " + arg.getClass());
        }
      }
    } else {
      builder.setHasArgs(false);
    }

    return builder.build();
  }

  public static QueryState fromProto(Common.QueryState protoState) {
    StateType type = StateType.fromProto(protoState.getType());
    String sql = protoState.getHasSql() ? protoState.getSql() : null;
    MetaDataOperation op = protoState.getHasOp()
        ? MetaDataOperation.fromProto(protoState.getOp()) : null;
    Object[] opArgs = null;
    if (protoState.getHasArgs()) {
      opArgs = new Object[protoState.getArgsCount()];
      int i = 0;
      for (Common.MetaDataOperationArgument arg : protoState.getArgsList()) {
        switch (arg.getType()) {
        case STRING:
          opArgs[i] = arg.getStringValue();
          break;
        case BOOL:
          opArgs[i] = arg.getBoolValue();
          break;
        case INT:
          opArgs[i] = arg.getIntValue();
          break;
        case REPEATED_STRING:
          opArgs[i] = arg.getStringArrayValuesList().toArray(
              new String[arg.getStringArrayValuesCount()]);
          break;
        case REPEATED_INT:
          int[] arr = new int[arg.getIntArrayValuesCount()];
          int offset = 0;
          for (Integer val : arg.getIntArrayValuesList()) {
            arr[offset] = val;
            offset++;
          }
          opArgs[i] = arr;
          break;
        case NULL:
          opArgs[i] = null;
          break;
        default:
          throw new RuntimeException("Could not interpret " + arg.getType());
        }

        i++;
      }
    }

    return new QueryState(type, sql, op, opArgs);
  }

  @Override public int hashCode() {
    return Objects.hash(metaDataOperation, Arrays.hashCode(operationArgs), sql);
  }

  @Override public boolean equals(Object o) {
    return o == this
        || o instanceof QueryState
        && metaDataOperation == ((QueryState) o).metaDataOperation
        && Arrays.deepEquals(operationArgs, ((QueryState) o).operationArgs)
        && Objects.equals(sql, ((QueryState) o).sql);
  }
}

// End QueryState.java
