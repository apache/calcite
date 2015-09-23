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

import org.apache.calcite.avatica.proto.Common;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

/**
 * Identifies an operation from {@link DatabaseMetaData} which returns a {@link ResultSet}. This
 * enum is used to allow clients to request the server to re-instantiate a {@link ResultSet} for
 * these operations which do not have a SQL string associated with them as a normal query does.
 */
public enum MetaDataOperation {
  GET_ATTRIBUTES,
  GET_BEST_ROW_IDENTIFIER,
  GET_CATALOGS,
  GET_CLIENT_INFO_PROPERTIES,
  GET_COLUMN_PRIVILEGES,
  GET_COLUMNS,
  GET_CROSS_REFERENCE,
  GET_EXPORTED_KEYS,
  GET_FUNCTION_COLUMNS,
  GET_FUNCTIONS,
  GET_IMPORTED_KEYS,
  GET_INDEX_INFO,
  GET_PRIMARY_KEYS,
  GET_PROCEDURE_COLUMNS,
  GET_PROCEDURES,
  GET_PSEUDO_COLUMNS,
  GET_SCHEMAS,
  GET_SCHEMAS_WITH_ARGS,
  GET_SUPER_TABLES,
  GET_SUPER_TYPES,
  GET_TABLE_PRIVILEGES,
  GET_TABLES,
  GET_TABLE_TYPES,
  GET_TYPE_INFO,
  GET_UDTS,
  GET_VERSION_COLUMNS;

  public Common.MetaDataOperation toProto() {
    switch (this) {
    case GET_ATTRIBUTES:
      return Common.MetaDataOperation.GET_ATTRIBUTES;
    case GET_BEST_ROW_IDENTIFIER:
      return Common.MetaDataOperation.GET_BEST_ROW_IDENTIFIER;
    case GET_CATALOGS:
      return Common.MetaDataOperation.GET_CATALOGS;
    case GET_CLIENT_INFO_PROPERTIES:
      return Common.MetaDataOperation.GET_CLIENT_INFO_PROPERTIES;
    case GET_COLUMNS:
      return Common.MetaDataOperation.GET_COLUMNS;
    case GET_COLUMN_PRIVILEGES:
      return Common.MetaDataOperation.GET_COLUMN_PRIVILEGES;
    case GET_CROSS_REFERENCE:
      return Common.MetaDataOperation.GET_CROSS_REFERENCE;
    case GET_EXPORTED_KEYS:
      return Common.MetaDataOperation.GET_EXPORTED_KEYS;
    case GET_FUNCTIONS:
      return Common.MetaDataOperation.GET_FUNCTIONS;
    case GET_FUNCTION_COLUMNS:
      return Common.MetaDataOperation.GET_FUNCTION_COLUMNS;
    case GET_IMPORTED_KEYS:
      return Common.MetaDataOperation.GET_IMPORTED_KEYS;
    case GET_INDEX_INFO:
      return Common.MetaDataOperation.GET_INDEX_INFO;
    case GET_PRIMARY_KEYS:
      return Common.MetaDataOperation.GET_PRIMARY_KEYS;
    case GET_PROCEDURES:
      return Common.MetaDataOperation.GET_PROCEDURES;
    case GET_PROCEDURE_COLUMNS:
      return Common.MetaDataOperation.GET_PROCEDURE_COLUMNS;
    case GET_PSEUDO_COLUMNS:
      return Common.MetaDataOperation.GET_PSEUDO_COLUMNS;
    case GET_SCHEMAS:
      return Common.MetaDataOperation.GET_SCHEMAS;
    case GET_SCHEMAS_WITH_ARGS:
      return Common.MetaDataOperation.GET_SCHEMAS_WITH_ARGS;
    case GET_SUPER_TABLES:
      return Common.MetaDataOperation.GET_SUPER_TABLES;
    case GET_SUPER_TYPES:
      return Common.MetaDataOperation.GET_SUPER_TYPES;
    case GET_TABLES:
      return Common.MetaDataOperation.GET_TABLES;
    case GET_TABLE_PRIVILEGES:
      return Common.MetaDataOperation.GET_TABLE_PRIVILEGES;
    case GET_TABLE_TYPES:
      return Common.MetaDataOperation.GET_TABLE_TYPES;
    case GET_TYPE_INFO:
      return Common.MetaDataOperation.GET_TYPE_INFO;
    case GET_UDTS:
      return Common.MetaDataOperation.GET_UDTS;
    case GET_VERSION_COLUMNS:
      return Common.MetaDataOperation.GET_VERSION_COLUMNS;
    default:
      throw new RuntimeException("Unknown type: " + this);
    }
  }

  public static MetaDataOperation fromProto(Common.MetaDataOperation protoOp) {
    // Null is acceptable
    if (null == protoOp) {
      return null;
    }

    switch (protoOp) {
    case GET_ATTRIBUTES:
      return MetaDataOperation.GET_ATTRIBUTES;
    case GET_BEST_ROW_IDENTIFIER:
      return MetaDataOperation.GET_BEST_ROW_IDENTIFIER;
    case GET_CATALOGS:
      return MetaDataOperation.GET_CATALOGS;
    case GET_CLIENT_INFO_PROPERTIES:
      return MetaDataOperation.GET_CLIENT_INFO_PROPERTIES;
    case GET_COLUMNS:
      return MetaDataOperation.GET_COLUMNS;
    case GET_COLUMN_PRIVILEGES:
      return MetaDataOperation.GET_COLUMN_PRIVILEGES;
    case GET_CROSS_REFERENCE:
      return MetaDataOperation.GET_CROSS_REFERENCE;
    case GET_EXPORTED_KEYS:
      return MetaDataOperation.GET_EXPORTED_KEYS;
    case GET_FUNCTIONS:
      return MetaDataOperation.GET_FUNCTIONS;
    case GET_FUNCTION_COLUMNS:
      return MetaDataOperation.GET_FUNCTION_COLUMNS;
    case GET_IMPORTED_KEYS:
      return MetaDataOperation.GET_IMPORTED_KEYS;
    case GET_INDEX_INFO:
      return MetaDataOperation.GET_INDEX_INFO;
    case GET_PRIMARY_KEYS:
      return MetaDataOperation.GET_PRIMARY_KEYS;
    case GET_PROCEDURES:
      return MetaDataOperation.GET_PROCEDURES;
    case GET_PROCEDURE_COLUMNS:
      return MetaDataOperation.GET_PROCEDURE_COLUMNS;
    case GET_PSEUDO_COLUMNS:
      return MetaDataOperation.GET_PSEUDO_COLUMNS;
    case GET_SCHEMAS:
      return MetaDataOperation.GET_SCHEMAS;
    case GET_SCHEMAS_WITH_ARGS:
      return MetaDataOperation.GET_SCHEMAS_WITH_ARGS;
    case GET_SUPER_TABLES:
      return MetaDataOperation.GET_SUPER_TABLES;
    case GET_SUPER_TYPES:
      return MetaDataOperation.GET_SUPER_TYPES;
    case GET_TABLES:
      return MetaDataOperation.GET_TABLES;
    case GET_TABLE_PRIVILEGES:
      return MetaDataOperation.GET_TABLE_PRIVILEGES;
    case GET_TABLE_TYPES:
      return MetaDataOperation.GET_TABLE_TYPES;
    case GET_TYPE_INFO:
      return MetaDataOperation.GET_TYPE_INFO;
    case GET_UDTS:
      return MetaDataOperation.GET_UDTS;
    case GET_VERSION_COLUMNS:
      return MetaDataOperation.GET_VERSION_COLUMNS;
    default:
      throw new RuntimeException("Unknown type: " + protoOp);
    }
  }
}

// End MetaDataOperation.java
