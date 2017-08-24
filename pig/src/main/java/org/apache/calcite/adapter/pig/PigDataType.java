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
package org.apache.calcite.adapter.pig;

import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.pig.data.DataType;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

/**
 * Supported Pig data types and their Calcite counterparts.
 */
public enum PigDataType {

  CHARARRAY(DataType.CHARARRAY, VARCHAR);

  private byte pigType; // Pig defines types using bytes
  private SqlTypeName sqlType;

  PigDataType(byte pigType, SqlTypeName sqlType) {
    this.pigType = pigType;
    this.sqlType = sqlType;
  }

  public byte getPigType() {
    return pigType;
  }

  public SqlTypeName getSqlType() {
    return sqlType;
  }

  public static PigDataType valueOf(byte pigType) {
    for (PigDataType pigDataType : values()) {
      if (pigDataType.pigType == pigType) {
        return pigDataType;
      }
    }
    throw new IllegalArgumentException(
        "Pig data type " + DataType.findTypeName(pigType) + " is not supported");
  }

  public static PigDataType valueOf(SqlTypeName sqlType) {
    for (PigDataType pigDataType : values()) {
      if (pigDataType.sqlType == sqlType) {
        return pigDataType;
      }
    }
    throw new IllegalArgumentException("SQL data type " + sqlType + " is not supported");
  }
}

// End PigDataType.java
