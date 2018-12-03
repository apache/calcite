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
package org.apache.calcite.adapter.druid;

import org.apache.calcite.sql.type.SqlTypeName;

/** Druid type. */
public enum DruidType {
  LONG(SqlTypeName.BIGINT),
  FLOAT(SqlTypeName.FLOAT),
  DOUBLE(SqlTypeName.DOUBLE),
  STRING(SqlTypeName.VARCHAR),
  COMPLEX(SqlTypeName.OTHER),
  HYPER_UNIQUE(SqlTypeName.VARBINARY),
  THETA_SKETCH(SqlTypeName.VARBINARY);

  /** The corresponding SQL type. */
  public final SqlTypeName sqlType;

  DruidType(SqlTypeName sqlType) {
    this.sqlType = sqlType;
  }

  /**
   * Returns true if and only if this enum should be used inside of a {@link ComplexMetric}
   * */
  public boolean isComplex() {
    return this == THETA_SKETCH || this == HYPER_UNIQUE || this == COMPLEX;
  }

  /**
   * Returns a DruidType matching the given String type from a Druid metric
   * */
  protected static DruidType getTypeFromMetric(String type) {
    assert type != null;
    if (type.equals("hyperUnique")) {
      return HYPER_UNIQUE;
    } else if (type.equals("thetaSketch")) {
      return THETA_SKETCH;
    } else if (type.startsWith("long") || type.equals("count")) {
      return LONG;
    } else if (type.startsWith("double")) {
      return DOUBLE;
    } else if (type.startsWith("float")) {
      return FLOAT;
    }
    throw new AssertionError("Unknown type: " + type);
  }

  /**
   * Returns a DruidType matching the String from a meta data query
   * */
  protected static DruidType getTypeFromMetaData(String type) {
    assert type != null;
    switch (type) {
    case "LONG":
      return LONG;
    case "FLOAT":
      return FLOAT;
    case "DOUBLE":
      return DOUBLE;
    case "STRING":
      return STRING;
    default:
      // Likely a sketch, or a type String from the aggregations field.
      return getTypeFromMetric(type);
    }
  }
}

// End DruidType.java
