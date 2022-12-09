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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataTypeSystem;

import java.util.Objects;

import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

/**
 * TZAwareSqlType represents a Bodo SQL type that contains
 * tz information.
 */
public class TZAwareSqlType extends AbstractSqlType {
  private final RelDataTypeSystem typeSystem;
  // The tz info
  private final BodoTZInfo tzInfo;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs a tzAware sql type. This should only be called from a factory
   * method.
   *
   * @param typeSystem Type system
   * @param tzInfo Timezone information
   * @param nullable Does this contain null values?
   */
  public TZAwareSqlType(RelDataTypeSystem typeSystem, BodoTZInfo tzInfo, boolean nullable) {
    super(TIMESTAMP_WITH_LOCAL_TIME_ZONE, nullable, null);
    this.typeSystem = Objects.requireNonNull(typeSystem, "typeSystem");
    this.tzInfo = Objects.requireNonNull(tzInfo, "tzInfo");
    computeDigest();
  }

  /**
   * Generates a string representation of this type.
   *
   * @param sb         StringBuilder into which to generate the string
   * @param withDetail when true, all detail information needed to compute a
   *                   unique digest (and return from getFullTypeString) should
   *                   be included;
   */
  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("TIMESTAMP(");
    sb.append(tzInfo.getPyZone());
    sb.append(")");
  }

  @Override public BodoTZInfo getTZInfo() {
    return tzInfo;
  }
}
