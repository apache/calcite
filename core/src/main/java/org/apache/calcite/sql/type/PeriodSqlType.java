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

import org.apache.calcite.rel.type.RelDataTypeField;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * SQL Period type.
 */
public class PeriodSqlType extends AbstractSqlType {

  /**
   * Creates an PeriodSqlType. This constructor should only be called
   * from a factory method.
   */
  public PeriodSqlType(boolean isNullable, @Nullable List<? extends RelDataTypeField> fields) {
    super(SqlTypeName.PERIOD, isNullable, fields);
    assert checkValidOperands(fields);
    computeDigest();
  }

  @Override protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("Period(");
    for (int i = 0; i < Objects.requireNonNull(fieldList, "fieldList").size(); i++) {
      if (withDetail) {
        sb.append(fieldList.get(i).getType().getFullTypeString());
      } else {
        sb.append(fieldList.get(i).getType().toString());
      }
      if (i == 0) {
        sb.append(", ");
      }
    }
    sb.append(")");
  }

  private boolean checkValidOperands(@Nullable List<? extends RelDataTypeField> fields) {
    if (fields == null || fields.size() != 2) {
      return false;
    }
    return SqlTypeUtil.isDatetime(fields.get(0).getType())
        && SqlTypeUtil.isDatetime(fields.get(1).getType());
  }
}
