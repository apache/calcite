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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlFloorFunction;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.util.RelToSqlConverterUtil.unparseSparkArrayAndMap;

/**
 * A <code>SqlDialect</code> implementation for the Doris database.
 */
public class DorisSqlDialect extends StarRocksSqlDialect {
  public static final SqlDialect DEFAULT = new DorisSqlDialect(DEFAULT_CONTEXT);

  /**
   * Creates a DorisSqlDialect.
   */
  public DorisSqlDialect(Context context) {
    super(context);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    switch (call.getKind()) {
    case MAP_VALUE_CONSTRUCTOR:
      unparseSparkArrayAndMap(writer, call, leftPrec, rightPrec);
      break;
    case FLOOR:
      if (call.operandCount() != 2) {
        super.unparseCall(writer, call, leftPrec, rightPrec);
        return;
      }
      final SqlLiteral timeUnitNode = call.operand(1);
      final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);
      SqlCall newCall =
          SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
              timeUnitNode.getParserPosition());
      SqlFloorFunction.unparseDatetimeFunction(writer, newCall, "DATE_TRUNC", true);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
      break;
    }
  }

  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case UTINYINT:
    case USMALLINT:
    case UINTEGER:
    case UBIGINT:
      throw new RuntimeException(
          "Doris doesn't support UNSIGNED TINYINT/SMALLINT/INTEGER/BIGINT!");
    default:
      break;
    }
    return super.getCastSpec(type);
  }
}
