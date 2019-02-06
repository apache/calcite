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

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.TimeZone;
import javax.annotation.Nullable;

/**
 * DruidSqlOperatorConverter implementation that handles Floor operations conversions
 */
public class FloorOperatorConversion implements DruidSqlOperatorConverter {
  @Override public SqlOperator calciteOperator() {
    return SqlStdOperatorTable.FLOOR;
  }

  @Nullable
  @Override public String toDruidExpression(RexNode rexNode, RelDataType rowType,
      DruidQuery druidQuery) {
    final RexCall call = (RexCall) rexNode;
    final RexNode arg = call.getOperands().get(0);
    final String druidExpression = DruidExpressions.toDruidExpression(
        arg,
        rowType,
        druidQuery);
    if (druidExpression == null) {
      return null;
    } else if (call.getOperands().size() == 1) {
      // case FLOOR(expr)
      return  DruidQuery.format("floor(%s)", druidExpression);
    } else if (call.getOperands().size() == 2) {
      // FLOOR(expr TO timeUnit)
      final TimeZone tz;
      if (arg.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
        tz = TimeZone.getTimeZone(druidQuery.getConnectionConfig().timeZone());
      } else {
        tz = DateTimeUtils.UTC_ZONE;
      }
      final Granularity granularity = DruidDateTimeUtils
          .extractGranularity(call, tz.getID());
      if (granularity == null) {
        return null;
      }
      String isoPeriodFormat = DruidDateTimeUtils.toISOPeriodFormat(granularity.getType());
      if (isoPeriodFormat == null) {
        return null;
      }
      return DruidExpressions.applyTimestampFloor(
          druidExpression,
          isoPeriodFormat,
          "",
          tz);
    } else {
      return null;
    }
  }
}

// End FloorOperatorConversion.java
