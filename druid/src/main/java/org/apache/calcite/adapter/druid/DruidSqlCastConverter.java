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

import com.google.common.collect.ImmutableList;

import org.joda.time.Period;

import java.util.TimeZone;

/**
 * Druid cast converter operator used to translates calcite casts to Druid expression cast
 */
public class DruidSqlCastConverter implements DruidSqlOperatorConverter {

  @Override public SqlOperator calciteOperator() {
    return SqlStdOperatorTable.CAST;
  }

  @Override public String toDruidExpression(RexNode rexNode, RelDataType topRel,
      DruidQuery druidQuery) {

    final RexNode operand = ((RexCall) rexNode).getOperands().get(0);
    final String operandExpression = DruidExpressions.toDruidExpression(operand,
        topRel, druidQuery);

    if (operandExpression == null) {
      return null;
    }

    final SqlTypeName fromType = operand.getType().getSqlTypeName();
    String fromTypeString = dateTimeFormatString(fromType);
    final SqlTypeName toType = rexNode.getType().getSqlTypeName();
    final String timeZoneConf = druidQuery.getConnectionConfig().timeZone();
    final TimeZone timeZone = TimeZone.getTimeZone(timeZoneConf == null ? "UTC" : timeZoneConf);
    final boolean nullEqualToEmpty = druidQuery.getConnectionConfig().nullEqualToEmpty();

    if (fromTypeString == null) {
      fromTypeString = nullEqualToEmpty ? "" :  null;
    }

    if (SqlTypeName.CHAR_TYPES.contains(fromType)
        && SqlTypeName.DATETIME_TYPES.contains(toType)) {
      //case chars to dates
      return castCharToDateTime(toType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
              ? timeZone : DateTimeUtils.UTC_ZONE,
          operandExpression, toType, fromTypeString);
    } else if (SqlTypeName.DATETIME_TYPES.contains(fromType)
        && SqlTypeName.CHAR_TYPES.contains(toType)) {
      //case dates to chars
      return castDateTimeToChar(fromType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
          ? timeZone : DateTimeUtils.UTC_ZONE, operandExpression, fromType);
    } else if (SqlTypeName.DATETIME_TYPES.contains(fromType)
        && toType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      if (timeZone.equals(DateTimeUtils.UTC_ZONE)) {
        // bail out, internal representation is the same,
        // we do not need to do anything
        return operandExpression;
      }
      // to timestamp with local time zone
      return castCharToDateTime(
          timeZone,
          castDateTimeToChar(DateTimeUtils.UTC_ZONE, operandExpression, fromType),
          toType,
          fromTypeString);
    } else if (fromType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        && SqlTypeName.DATETIME_TYPES.contains(toType)) {
      if (toType != SqlTypeName.DATE && timeZone.equals(DateTimeUtils.UTC_ZONE)) {
        // bail out, internal representation is the same,
        // we do not need to do anything
        return operandExpression;
      }
      // timestamp with local time zone to other types
      return castCharToDateTime(
          DateTimeUtils.UTC_ZONE,
          castDateTimeToChar(timeZone, operandExpression, fromType),
          toType,
          fromTypeString);
    } else {
      // Handle other casts.
      final DruidType fromExprType = DruidExpressions.EXPRESSION_TYPES.get(fromType);
      final DruidType toExprType = DruidExpressions.EXPRESSION_TYPES.get(toType);

      if (fromExprType == null || toExprType == null) {
        // Unknown types bail out.
        return null;
      }
      final String typeCastExpression;
      if (fromExprType != toExprType) {
        typeCastExpression = DruidQuery.format("CAST(%s, '%s')", operandExpression,
            toExprType
            .toString());
      } else {
        // case it is the same type it is ok to skip CAST
        typeCastExpression = operandExpression;
      }

      if (toType == SqlTypeName.DATE) {
        // Floor to day when casting to DATE.
        return DruidExpressions.applyTimestampFloor(
            typeCastExpression,
            Period.days(1).toString(),
            "",
            TimeZone.getTimeZone(druidQuery.getConnectionConfig().timeZone()));
      } else {
        return typeCastExpression;
      }

    }
  }

  private static String castCharToDateTime(
      TimeZone timeZone,
      String operand,
      final SqlTypeName toType, String format) {
    // Cast strings to date times by parsing them from SQL format.
    final String timestampExpression = DruidExpressions.functionCall(
        "timestamp_parse",
        ImmutableList.of(
            operand,
            DruidExpressions.stringLiteral(format),
            DruidExpressions.stringLiteral(timeZone.getID())));

    if (toType == SqlTypeName.DATE) {
      // case to date we need to floor to day first
      return DruidExpressions.applyTimestampFloor(
          timestampExpression,
          Period.days(1).toString(),
          "",
          timeZone);
    } else if (toType == SqlTypeName.TIMESTAMP
        || toType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return timestampExpression;
    } else {
      throw new IllegalStateException(
          DruidQuery.format("Unsupported DateTime type[%s]", toType));
    }
  }

  private static String castDateTimeToChar(
      final TimeZone timeZone,
      final String operand,
      final SqlTypeName fromType) {
    return DruidExpressions.functionCall(
        "timestamp_format",
        ImmutableList.of(
            operand,
            DruidExpressions.stringLiteral(dateTimeFormatString(fromType)),
            DruidExpressions.stringLiteral(timeZone.getID())));
  }

  public static String dateTimeFormatString(final SqlTypeName sqlTypeName) {
    if (sqlTypeName == SqlTypeName.DATE) {
      return "yyyy-MM-dd";
    } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
      return "yyyy-MM-dd HH:mm:ss";
    } else if (sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    } else {
      return null;
    }
  }
}

// End DruidSqlCastConverter.java
