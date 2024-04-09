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
package org.apache.calcite.sql.fun;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableSet;

import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getOperandLiteralValueOrThrow;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * The SQL <code>EXTRACT</code> operator. Extracts a specified field value from
 * a DATETIME or an INTERVAL. E.g.<br>
 * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>
 * 23</code>
 */
public class SqlExtractFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  // SQL2003, Part 2, Section 4.4.3 - extract returns a exact numeric
  // TODO: Return type should be decimal for seconds
  public SqlExtractFunction(String name) {
    super(name, SqlKind.EXTRACT, ReturnTypes.BIGINT_NULLABLE, null,
        OperandTypes.INTERVALINTERVAL_INTERVALDATETIME,
        SqlFunctionCategory.SYSTEM);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public String getSignatureTemplate(int operandsCount) {
    Util.discard(operandsCount);
    return "{0}({1} FROM {2})";
  }

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    SqlIntervalQualifier.asIdentifier(call.operand(0))
        .unparse(writer, 0, 0);
    writer.sep("FROM");
    call.operand(1).unparse(writer, 0, 0);
    writer.endFunCall(frame);
  }

  // List of types that support EXTRACT(X, ...) where X is MONTH or larger
  private static final ImmutableSet<SqlTypeName> MONTH_AND_ABOVE_TYPES =
      new ImmutableSet.Builder<SqlTypeName>()
      .add(SqlTypeName.DATE)
      .add(SqlTypeName.TIMESTAMP)
      .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
      .addAll(SqlTypeName.YEAR_INTERVAL_TYPES)
      .build();

  // List of types that support EXTRACT(X, ...) where X is between DAY and WEEK
  private static final ImmutableSet<SqlTypeName> DAY_TO_WEEK_TYPES =
      new ImmutableSet.Builder<SqlTypeName>()
          .add(SqlTypeName.DATE)
          .add(SqlTypeName.TIMESTAMP)
          .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
          .build();

  // List of types that support EXTRACT(EPOCH, ...)
  private static final ImmutableSet<SqlTypeName> EPOCH_TYPES =
      new ImmutableSet.Builder<SqlTypeName>()
          .add(SqlTypeName.DATE)
          .add(SqlTypeName.TIMESTAMP)
          .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
          .addAll(SqlTypeName.YEAR_INTERVAL_TYPES)
          .addAll(SqlTypeName.DAY_INTERVAL_TYPES)
          .build();

  // List of types that support EXTRACT(DAY, ...)
  private static final ImmutableSet<SqlTypeName> DAY_TYPES =
      new ImmutableSet.Builder<SqlTypeName>()
          .add(SqlTypeName.DATE)
          .add(SqlTypeName.TIMESTAMP)
          .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
          .add(SqlTypeName.INTERVAL_DAY)
          .add(SqlTypeName.INTERVAL_DAY_HOUR)
          .add(SqlTypeName.INTERVAL_DAY_MINUTE)
          .add(SqlTypeName.INTERVAL_DAY_SECOND)
          .addAll(SqlTypeName.YEAR_INTERVAL_TYPES)
          .build();

  // List of types that support EXTRACT(X, ...) where X is
  // between HOUR and NANOSECOND
  private static final ImmutableSet<SqlTypeName> HOUR_TO_NANOSECOND_TYPES =
      new ImmutableSet.Builder<SqlTypeName>()
          .add(SqlTypeName.DATE)
          .add(SqlTypeName.TIMESTAMP)
          .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
          .add(SqlTypeName.TIME)
          .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
          .addAll(SqlTypeName.YEAR_INTERVAL_TYPES)
          .addAll(SqlTypeName.DAY_INTERVAL_TYPES)
          .build();

  @Override public void validateCall(SqlCall call, SqlValidator validator,
      SqlValidatorScope scope, SqlValidatorScope operandScope) {
    super.validateCall(call, validator, scope, operandScope);

    // This is either a time unit or a time frame:
    //
    //  * In "EXTRACT(YEAR FROM x)" operand 0 is a SqlIntervalQualifier with
    //    startUnit = YEAR and timeFrameName = null.
    //
    //  * In "EXTRACT(MINUTE15 FROM x)" operand 0 is a SqlIntervalQualifier with
    //    startUnit = EPOCH and timeFrameName = 'MINUTE15'.
    //
    // If the latter, check that timeFrameName is valid.
    SqlIntervalQualifier qualifier = call.operand(0);
    validator.validateTimeFrame(qualifier);
    TimeUnitRange range = qualifier.timeUnitRange;

    RelDataType type = validator.getValidatedNodeTypeIfKnown(call.operand(1));
    if (type == null) {
      return;
    }

    SqlTypeName typeName = type.getSqlTypeName();
    boolean legal;
    switch (range) {
    case YEAR:
    case MONTH:
    case ISOYEAR:
    case QUARTER:
    case DECADE:
    case CENTURY:
    case MILLENNIUM:
      legal = MONTH_AND_ABOVE_TYPES.contains(typeName);
      break;
    case WEEK:
    case DOW:
    case ISODOW:
    case DOY:
      legal = DAY_TO_WEEK_TYPES.contains(typeName);
      break;
    case EPOCH:
      legal = EPOCH_TYPES.contains(typeName);
      break;
    case DAY:
      legal = DAY_TYPES.contains(typeName);
      break;
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
    case NANOSECOND:
      legal = HOUR_TO_NANOSECOND_TYPES.contains(typeName);
      break;
    case YEAR_TO_MONTH:
    case DAY_TO_HOUR:
    case DAY_TO_MINUTE:
    case DAY_TO_SECOND:
    case HOUR_TO_MINUTE:
    case HOUR_TO_SECOND:
    case MINUTE_TO_SECOND:
    default:
      legal = false;
      break;
    }

    if (!legal) {
      throw validator.newValidationError(call,
          RESOURCE.canNotApplyOp2Type(call.getOperator().getName(),
              call.getCallSignature(validator, scope),
              call.getOperator().getAllowedSignatures()));
    }
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    TimeUnitRange value = getOperandLiteralValueOrThrow(call, 0, TimeUnitRange.class);
    switch (value) {
    case YEAR:
      return call.getOperandMonotonicity(1).unstrict();
    default:
      return SqlMonotonicity.NOT_MONOTONIC;
    }
  }
}
