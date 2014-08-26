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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;

/**
 * IntervalSqlType represents a standard SQL datetime interval type.
 */
public class IntervalSqlType extends AbstractSqlType {
  //~ Instance fields --------------------------------------------------------

  private SqlIntervalQualifier intervalQualifier;

  //~ Constructors -----------------------------------------------------------

  /**
   * Constructs an IntervalSqlType. This should only be called from a factory
   * method.
   */
  public IntervalSqlType(
      SqlIntervalQualifier intervalQualifier,
      boolean isNullable) {
    super(
        intervalQualifier.isYearMonth() ? SqlTypeName.INTERVAL_YEAR_MONTH
            : SqlTypeName.INTERVAL_DAY_TIME,
        isNullable,
        null);
    this.intervalQualifier = intervalQualifier;
    computeDigest();
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelDataTypeImpl
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("INTERVAL ");
    sb.append(intervalQualifier.toString());
  }

  // implement RelDataType
  public SqlIntervalQualifier getIntervalQualifier() {
    return intervalQualifier;
  }

  /**
   * Combines two IntervalTypes and returns the result. E.g. the result of
   * combining<br>
   * <code>INTERVAL DAY TO HOUR</code><br>
   * with<br>
   * <code>INTERVAL SECOND</code> is<br>
   * <code>INTERVAL DAY TO SECOND</code>
   */
  public IntervalSqlType combine(
      RelDataTypeFactoryImpl typeFactory,
      IntervalSqlType that) {
    assert this.intervalQualifier.isYearMonth()
        == that.intervalQualifier.isYearMonth();
    boolean nullable = isNullable || that.isNullable;
    SqlIntervalQualifier.TimeUnit thisStart =
        this.intervalQualifier.getStartUnit();
    SqlIntervalQualifier.TimeUnit thisEnd =
        this.intervalQualifier.getEndUnit();
    SqlIntervalQualifier.TimeUnit thatStart =
        that.intervalQualifier.getStartUnit();
    SqlIntervalQualifier.TimeUnit thatEnd =
        that.intervalQualifier.getEndUnit();

    assert null != thisStart;
    assert null != thatStart;

    int secondPrec =
        this.intervalQualifier.getStartPrecisionPreservingDefault();
    int fracPrec =
        SqlIntervalQualifier
            .combineFractionalSecondPrecisionPreservingDefault(
                this.intervalQualifier,
                that.intervalQualifier);

    if (thisStart.ordinal() > thatStart.ordinal()) {
      thisEnd = thisStart;
      thisStart = thatStart;
      secondPrec =
          that.intervalQualifier.getStartPrecisionPreservingDefault();
    } else if (thisStart.ordinal() == thatStart.ordinal()) {
      secondPrec =
          SqlIntervalQualifier.combineStartPrecisionPreservingDefault(
              this.intervalQualifier,
              that.intervalQualifier);
    } else if (
        (null == thisEnd)
            || (thisEnd.ordinal() < thatStart.ordinal())) {
      thisEnd = thatStart;
    }

    if (null != thatEnd) {
      if ((null == thisEnd)
          || (thisEnd.ordinal() < thatEnd.ordinal())) {
        thisEnd = thatEnd;
      }
    }

    RelDataType intervalType =
        typeFactory.createSqlIntervalType(
            new SqlIntervalQualifier(
                thisStart,
                secondPrec,
                thisEnd,
                fracPrec,
                SqlParserPos.ZERO));
    intervalType =
        typeFactory.createTypeWithNullability(
            intervalType,
            nullable);
    return (IntervalSqlType) intervalType;
  }

  // implement RelDataType
  public int getPrecision() {
    return intervalQualifier.getStartPrecision();
  }

  @Override
  public int getScale() {
    // TODO Auto-generated method stub
    return intervalQualifier.getFractionalSecondPrecision();
  }

}

// End IntervalSqlType.java
