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
package org.apache.calcite.test;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;

/**
 * Unit test for SQL limits.
 */
public class SqlLimitsTest {
  //~ Static fields/initializers ---------------------------------------------

  //~ Constructors -----------------------------------------------------------

  public SqlLimitsTest() {
  }

  //~ Methods ----------------------------------------------------------------

  protected DiffRepository getDiffRepos() {
    return DiffRepository.lookup(SqlLimitsTest.class);
  }

  /** Returns a list of typical types. */
  public static List<RelDataType> getTypes(RelDataTypeFactory typeFactory) {
    final int maxPrecision =
        typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL);
    return ImmutableList.of(
        typeFactory.createSqlType(SqlTypeName.BOOLEAN),
        typeFactory.createSqlType(SqlTypeName.TINYINT),
        typeFactory.createSqlType(SqlTypeName.SMALLINT),
        typeFactory.createSqlType(SqlTypeName.INTEGER),
        typeFactory.createSqlType(SqlTypeName.BIGINT),
        typeFactory.createSqlType(SqlTypeName.DECIMAL),
        typeFactory.createSqlType(SqlTypeName.DECIMAL, 5),
        typeFactory.createSqlType(SqlTypeName.DECIMAL, 6, 2),
        typeFactory.createSqlType(SqlTypeName.DECIMAL, maxPrecision, 0),
        typeFactory.createSqlType(SqlTypeName.DECIMAL, maxPrecision, 5),

        // todo: test IntervalDayTime and IntervalYearMonth
        // todo: test Float, Real, Double

        typeFactory.createSqlType(SqlTypeName.CHAR, 5),
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 1),
        typeFactory.createSqlType(SqlTypeName.VARCHAR, 20),
        typeFactory.createSqlType(SqlTypeName.BINARY, 3),
        typeFactory.createSqlType(SqlTypeName.VARBINARY, 4),
        typeFactory.createSqlType(SqlTypeName.DATE),
        typeFactory.createSqlType(SqlTypeName.TIME, 0),
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 0));
  }

  @BeforeClass public static void setUSLocale() {
    // This ensures numbers in exceptions are printed as in asserts.
    // For example, 1,000 vs 1 000
    Locale.setDefault(Locale.US);
  }

  @Test public void testPrintLimits() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    final List<RelDataType> types =
        getTypes(new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
    for (RelDataType type : types) {
      pw.println(type.toString());
      printLimit(
          pw,
          "  min - epsilon:          ",
          type,
          false,
          SqlTypeName.Limit.OVERFLOW,
          true);
      printLimit(
          pw,
          "  min:                    ",
          type,
          false,
          SqlTypeName.Limit.OVERFLOW,
          false);
      printLimit(
          pw,
          "  zero - delta:           ",
          type,
          false,
          SqlTypeName.Limit.UNDERFLOW,
          false);
      printLimit(
          pw,
          "  zero - delta + epsilon: ",
          type,
          false,
          SqlTypeName.Limit.UNDERFLOW,
          true);
      printLimit(
          pw,
          "  zero:                   ",
          type,
          false,
          SqlTypeName.Limit.ZERO,
          false);
      printLimit(
          pw,
          "  zero + delta - epsilon: ",
          type,
          true,
          SqlTypeName.Limit.UNDERFLOW,
          true);
      printLimit(
          pw,
          "  zero + delta:           ",
          type,
          true,
          SqlTypeName.Limit.UNDERFLOW,
          false);
      printLimit(
          pw,
          "  max:                    ",
          type,
          true,
          SqlTypeName.Limit.OVERFLOW,
          false);
      printLimit(
          pw,
          "  max + epsilon:          ",
          type,
          true,
          SqlTypeName.Limit.OVERFLOW,
          true);
      pw.println();
    }
    pw.flush();
    getDiffRepos().assertEquals("output", "${output}", sw.toString());
  }

  private void printLimit(
      PrintWriter pw,
      String desc,
      RelDataType type,
      boolean sign,
      SqlTypeName.Limit limit,
      boolean beyond) {
    Object o = ((BasicSqlType) type).getLimit(sign, limit, beyond);
    if (o == null) {
      return;
    }
    pw.print(desc);
    String s;
    if (o instanceof byte[]) {
      int k = 0;
      StringBuilder buf = new StringBuilder("{");
      for (byte b : (byte[]) o) {
        if (k++ > 0) {
          buf.append(", ");
        }
        buf.append(Integer.toHexString(b & 0xff));
      }
      buf.append("}");
      s = buf.toString();
    } else if (o instanceof Calendar) {
      Calendar calendar = (Calendar) o;
      DateFormat dateFormat = getDateFormat(type.getSqlTypeName());
      dateFormat.setTimeZone(DateTimeUtils.UTC_ZONE);
      s = dateFormat.format(calendar.getTime());
    } else {
      s = o.toString();
    }
    pw.print(s);
    SqlLiteral literal =
        type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
    pw.print("; as SQL: ");
    pw.print(literal.toSqlString(AnsiSqlDialect.DEFAULT));
    pw.println();
  }

  private DateFormat getDateFormat(SqlTypeName typeName) {
    switch (typeName) {
    case DATE:
      return new SimpleDateFormat("MMM d, yyyy", Locale.ROOT);
    case TIME:
      return new SimpleDateFormat("hh:mm:ss a", Locale.ROOT);
    default:
      return new SimpleDateFormat("MMM d, yyyy hh:mm:ss a", Locale.ROOT);
    }
  }
}

// End SqlLimitsTest.java
