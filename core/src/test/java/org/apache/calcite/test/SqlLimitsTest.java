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
package org.eigenbase.test;

import java.io.*;
import java.text.*;
import java.util.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeSystem;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;

import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;

import com.google.common.collect.ImmutableList;

import org.junit.BeforeClass;
import org.junit.Test;

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
      DateFormat dateFormat;
      switch (type.getSqlTypeName()) {
      case DATE:
        dateFormat = DateFormat.getDateInstance();
        break;
      case TIME:
        dateFormat = DateFormat.getTimeInstance();
        break;
      default:
        dateFormat = DateFormat.getDateTimeInstance();
        break;
      }
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
      s = dateFormat.format(calendar.getTime());
    } else {
      s = o.toString();
    }
    pw.print(s);
    SqlLiteral literal =
        type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
    pw.print("; as SQL: ");
    pw.print(literal.toSqlString(SqlDialect.DUMMY));
    pw.println();
  }
}

// End SqlLimitsTest.java
