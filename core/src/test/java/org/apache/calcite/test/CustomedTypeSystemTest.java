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

import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.fun.OracleSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.test.CustomedSqlTestFactory;
import org.apache.calcite.sql.test.DefaultSqlTestFactory;
import org.apache.calcite.sql.test.DelegatingSqlTestFactory;
import org.apache.calcite.sql.test.SqlOperatorBaseTest;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlTesterImpl;
import org.apache.calcite.sql.validate.SqlConformance;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Embodiment of {@link SqlOperatorBaseTest}
 * that verify shouldRaggedFixedLengthValueUnionBeVariable() in {@link SqlConformance}.
 */
public class CustomedTypeSystemTest extends SqlOperatorBaseTest {

  private static final SqlTester.VmName VM_JAVA = SqlTester.VmName.JAVA;
  /**
   * Creates a CustomedTypeSystemTest.
   */
  public CustomedTypeSystemTest() {
    super(false, tester(CustomedSqlTestFactory.INSTANCE));
  }

  @Test public void testLeastFunc() {
    tester.setFor(OracleSqlOperatorTable.LEAST);
    final SqlTester tester1 = oracleTester();
    tester1.checkString("least('on', 'earth')", "earth", "VARCHAR(5) NOT NULL");
    tester1.checkString("least('show', 'on', 'earth')", "earth",
        "VARCHAR(5) NOT NULL");
    tester1.checkScalar("least(12, CAST(NULL AS INTEGER), 3)", null, "INTEGER");
    tester1.checkScalar("least(false, true)", false, "BOOLEAN NOT NULL");
  }

  @Test public void testCase() {
    tester.setFor(SqlStdOperatorTable.CASE);
    tester.checkScalarExact("case when 'a'='a' then 1 end", "1");

    tester.checkString(
        "case 2 when 1 then 'a' when 2 then 'bcd' end",
        "bcd",
        "VARCHAR(3)");
    tester.checkString(
        "case 1 when 1 then 'a' when 2 then 'bcd' end",
        "a",
        "VARCHAR(3)");
    tester.checkString(
        "case 1 when 1 then cast('a' as varchar(1)) "
        + "when 2 then cast('bcd' as varchar(3)) end",
        "a",
        "VARCHAR(3)");
    if (DECIMAL) {
      tester.checkScalarExact(
          "case 2 when 1 then 11.2 when 2 then 4.543 else null end",
          "DECIMAL(5, 3)",
          "4.543");
      tester.checkScalarExact(
          "case 1 when 1 then 11.2 when 2 then 4.543 else null end",
          "DECIMAL(5, 3)",
          "11.200");
    }
    tester.checkScalarExact("case 'a' when 'a' then 1 end", "1");
    tester.checkScalarApprox(
        "case 1 when 1 then 11.2e0 when 2 then cast(4 as bigint) else 3 end",
        "DOUBLE NOT NULL",
        11.2,
        0);
    tester.checkScalarApprox(
        "case 1 when 1 then 11.2e0 when 2 then 4 else null end",
        "DOUBLE",
        11.2,
        0);
    tester.checkScalarApprox(
        "case 2 when 1 then 11.2e0 when 2 then 4 else null end",
        "DOUBLE",
        4,
        0);
    tester.checkScalarApprox(
        "case 1 when 1 then 11.2e0 when 2 then 4.543 else null end",
        "DOUBLE",
        11.2,
        0);
    tester.checkScalarApprox(
        "case 2 when 1 then 11.2e0 when 2 then 4.543 else null end",
        "DOUBLE",
        4.543,
        0);
    tester.checkNull("case 'a' when 'b' then 1 end");

    // Per spec, 'case x when y then ...'
    // translates to 'case when x = y then ...'
    // so nulls do not match.
    // (Unlike Oracle's 'decode(null, null, ...)', by the way.)
    tester.checkString(
        "case cast(null as int) when cast(null as int) then 'nulls match' else 'nulls do not match' end",
        "nulls do not match",
        "VARCHAR(18) NOT NULL");

    tester.checkScalarExact(
        "case when 'a'=cast(null as varchar(1)) then 1 else 2 end",
        "2");

    // equivalent to "nullif('a',cast(null as varchar(1)))"
    tester.checkString(
        "case when 'a' = cast(null as varchar(1)) then null else 'a' end",
        "a",
        "CHAR(1)");

    if (TODO) {
      tester.checkScalar(
          "case 1 when 1 then row(1,2) when 2 then row(2,3) end",
          "ROW(INTEGER NOT NULL, INTEGER NOT NULL)",
          "row(1,2)");
      tester.checkScalar(
          "case 1 when 1 then row('a','b') when 2 then row('ab','cd') end",
          "ROW(CHAR(2) NOT NULL, CHAR(2) NOT NULL)",
          "row('a ','b ')");
    }

    // multiple values in some cases (introduced in SQL:2011)
    tester.checkString(
        "case 1 "
        + "when 1, 2 then '1 or 2' "
        + "when 2 then 'not possible' "
        + "when 3, 2 then '3' "
        + "else 'none of the above' "
        + "end",
        "1 or 2",
        "VARCHAR(17) NOT NULL");
    tester.checkString(
        "case 2 "
        + "when 1, 2 then '1 or 2' "
        + "when 2 then 'not possible' "
        + "when 3, 2 then '3' "
        + "else 'none of the above' "
        + "end",
        "1 or 2",
        "VARCHAR(17) NOT NULL");
    tester.checkString(
        "case 3 "
        + "when 1, 2 then '1 or 2' "
        + "when 2 then 'not possible' "
        + "when 3, 2 then '3' "
        + "else 'none of the above' "
        + "end",
        "3",
        "VARCHAR(17) NOT NULL");
    tester.checkString(
        "case 4 "
        + "when 1, 2 then '1 or 2' "
        + "when 2 then 'not possible' "
        + "when 3, 2 then '3' "
        + "else 'none of the above' "
        + "end",
        "none of the above",
        "VARCHAR(17) NOT NULL");
  }

  @Test public void testGreatestFunc() {
    tester.setFor(OracleSqlOperatorTable.GREATEST);
    final SqlTester tester1 = oracleTester();
    tester1.checkString("greatest('on', 'earth')", "on   ", "VARCHAR(5) NOT NULL");
    tester1.checkString("greatest('show', 'on', 'earth')", "show ",
                        "VARCHAR(5) NOT NULL");
    tester1.checkScalar("greatest(12, CAST(NULL AS INTEGER), 3)", null, "INTEGER");
    tester1.checkScalar("greatest(false, true)", true, "BOOLEAN NOT NULL");
  }

  @Test public void testMapValueConstructor() {
    tester.setFor(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, VM_JAVA);

    tester.checkFails(
        "^Map[]^", "Map requires at least 2 arguments", false);

    tester.checkFails(
        "^Map[1, 'x', 2]^",
        "Map requires an even number of arguments",
        false);

    tester.checkFails(
        "^map[1, 1, 2, 'x']^", "Parameters must be of the same type",
        false);
    tester.checkScalarExact(
        "map['washington', 1, 'obama', 44]",
        "(VARCHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL",
        "{washington=1, obama=44}");
  }

  @Test public void testNvlFunc() {
    tester.setFor(OracleSqlOperatorTable.NVL);
    final SqlTester tester1 = oracleTester();
    tester1.checkScalar("nvl(1, 2)", "1", "INTEGER NOT NULL");
    tester1.checkFails("^nvl(1, true)^", "Parameters must be of the same type",
        false);
    tester1.checkScalar("nvl(true, false)", true, "BOOLEAN NOT NULL");
    tester1.checkScalar("nvl(false, true)", false, "BOOLEAN NOT NULL");
    tester1.checkString("nvl('abc', 'de')", "abc", "VARCHAR(3) NOT NULL");
    tester1.checkString("nvl('abc', 'defg')", "abc ", "VARCHAR(4) NOT NULL");
    tester1.checkString("nvl('abc', CAST(NULL AS VARCHAR(20)))", "abc",
        "VARCHAR(20) NOT NULL");
    tester1.checkString("nvl(CAST(NULL AS VARCHAR(20)), 'abc')", "abc",
        "VARCHAR(20) NOT NULL");
    tester1.checkNull(
        "nvl(CAST(NULL AS VARCHAR(6)), cast(NULL AS VARCHAR(4)))");
  }

  public static SqlTester tester() {
    return new TesterImpl(DefaultSqlTestFactory.INSTANCE);
  }

  public static SqlTester tester(SqlTestFactory sqlTestFactory) {
    return new TesterImpl(sqlTestFactory);
  }

  /**
   * Implementation of {@link org.apache.calcite.sql.test.SqlTester} based on a
   * JDBC connection.
   */
  protected static class TesterImpl extends SqlTesterImpl {
    public TesterImpl(SqlTestFactory testFactory) {
      super(testFactory);
    }

    @Override public void check(String query, TypeChecker typeChecker,
        ParameterChecker parameterChecker, ResultChecker resultChecker) {
      super.check(query, typeChecker, parameterChecker, resultChecker);
      //noinspection unchecked
      final CalciteAssert.ConnectionFactory connectionFactory =
          (CalciteAssert.ConnectionFactory)
              getFactory().get("connectionFactory");
      try (Connection connection = CalciteMetaImpl.connect(null,
          new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT,
              (SqlConformance) factory.get("conformance")));
           Statement statement = connection.createStatement()) {
        final ResultSet resultSet =
                statement.executeQuery(query);
        resultChecker.checkResult(resultSet);
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override protected SqlOperatorBaseTest.TesterImpl with(
        final String name2, final Object value) {
      return new SqlOperatorBaseTest.TesterImpl(
        new DelegatingSqlTestFactory(factory) {
          @Override public Object get(String name) {
            if (name.equals(name2)) {
              return value;
            }
            return super.get(name);
          }
        });
    }
  }
}

// End CustomedTypeSystemTest.java
