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

import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.BarfingInvocationHandler;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility to generate a SQL script from validator test.
 */
public class SqlTestGen {
  private SqlTestGen() {}

  //~ Methods ----------------------------------------------------------------

  public static void main(String[] args) {
    new SqlTestGen().genValidatorTest();
  }

  private void genValidatorTest() {
    final File file = new File("validatorTest.sql");
    try (PrintWriter pw = Util.printWriter(file)) {
      Method[] methods = getJunitMethods(SqlValidatorSpooler.class);
      for (Method method : methods) {
        final SqlValidatorSpooler test = new SqlValidatorSpooler(pw);
        final Object result = method.invoke(test);
        assert result == null;
      }
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }
  }

  /**
   * Returns a list of all of the Junit methods in a given class.
   */
  private static Method[] getJunitMethods(Class<SqlValidatorSpooler> clazz) {
    List<Method> list = new ArrayList<>();
    for (Method method : clazz.getMethods()) {
      if (method.getName().startsWith("test")
          && Modifier.isPublic(method.getModifiers())
          && !Modifier.isStatic(method.getModifiers())
          && (method.getParameterTypes().length == 0)
          && (method.getReturnType() == Void.TYPE)) {
        list.add(method);
      }
    }
    return list.toArray(new Method[0]);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Subversive subclass, which spools results to a writer rather than running
   * tests.
   */
  private static class SqlValidatorSpooler extends SqlValidatorTest {
    private static final SqlTestFactory SPOOLER_VALIDATOR = SqlTestFactory.INSTANCE.withValidator(
        (opTab, catalogReader, typeFactory, conformance) ->
            (SqlValidator) Proxy.newProxyInstance(
                SqlValidatorSpooler.class.getClassLoader(),
                new Class[]{SqlValidator.class},
                new MyInvocationHandler()));

    private final PrintWriter pw;

    private SqlValidatorSpooler(PrintWriter pw) {
      this.pw = pw;
    }

    public SqlTester getTester() {
      return new SqlValidatorTester(SPOOLER_VALIDATOR) {
        public void assertExceptionIsThrown(
            String sql,
            String expectedMsgPattern) {
          if (expectedMsgPattern == null) {
            // This SQL statement is supposed to succeed.
            // Generate it to the file, so we can see what
            // output it produces.
            pw.println("-- " /* + getName() */);
            pw.println(sql);
            pw.println(";");
          } else {
            // Do nothing. We know that this fails the validator
            // test, so we don't learn anything by having it fail
            // from SQL.
          }
        }

        @Override public void checkColumnType(String sql, String expected) {
        }

        @Override public void checkResultType(String sql, String expected) {
        }

        public void checkType(
            String sql,
            String expected) {
          // We could generate the SQL -- or maybe describe -- but
          // ignore it for now.
        }

        public void checkCollation(
            String expression,
            String expectedCollationName,
            SqlCollation.Coercibility expectedCoercibility) {
          // We could generate the SQL -- or maybe describe -- but
          // ignore it for now.
        }

        public void checkCharset(
            String expression,
            Charset expectedCharset) {
          // We could generate the SQL -- or maybe describe -- but
          // ignore it for now.
        }

        @Override public void checkIntervalConv(String sql, String expected) {
        }

        @Override public void checkRewrite(
            SqlValidator validator,
            String query,
            String expectedRewrite) {
        }

        @Override public void checkFieldOrigin(
            String sql,
            String fieldOriginList) {
        }
      };
    }

    /**
     * Handles the methods in
     * {@link org.apache.calcite.sql.validate.SqlValidator} that are called
     * from validator tests.
     */
    public static class MyInvocationHandler extends BarfingInvocationHandler {
      public void setIdentifierExpansion(boolean b) {
      }

      public void setColumnReferenceExpansion(boolean b) {
      }

      public void setCallRewrite(boolean b) {
      }

      public boolean shouldExpandIdentifiers() {
        return true;
      }
    }
  }
}

// End SqlTestGen.java
