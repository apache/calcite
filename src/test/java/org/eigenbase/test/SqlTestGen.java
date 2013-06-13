/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.test;

import java.io.*;

import java.lang.reflect.*;

import java.nio.charset.*;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * Utility to generate a SQL script from validator test.
 */
public class SqlTestGen {
    //~ Methods ----------------------------------------------------------------

    public static void main(String [] args)
    {
        new SqlTestGen().genValidatorTest();
    }

    private void genValidatorTest()
    {
        FileOutputStream fos = null;
        PrintWriter pw = null;
        try {
            File file = new File("validatorTest.sql");
            fos = new FileOutputStream(file);
            pw = new PrintWriter(fos);
            Method [] methods = getJunitMethods(SqlValidatorSpooler.class);
            for (Method method : methods) {
                final SqlValidatorSpooler test = new SqlValidatorSpooler(pw);
                final Object result = method.invoke(test);
                assert result == null;
            }
        } catch (IOException e) {
            throw Util.newInternal(e);
        } catch (IllegalAccessException e) {
            throw Util.newInternal(e);
        } catch (IllegalArgumentException e) {
            throw Util.newInternal(e);
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw Util.newInternal(e);
        } finally {
            if (pw != null) {
                pw.flush();
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    throw Util.newInternal(e);
                }
            }
        }
    }

    /**
     * Returns a list of all of the Junit methods in a given class.
     */
    private static Method [] getJunitMethods(Class<SqlValidatorSpooler> clazz)
    {
        List<Method> list = new ArrayList<Method>();
        for (Method method : clazz.getMethods()) {
            if (method.getName().startsWith("test")
                && Modifier.isPublic(method.getModifiers())
                && !Modifier.isStatic(method.getModifiers())
                && (method.getParameterTypes().length == 0)
                && (method.getReturnType() == Void.TYPE))
            {
                list.add(method);
            }
        }
        return list.toArray(new Method[list.size()]);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Subversive subclass, which spools results to a writer rather than running
     * tests.
     */
    private static class SqlValidatorSpooler
        extends SqlValidatorTest
    {
        private final PrintWriter pw;

        private SqlValidatorSpooler(PrintWriter pw) {
            this.pw = pw;
        }

        public SqlValidatorTestCase.Tester getTester(
            SqlConformance conformance)
        {
            return new TesterImpl(conformance) {
                public SqlValidator getValidator()
                {
                    throw new UnsupportedOperationException();
                }

                public void assertExceptionIsThrown(
                    String sql,
                    String expectedMsgPattern)
                {
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

                public RelDataType getColumnType(String sql)
                {
                    return null;
                }

                public void checkType(
                    String sql,
                    String expected)
                {
                    // We could generate the SQL -- or maybe describe -- but
                    // ignore it for now.
                }

                public void checkCollation(
                    String sql,
                    String expectedCollationName,
                    SqlCollation.Coercibility expectedCoercibility)
                {
                    // We could generate the SQL -- or maybe describe -- but
                    // ignore it for now.
                }

                public void checkCharset(
                    String sql,
                    Charset expectedCharset)
                {
                    // We could generate the SQL -- or maybe describe -- but
                    // ignore it for now.
                }
            };
        }
    }
}

// End SqlTestGen.java
