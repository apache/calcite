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
package net.hydromatic.optiq.test;

import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.impl.jdbc.JdbcQueryProvider;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import junit.framework.Assert;
import junit.framework.TestSuite;

import org.eigenbase.sql.test.SqlOperatorTest;
import org.eigenbase.test.SqlToRelConverterTest;
import org.eigenbase.util.Bug;
import org.eigenbase.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;

/**
 * Fluid DSL for testing Optiq connections and queries.
 *
 * @author jhyde
 */
public class OptiqAssert {
    public static AssertThat assertThat() {
        return new AssertThat(Config.REGULAR);
    }

    /** Returns a {@link junit} suite of all Optiq tests. */
    public static TestSuite suite() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTestSuite(JdbcTest.class);
        testSuite.addTestSuite(LinqFrontJdbcBackTest.class);
        testSuite.addTestSuite(JdbcFrontLinqBackTest.class);
        testSuite.addTestSuite(JdbcFrontJdbcBackLinqMiddleTest.class);
        testSuite.addTestSuite(JdbcFrontJdbcBackTest.class);
        testSuite.addTestSuite(SqlToRelConverterTest.class);
        testSuite.addTestSuite(SqlFunctionsTest.class);
        testSuite.addTestSuite(SqlOperatorTest.class);
        if (Bug.TodoFixed) {
            // 96 failures currently
            testSuite.addTestSuite(OptiqSqlOperatorTest.class);
        }
        return testSuite;
    }

    static Function1<Throwable, Void> checkException(
        final String expected)
    {
        return new Function1<Throwable, Void>() {
            public Void apply(Throwable p0) {
                StringWriter stringWriter = new StringWriter();
                PrintWriter printWriter = new PrintWriter(stringWriter);
                p0.printStackTrace(printWriter);
                printWriter.flush();
                String stack = stringWriter.toString();
                Assert.assertTrue(stack, stack.contains(expected));
                return null;
            }
        };
    }

    static Function1<String, Void> checkResult(final String expected) {
        return new Function1<String, Void>() {
            public Void apply(String p0) {
                Assert.assertEquals(expected, p0);
                return null;
            }
        };
    }

    public static Function1<String, Void> checkResultContains(
        final String expected)
    {
        return new Function1<String, Void>() {
            public Void apply(String p0) {
                Assert.assertTrue(p0, p0.contains(expected));
                return null;
            }
        };
    }

    static void assertQuery(
        Connection connection,
        String sql,
        Function1<String, Void> resultChecker,
        Function1<Throwable, Void> exceptionChecker)
        throws Exception
    {
        Statement statement = connection.createStatement();
        ResultSet resultSet;
        try {
            resultSet = statement.executeQuery(sql);
            if (exceptionChecker != null) {
                exceptionChecker.apply(null);
                return;
            }
        } catch (Exception e) {
            if (exceptionChecker != null) {
                exceptionChecker.apply(e);
                return;
            }
            throw e;
        } catch (Error e) {
            if (exceptionChecker != null) {
                exceptionChecker.apply(e);
                return;
            }
            throw e;
        }
        StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1;; i++) {
                buf.append(resultSet.getMetaData().getColumnLabel(i))
                    .append("=")
                    .append(resultSet.getObject(i));
                if (i == n) {
                    break;
                }
                buf.append("; ");
            }
            buf.append("\n");
        }
        resultSet.close();
        statement.close();
        connection.close();

        if (resultChecker != null) {
            resultChecker.apply(buf.toString());
        }
    }

    /**
     * Result of calling {@link OptiqAssert#assertThat}.
     */
    public static class AssertThat {
        private final ConnectionFactory connectionFactory;

        private AssertThat(Config config) {
            this(new ConfigConnectionFactory(config));
        }

        private AssertThat(ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        public AssertThat with(Config config) {
            return new AssertThat(config);
        }

        public AssertThat with(ConnectionFactory connectionFactory) {
            return new AssertThat(connectionFactory);
        }

        public AssertQuery query(String sql) {
            System.out.println(sql);
            return new AssertQuery(connectionFactory, sql);
        }

        public <T> T doWithConnection(Function1<OptiqConnection, T> fn)
            throws Exception
        {
            Connection connection = connectionFactory.createConnection();
            try {
                return fn.apply((OptiqConnection) connection);
            } finally {
                connection.close();
            }
        }

        public AssertThat withSchema(String schema) {
            return new AssertThat(
                new SchemaConnectionFactory(connectionFactory, schema));
        }
    }

    public interface ConnectionFactory {
        OptiqConnection createConnection() throws Exception;
    }

    private static class ConfigConnectionFactory implements ConnectionFactory {
        private final Config config;

        public ConfigConnectionFactory(Config config) {
            this.config = config;
        }

        public OptiqConnection createConnection() throws Exception {
            switch (config) {
            case REGULAR:
                return JdbcTest.getConnection("hr", "foodmart");
            case REGULAR_PLUS_METADATA:
                return JdbcTest.getConnection("hr", "foodmart", "metadata");
            case JDBC_FOODMART2:
                return JdbcTest.getConnection(null, false);
            case JDBC_FOODMART:
                return JdbcTest.getConnection(
                    JdbcQueryProvider.INSTANCE, false);
            case FOODMART_CLONE:
                return JdbcTest.getConnection(JdbcQueryProvider.INSTANCE, true);
            default:
                throw Util.unexpected(config);
            }
        }
    }

    private static class DelegatingConnectionFactory
        implements ConnectionFactory
    {
        private final ConnectionFactory factory;

        public DelegatingConnectionFactory(ConnectionFactory factory) {
            this.factory = factory;
        }

        public OptiqConnection createConnection() throws Exception {
            return factory.createConnection();
        }
    }

    private static class SchemaConnectionFactory
        extends DelegatingConnectionFactory
    {
        private final String schema;

        public SchemaConnectionFactory(ConnectionFactory factory, String schema)
        {
            super(factory);
            this.schema = schema;
        }

        @Override
        public OptiqConnection createConnection() throws Exception {
            OptiqConnection connection = super.createConnection();
            connection.setSchema(schema);
            return connection;
        }
    }

    public static class AssertQuery {
        private final String sql;
        private ConnectionFactory connectionFactory;

        private AssertQuery(ConnectionFactory connectionFactory, String sql) {
            this.sql = sql;
            this.connectionFactory = connectionFactory;
        }

        protected Connection createConnection() throws Exception {
            return connectionFactory.createConnection();
        }

        public void returns(String expected) {
            returns(checkResult(expected));
        }

        public void returns(Function1<String, Void> checker) {
            try {
                assertQuery(
                    createConnection(), sql, checker, null);
            } catch (Exception e) {
                throw new RuntimeException(
                    "exception while executing [" + sql + "]", e);
            }
        }

        public void throws_(String message) {
            try {
                assertQuery(
                    createConnection(), sql, null, checkException(message));
            } catch (Exception e) {
                throw new RuntimeException(
                    "exception while executing [" + sql + "]", e);
            }
        }

        public void runs() {
            try {
                assertQuery(createConnection(), sql, null, null);
            } catch (Exception e) {
                throw new RuntimeException(
                    "exception while executing [" + sql + "]", e);
            }
        }
    }

    public enum Config {
        /**
         * Configuration that creates a connection with two in-memory data sets:
         * {@link net.hydromatic.optiq.test.JdbcTest.HrSchema} and
         * {@link net.hydromatic.optiq.test.JdbcTest.FoodmartSchema}.
         */
        REGULAR,

        /**
         * Configuration that creates a connection to a MySQL server. Tables
         * such as "customer" and "sales_fact_1997" are available. Queries
         * are processed by generating Java that calls linq4j operators
         * such as
         * {@link net.hydromatic.linq4j.Enumerable#where(net.hydromatic.linq4j.function.Predicate1)}.
         */
        JDBC_FOODMART,
        JDBC_FOODMART2,

        /** Configuration that contains an in-memory clone of the FoodMart
         * database. */
        FOODMART_CLONE,

        /** Configuration that includes the metadata schema. */
        REGULAR_PLUS_METADATA,
    }
}

// End OptiqAssert.java
