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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Extensions;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.runtime.ByteString;
import net.hydromatic.optiq.server.OptiqServer;
import net.hydromatic.optiq.server.OptiqServerStatement;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.BasicSqlType;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * Implementation of JDBC connection
 * in the Optiq engine.
 *
 * <p>Abstract to allow newer versions of JDBC to add methods.</p>
 */
abstract class OptiqConnectionImpl implements OptiqConnection {
    public static final String CONNECT_STRING_PREFIX = "jdbc:optiq:";

    public final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();

    private boolean autoCommit;
    private boolean closed;
    private boolean readOnly;
    private int transactionIsolation;
    private int holdability;
    private int networkTimeout;
    private String catalog;

    final MutableSchema rootSchema = new MapSchema(typeFactory);
    final UnregisteredDriver driver;
    final net.hydromatic.optiq.jdbc.Factory factory;
    private final String url;
    private final Properties info;
    private String schema;
    private final OptiqDatabaseMetaData metaData;
    final Helper helper = Helper.INSTANCE;

    final OptiqServer server = new OptiqServer() {
        final List<OptiqServerStatement> statementList =
            new ArrayList<OptiqServerStatement>();

        public void removeStatement(OptiqServerStatement optiqServerStatement) {
            statementList.add(optiqServerStatement);
        }

        public void addStatement(OptiqServerStatement statement) {
            statementList.add(statement);
        }
    };

    /**
     * Creates an OptiqConnectionImpl.
     *
     * <p>Not public; method is called only from the driver.</p>
     *
     * @param driver Driver
     * @param factory Factory for JDBC objects
     * @param url Server URL
     * @param info Other connection properties
     */
    OptiqConnectionImpl(
        UnregisteredDriver driver,
        net.hydromatic.optiq.jdbc.Factory factory,
        String url,
        Properties info)
    {
        this.driver = driver;
        this.factory = factory;
        this.url = url;
        this.info = info;
        this.metaData = factory.newDatabaseMetaData(this);
        this.holdability = metaData.getResultSetHoldability();
    }

    // OptiqConnection methods

    public MutableSchema getRootSchema() {
        return rootSchema;
    }


    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }

    // QueryProvider methods

    public <T> Queryable<T> createQuery(
        Expression expression, Class<T> rowType)
    {
        return new ObjectAbstractQueryable2<T>(this, rowType, expression);
    }

    public <T> Queryable<T> createQuery(Expression expression, Type rowType) {
        return new ObjectAbstractQueryable2<T>(this, rowType, expression);
    }

    public <T> T execute(Expression expression, Type type) {
        return null; // TODO:
    }

    public <T> T execute(Expression expression, Class<T> type) {
        return null; // TODO:
    }

    // Connection methods

    public Statement createStatement() throws SQLException {
        //noinspection MagicConstant
        return createStatement(
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY,
            holdability);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        //noinspection MagicConstant
        return prepareStatement(
            sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
            holdability);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }

    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    public void commit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void rollback() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void close() throws SQLException {
        closed = true;
    }

    public boolean isClosed() throws SQLException {
        return closed;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return metaData;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    public void setCatalog(String catalog) throws SQLException {
        this.catalog = catalog;
    }

    public String getCatalog() throws SQLException {
        return catalog;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        this.transactionIsolation = level;
    }

    public int getTransactionIsolation() throws SQLException {
        return transactionIsolation;
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Statement createStatement(
        int resultSetType, int resultSetConcurrency) throws SQLException
    {
        //noinspection MagicConstant
        return createStatement(
            resultSetType, resultSetConcurrency, holdability);
    }

    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency) throws SQLException
    {
        //noinspection MagicConstant
        return prepareStatement(
            sql, resultSetType, resultSetConcurrency, holdability);
    }

    public CallableStatement prepareCall(
        String sql,
        int resultSetType,
        int resultSetConcurrency) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setHoldability(int holdability) throws SQLException {
        if (!(holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT
              || holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT))
        {
            throw new SQLException("invalid value");
        }
        this.holdability = holdability;
    }

    public int getHoldability() throws SQLException {
        return holdability;
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) throws SQLException
    {
        OptiqStatement statement =
            factory.newStatement(
                this, resultSetType, resultSetConcurrency,
                resultSetHoldability);
        server.addStatement(statement);
        return statement;
    }

    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) throws SQLException
    {
        OptiqPreparedStatement statement =
            factory.newPreparedStatement(
                this,
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability);
        server.addStatement(statement);
        return statement;
    }

    public CallableStatement prepareCall(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public PreparedStatement prepareStatement(
        String sql, int autoGeneratedKeys) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public PreparedStatement prepareStatement(
        String sql, int[] columnIndexes) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public PreparedStatement prepareStatement(
        String sql, String[] columnNames) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setClientInfo(
        String name, String value) throws SQLClientInfoException
    {
        throw new UnsupportedOperationException();
    }

    public void setClientInfo(Properties properties)
        throws SQLClientInfoException
    {
        throw new UnsupportedOperationException();
    }

    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array createArrayOf(
        String typeName, Object[] elements) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Struct createStruct(
        String typeName, Object[] attributes) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setSchema(String schema) throws SQLException {
        this.schema = schema;
    }

    public String getSchema() throws SQLException {
        return schema;
    }

    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNetworkTimeout(
        Executor executor, int milliseconds) throws SQLException
    {
        this.networkTimeout = milliseconds;
    }

    public int getNetworkTimeout() throws SQLException {
        return networkTimeout;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw helper.createException(
            "does not implement '" + iface + "'");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    static boolean acceptsURL(String url) {
        return url.startsWith(CONNECT_STRING_PREFIX);
    }

    private static class JavaTypeFactoryImpl
        extends SqlTypeFactoryImpl
        implements JavaTypeFactory
    {
        public RelDataType createStructType(Class type) {
            List<RelDataTypeField> list = new ArrayList<RelDataTypeField>();
            for (Field field : type.getFields()) {
                // FIXME: watch out for recursion
                list.add(
                    new RelDataTypeFieldImpl(
                        field.getName(),
                        list.size(),
                        createType(field.getType())));
            }
            return canonize(
                new JavaRecordType(
                    list.toArray(new RelDataTypeField[list.size()]),
                    type));
        }

        public RelDataType createType(Class type) {
            if (type.isPrimitive()) {
                return createJavaType(type);
            } else if (type == String.class) {
                // TODO: similar special treatment for BigDecimal, BigInteger,
                //  Date, Time, Timestamp, Double etc.
                return createJavaType(type);
            } else if (type.isArray()) {
                return createMultisetType(
                    createType(type.getComponentType()), -1);
            } else {
                return createStructType(type);
            }
        }

        public Class getJavaClass(RelDataType type) {
            if (type instanceof JavaRecordType) {
                JavaRecordType javaRecordType = (JavaRecordType) type;
                return javaRecordType.clazz;
            }
            if (type instanceof JavaType) {
                JavaType javaType = (JavaType) type;
                return javaType.getJavaClass();
            }
            if (type.isStruct() && type.getFieldCount() == 1) {
                return getJavaClass(type.getFieldList().get(0).getType());
            }
            if (type instanceof BasicSqlType) {
                switch (type.getSqlTypeName()) {
                case VARCHAR:
                case CHAR:
                    return String.class;
                case INTEGER:
                    return Integer.class;
                case BIGINT:
                    return Long.class;
                case SMALLINT:
                    return Short.class;
                case TINYINT:
                    return Byte.class;
                case DECIMAL:
                    return BigDecimal.class;
                case BOOLEAN:
                    return Boolean.class;
                case BINARY:
                case VARBINARY:
                    return ByteString.class;
                case DATE:
                    return java.sql.Date.class;
                case TIME:
                    return Time.class;
                case TIMESTAMP:
                    return Timestamp.class;
                }
            }
            return null;
        }
    }

    private static class JavaRecordType extends RelRecordType {
        private final Class clazz;

        public JavaRecordType(RelDataTypeField[] fields, Class clazz) {
            super(fields);
            this.clazz = clazz;
            assert clazz != null;
        }
    }

    static class ObjectAbstractQueryable2<T>
        extends Extensions.AbstractQueryable2<T>
    {
        public ObjectAbstractQueryable2(
            OptiqConnection connection,
            Type elementType,
            Expression expression)
        {
            super(connection, elementType, expression);
        }

        public Enumerator<T> enumerator() {
            try {
                final OptiqConnection connection = (OptiqConnection) provider;
                Statement statement = connection.createStatement();
                OptiqPrepare.PrepareResult enumerable =
                    net.hydromatic.optiq.prepare.Factory.implement().prepare2(
                        new OptiqPrepare.Statement() {
                            public JavaTypeFactory getTypeFactory() {
                                return connection.getTypeFactory();
                            }

                            public Schema getRootSchema() {
                                return connection.getRootSchema();
                            }

                            public MutableSchema getRoot() {
                                return connection.getRootSchema();
                            }
                        },
                        expression,
                        elementType);
                return (Enumerator) enumerable.execute();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

// End OptiqConnectionImpl.java
