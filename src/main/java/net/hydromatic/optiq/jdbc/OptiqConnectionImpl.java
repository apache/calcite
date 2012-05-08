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

import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.impl.java.MapSchema;
import net.hydromatic.optiq.server.OptiqServer;
import net.hydromatic.optiq.server.OptiqServerStatement;

import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

/**
 * Implementation of JDBC connection
 * in the OPTIQ engine.
 *
 * <p>Abstract to allow newer versions of JDBC to add methods.</p>
 */
abstract class OptiqConnectionImpl implements OptiqConnection {
    public static final String CONNECT_STRING_PREFIX = "jdbc:optiq:";
    static final Helper HELPER = new Helper();

    public final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl();

    private boolean autoCommit;
    private boolean closed;
    private boolean readOnly;
    private int transactionIsolation;
    private int holdability;
    private int networkTimeout;
    private String catalog;

    final MutableSchema rootSchema = new MapSchema();
    final UnregisteredDriver driver;
    final Factory factory;
    private final String url;
    private final Properties info;
    private String schema;
    private final OptiqDatabaseMetaData metaData;

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

    OptiqConnectionImpl(
        UnregisteredDriver driver, Factory factory, String url, Properties info)
    {
        this.driver = driver;
        this.factory = factory;
        this.url = url;
        this.info = info;
        this.metaData = factory.newDatabaseMetaData(this);
        this.holdability = metaData.getResultSetHoldability();
    }

    public MutableSchema getRootSchema() {
        return rootSchema;
    }

    public RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public Statement createStatement() throws SQLException {
        //noinspection MagicConstant
        return createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
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
                this,
                resultSetType,
                resultSetConcurrency,
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
        throw HELPER.createException(
            "does not implement '" + iface + "'");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    static boolean acceptsURL(String url) {
        return url.startsWith(CONNECT_STRING_PREFIX);
    }

    static class Helper {
        public RuntimeException todo() {
            return new RuntimeException("todo: implement this method");
        }

        public SQLException createException(String message, Exception e) {
            return new SQLException(message, e);
        }

        public SQLException createException(String message) {
            return new SQLException(message);
        }

        public SQLException toSQLException(SQLException exception) {
            return exception;
        }
    }
}

// End OptiqConnectionImpl.java
