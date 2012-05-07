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

import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Properties;

/**
 * Implementation of {@link Factory} for JDBC 4.1 (corresponds to JDK 1.7).
 */
class FactoryJdbc41 implements Factory {
    public OptiqConnectionImpl newConnection(
        UnregisteredDriver driver, Factory factory, String url, Properties info)
    {
        return new OptiqConnectionJdbc41(driver, factory, url, info);
    }

    public OptiqStatement newStatement(OptiqConnectionImpl connection) {
        return new OptiqStatementJdbc41(connection);
    }

    public OptiqPreparedStatement newPreparedStatement(
        OptiqConnectionImpl connection, String sql) throws SQLException
    {
        return new OptiqPreparedStatementJdbc41(connection, sql);
    }

    public OptiqResultSet newResultSet(
        OptiqStatement statement, OptiqPrepare.PrepareResult prepareResult)
    {
        return new OptiqResultSet(statement, prepareResult);
    }

    private static class OptiqConnectionJdbc41 extends OptiqConnectionImpl {
        OptiqConnectionJdbc41(
            UnregisteredDriver driver,
            Factory factory,
            String url,
            Properties info)
        {
            super(driver, factory, url, info);
        }
    }

    private static class OptiqStatementJdbc41 extends OptiqStatement {
        public OptiqStatementJdbc41(OptiqConnectionImpl connection) {
            super(connection);
        }

        public void closeOnCompletion() throws SQLException {
            this.closeOnCompletion = true;
        }

        public boolean isCloseOnCompletion() throws SQLException {
            return closeOnCompletion;
        }
    }

    private static class OptiqPreparedStatementJdbc41
        extends OptiqPreparedStatement
    {
        OptiqPreparedStatementJdbc41(
            OptiqConnectionImpl connection, String sql) throws SQLException
        {
            super(connection, sql);
        }

        public void setRowId(
            int parameterIndex,
            RowId x) throws SQLException
        {
            getParameter(parameterIndex).setRowId(x);
        }

        public void setNString(
            int parameterIndex, String value) throws SQLException
        {
            getParameter(parameterIndex).setNString(value);
        }

        public void setNCharacterStream(
            int parameterIndex,
            Reader value,
            long length) throws SQLException
        {
            getParameter(parameterIndex).setNCharacterStream(value, length);
        }

        public void setNClob(
            int parameterIndex,
            NClob value) throws SQLException
        {
            getParameter(parameterIndex).setNClob(value);
        }

        public void setClob(
            int parameterIndex,
            Reader reader,
            long length) throws SQLException
        {
            getParameter(parameterIndex).setClob(reader, length);
        }

        public void setBlob(
            int parameterIndex,
            InputStream inputStream,
            long length) throws SQLException
        {
            getParameter(parameterIndex).setBlob(inputStream, length);
        }

        public void setNClob(
            int parameterIndex,
            Reader reader,
            long length) throws SQLException
        {
            getParameter(parameterIndex).setNClob(reader, length);
        }

        public void setSQLXML(
            int parameterIndex, SQLXML xmlObject) throws SQLException
        {
            getParameter(parameterIndex).setSQLXML(xmlObject);
        }

        public void setAsciiStream(
            int parameterIndex,
            InputStream x,
            long length) throws SQLException
        {
            getParameter(parameterIndex).setAsciiStream(x, length);
        }

        public void setBinaryStream(
            int parameterIndex,
            InputStream x,
            long length) throws SQLException
        {
            getParameter(parameterIndex).setBinaryStream(x, length);
        }

        public void setCharacterStream(
            int parameterIndex,
            Reader reader,
            long length) throws SQLException
        {
            getParameter(parameterIndex).setCharacterStream(reader, length);
        }

        public void setAsciiStream(
            int parameterIndex, InputStream x) throws SQLException
        {
            getParameter(parameterIndex).setAsciiStream(x);
        }

        public void setBinaryStream(
            int parameterIndex, InputStream x) throws SQLException
        {
            getParameter(parameterIndex).setBinaryStream(x);
        }

        public void setCharacterStream(
            int parameterIndex, Reader reader) throws SQLException
        {
            getParameter(parameterIndex).setCharacterStream(reader);
        }

        public void setNCharacterStream(
            int parameterIndex, Reader value) throws SQLException
        {
            getParameter(parameterIndex).setNCharacterStream(value);
        }

        public void setClob(
            int parameterIndex,
            Reader reader) throws SQLException
        {
            getParameter(parameterIndex).setClob(reader);
        }

        public void setBlob(
            int parameterIndex, InputStream inputStream) throws SQLException
        {
            getParameter(parameterIndex).setBlob(inputStream);
        }

        public void setNClob(
            int parameterIndex, Reader reader) throws SQLException
        {
            getParameter(parameterIndex).setNClob(reader);
        }

        public void closeOnCompletion() throws SQLException {
            closeOnCompletion = true;
        }

        public boolean isCloseOnCompletion() throws SQLException {
            return closeOnCompletion;
        }
    }
}

// End FactoryJdbc41.java
