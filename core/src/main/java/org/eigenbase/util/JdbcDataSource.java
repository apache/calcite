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
package org.eigenbase.util;

import java.io.*;

import java.sql.*;
import java.util.logging.Logger;

import javax.sql.*;

import org.eigenbase.util14.Unwrappable;

/**
 * Adapter to make a JDBC connection into a {@link javax.sql.DataSource}.
 */
public class JdbcDataSource
    extends Unwrappable
    implements DataSource
{
    //~ Instance fields --------------------------------------------------------

    private final String url;
    private PrintWriter logWriter;
    private int loginTimeout;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a JDBC data source.
     *
     * @param url URL of JDBC connection (must not be null)
     *
     * @pre url != null
     */
    public JdbcDataSource(String url)
    {
        assert (url != null);
        this.url = url;
    }

    //~ Methods ----------------------------------------------------------------

    public Connection getConnection()
        throws SQLException
    {
        if (url.startsWith("jdbc:hsqldb:")) {
            // Hsqldb requires a username, but doesn't support username as part
            // of the URL, durn it. Assume that the username is "sa".
            return DriverManager.getConnection(url, "sa", "");
        } else {
            return DriverManager.getConnection(url);
        }
    }

    public String getUrl()
    {
        return url;
    }

    public Connection getConnection(
        String username,
        String password)
        throws SQLException
    {
        return DriverManager.getConnection(url, username, password);
    }

    public void setLogWriter(PrintWriter out)
        throws SQLException
    {
        logWriter = out;
    }

    public PrintWriter getLogWriter()
        throws SQLException
    {
        return logWriter;
    }

    public void setLoginTimeout(int seconds)
        throws SQLException
    {
        loginTimeout = seconds;
    }

    public int getLoginTimeout()
        throws SQLException
    {
        return loginTimeout;
    }

    // for JDK 1.7
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}

// End JdbcDataSource.java
