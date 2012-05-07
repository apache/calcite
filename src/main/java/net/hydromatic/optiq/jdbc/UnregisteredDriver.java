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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Implementation of OPTIQ JDBC driver that does not register itself.
 */
class UnregisteredDriver implements java.sql.Driver {
    final DriverVersion version = new DriverVersion();
    final Factory factory;

    UnregisteredDriver() {
        this.factory = createFactory();
    }

    private static Factory createFactory() {
        final String factoryClassName = getFactoryClassName();
        try {
            final Class<?> clazz = Class.forName(factoryClassName);
            return (Factory) clazz.newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getFactoryClassName() {
        try {
            // If java.sql.PseudoColumnUsage is present, we are running JDBC 4.1
            // or later.
            Class.forName("java.sql.PseudoColumnUsage");
            return "net.hydromatic.optiq.jdbc.FactoryJdbc41";
        } catch (ClassNotFoundException e) {
            // java.sql.PseudoColumnUsage is not present. This means we are
            // running JDBC 4.0 or earlier.
            try {
                Class.forName("java.sql.Wrapper");
                return "net.hydromatic.optiq.jdbc.FactoryJdbc4Impl";
            } catch (ClassNotFoundException e2) {
                // java.sql.Wrapper is not present. This means we are running
                // JDBC 3.0 or earlier (probably JDK 1.5). Load the JDBC 3.0
                // factory.
                return "net.hydromatic.optiq.jdbc.FactoryJdbc3Impl";
            }
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        return factory.newConnection(this, factory, url, info);
    }

    public boolean acceptsURL(String url) throws SQLException {
        return OptiqConnectionImpl.acceptsURL(url);
    }

    public DriverPropertyInfo[] getPropertyInfo(
        String url, Properties info) throws SQLException
    {
        List<DriverPropertyInfo> list = new ArrayList<DriverPropertyInfo>();

        // First, add the contents of info
        for (Map.Entry<Object, Object> entry : info.entrySet()) {
            list.add(
                new DriverPropertyInfo(
                    (String) entry.getKey(),
                    (String) entry.getValue()));
        }
        // Next, add property definitions not mentioned in info
        for (ConnectionProperty p : ConnectionProperty.values()) {
            if (info.containsKey(p.name())) {
                continue;
            }
            list.add(
                new DriverPropertyInfo(
                    p.name(),
                    null));
        }
        return list.toArray(new DriverPropertyInfo[list.size()]);
    }

    // JDBC 4.1 support (JDK 1.7 and higher)
    public Logger getParentLogger() {
        return Logger.getLogger("");
    }

    /**
     * Returns the driver name. Not in the JDBC API.
     *
     * @return Driver name
     */
    String getName() {
        return version.name;
    }

    /**
     * Returns the driver version. Not in the JDBC API.
     *
     * @return Driver version
     */
    String getVersion() {
        return version.versionString;
    }

    public int getMajorVersion() {
        return version.majorVersion;
    }

    public int getMinorVersion() {
        return version.minorVersion;
    }

    public boolean jdbcCompliant() {
        return true;
    }

    /**
     * Registers this driver with the driver manager.
     */
    void register()
    {
        try {
            DriverManager.registerDriver(this);
        } catch (SQLException e) {
            System.out.println(
                "Error occurred while registering JDBC driver "
                + this + ": " + e.toString());
        }
    }
}

// End UnregisteredDriver.java
