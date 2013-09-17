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

import net.hydromatic.linq4j.function.Function0;

import org.eigenbase.util14.ConnectStringParser;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Implementation of Optiq JDBC driver that does not register itself.
 *
 * <p>You can easily create a "vanity driver" that recognizes its own
 * URL prefix as a sub-class of this class. Per the JDBC specification it
 * must register itself when the class is loaded.</p>
 *
 * <p>Derived classes must implement {@link #createDriverVersion()} and
 * {@link #getConnectStringPrefix()}, and may override
 * {@link #createFactory()}.</p>
 */
public abstract class UnregisteredDriver implements java.sql.Driver {
  final DriverVersion version;
  final Factory factory;
  final Function0<OptiqPrepare> prepareFactory;
  final Handler handler;

  protected UnregisteredDriver() {
    this.factory = createFactory();
    this.prepareFactory = createPrepareFactory();
    this.version = createDriverVersion();
    this.handler = createHandler();
  }

  protected Function0<OptiqPrepare> createPrepareFactory() {
    return OptiqPrepare.DEFAULT_FACTORY;
  }

  /**
   * Creates a factory for JDBC objects (connection, statement).
   * Called from the driver constructor.
   *
   * <p>The default implementation calls {@link JdbcVersion#current},
   * then {@link #getFactoryClassName} with that version,
   * then passes that class name to {@link #instantiateFactory(String)}.
   * This approach is recommended it does not include in the code references
   * to classes that may not be instantiable in all JDK versions.
   * But drivers are free to do it their own way.</p>
   *
   * @return JDBC object factory
   */
  protected Factory createFactory() {
    return instantiateFactory(getFactoryClassName(JdbcVersion.current()));
  }

  /** Creates a Handler. */
  protected Handler createHandler() {
    return new HandlerImpl();
  }

  /**
   * Returns the name of a class to be factory for JDBC objects
   * (connection, statement) appropriate for the current JDBC version.
   */
  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
    case JDBC_30:
      return "net.hydromatic.optiq.jdbc.FactoryJdbc3Impl";
    case JDBC_40:
      return "net.hydromatic.optiq.jdbc.FactoryJdbc4Impl";
    case JDBC_41:
    default:
      return "net.hydromatic.optiq.jdbc.FactoryJdbc41";
    }
  }

  /**
   * Creates an object describing the name and version of this driver.
   * Called from the driver constructor.
   */
  protected abstract DriverVersion createDriverVersion();

  /** Helper method for creating factories. */
  protected static Factory instantiateFactory(String factoryClassName) {
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

  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    final String prefix = getConnectStringPrefix();
    assert url.startsWith(prefix);
    final String urlSuffix = url.substring(prefix.length());
    final Properties info2 = ConnectStringParser.parse(urlSuffix, info);
    final OptiqConnectionImpl connection =
        factory.newConnection(this, factory, prepareFactory, url, info2);
    handler.onConnectionInit(connection);
    return connection;
  }

  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(getConnectStringPrefix());
  }

  /** Returns the prefix of the connect string that this driver will recognize
   * as its own. For example, "jdbc:optiq:". */
  protected abstract String getConnectStringPrefix();

  public DriverPropertyInfo[] getPropertyInfo(
      String url, Properties info) throws SQLException {
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
   * Returns the driver version object. Not in the JDBC API.
   *
   * @return Driver version
   */
  public DriverVersion getDriverVersion() {
    return version;
  }

  public final int getMajorVersion() {
    return version.majorVersion;
  }

  public final int getMinorVersion() {
    return version.minorVersion;
  }

  public boolean jdbcCompliant() {
    return version.jdbcCompliant;
  }

  /**
   * Registers this driver with the driver manager.
   */
  protected void register() {
    try {
      DriverManager.registerDriver(this);
    } catch (SQLException e) {
      System.out.println(
          "Error occurred while registering JDBC driver "
              + this + ": " + e.toString());
    }
  }

  /** JDBC version. */
  protected enum JdbcVersion {
    /** Unknown JDBC version. */
    JDBC_UNKNOWN,
    /** JDBC version 3.0. Generally associated with JDK 1.5. */
    JDBC_30,
    /** JDBC version 4.0. Generally associated with JDK 1.6. */
    JDBC_40,
    /** JDBC version 4.1. Generally associated with JDK 1.7. */
    JDBC_41;

    /** Deduces the current JDBC version. */
    public static JdbcVersion current() {
      try {
        // If java.sql.PseudoColumnUsage is present, we are running JDBC
        // 4.1 or later.
        Class.forName("java.sql.PseudoColumnUsage");
        return JDBC_41;
      } catch (ClassNotFoundException e) {
        // java.sql.PseudoColumnUsage is not present. This means we are
        // running JDBC 4.0 or earlier.
        try {
          Class.forName("java.sql.Wrapper");
          return JDBC_40;
        } catch (ClassNotFoundException e2) {
          // java.sql.Wrapper is not present. This means we are
          // running JDBC 3.0 or earlier (probably JDK 1.5).
          return JDBC_30;
        }
      }
    }
  }
}

// End UnregisteredDriver.java
