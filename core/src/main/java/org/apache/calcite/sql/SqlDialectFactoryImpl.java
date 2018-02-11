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
package org.apache.calcite.sql;

import org.apache.calcite.config.NullCollation;

import org.apache.calcite.sql.dialect.AccessSqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.Db2SqlDialect;
import org.apache.calcite.sql.dialect.DerbySqlDialect;
import org.apache.calcite.sql.dialect.FirebirdSqlDialect;
import org.apache.calcite.sql.dialect.H2SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.apache.calcite.sql.dialect.InfobrightSqlDialect;
import org.apache.calcite.sql.dialect.InformixSqlDialect;
import org.apache.calcite.sql.dialect.IngresSqlDialect;
import org.apache.calcite.sql.dialect.InterbaseSqlDialect;
import org.apache.calcite.sql.dialect.JethrodataSqlDialect;
import org.apache.calcite.sql.dialect.LucidDbSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.NeoviewSqlDialect;
import org.apache.calcite.sql.dialect.NetezzaSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.ParaccelSqlDialect;
import org.apache.calcite.sql.dialect.PhoenixSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;
import org.apache.calcite.sql.dialect.SybaseSqlDialect;
import org.apache.calcite.sql.dialect.TeradataSqlDialect;
import org.apache.calcite.sql.dialect.VerticaSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;

/**
 * The default implementation of a <code>SqlDialectFactory</code>.
 */
public class SqlDialectFactoryImpl implements SqlDialectFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlDialectFactoryImpl.class);

  public static final SqlDialectFactoryImpl INSTANCE = new SqlDialectFactoryImpl();

  private final Map<String, ImmutableMap<String, HashSet<JethroSupportedFunction>>>
      supportedJethroFunctions = new HashMap<>();

  /**
   * A class to hold one jethro supported function info
   */
  public static class JethroSupportedFunction {
    private final String funcName;
    private final SqlTypeName [] operandsType;

    /**
     * @param funcName The sql function name
     * @param operands The sql function parameters type
     */
    JethroSupportedFunction(String funcName, String operands) {
      super();

      this.funcName = funcName;
      String[] operandsStrType = operands.split(":");
      this.operandsType = new SqlTypeName [operandsStrType.length];
      for (int i = 0; i < this.operandsType.length; ++i) {
        SqlTypeName curr_t = SqlTypeName.ANY;
        switch (operandsStrType [i].toLowerCase(Locale.getDefault())) {
        case "bigint":
        case "long":
          curr_t = SqlTypeName.BIGINT;
          break;
        case "integer":
        case "int":
          curr_t = SqlTypeName.INTEGER;
          break;
        case "double":
          curr_t = SqlTypeName.DOUBLE;
          break;
        case "float":
          curr_t = SqlTypeName.FLOAT;
          break;
        case "string":
          curr_t = SqlTypeName.CHAR;
          break;
        case "simeStamp":
          curr_t = SqlTypeName.TIMESTAMP;
          break;

        default:
          break;
        }
        operandsType [i] = curr_t;
      }
    }

    public String getFuncName() {
      return funcName;
    }

    public SqlTypeName[] getOperandsType() {
      return operandsType;
    }
  };

  /**
   * @param jethroConnection The JethroData jdbc data source
   * @return
   * @throws SQLException
   */
  private synchronized
                    ImmutableMap<String, HashSet<JethroSupportedFunction>> getSupportedFunctions(
                                         Connection jethroConnection) throws SQLException {
    if (jethroConnection == null) {
      throw new SQLException("JethrodataSqlDialect reuqies a connection");
    }

    DatabaseMetaData metaData = jethroConnection.getMetaData();
    assert "JethroData".equals(metaData.getDatabaseProductName());
    String productVersion = metaData.getDatabaseProductVersion();
    ImmutableMap<String, HashSet<JethroSupportedFunction>> res =
            supportedJethroFunctions.get(productVersion);

    if (res == null) {
      Statement jethroStatement = null;
      ResultSet functionsTupleSet = null;
      try {
        jethroStatement = jethroConnection.createStatement();
        functionsTupleSet = jethroStatement.executeQuery("show functions extended");

        Map<String, HashSet<JethroSupportedFunction>> supportedFunctions = new HashMap<>();
        while (functionsTupleSet.next()) {
          String functionName = functionsTupleSet.getString(1);
          String operandsType = functionsTupleSet.getString(3);
          HashSet<JethroSupportedFunction> funcSignatures = supportedFunctions.get(functionName);
          if (funcSignatures == null) {
            funcSignatures = new HashSet<>();
            supportedFunctions.put(functionName, funcSignatures);
          }
          funcSignatures.add(new JethroSupportedFunction(functionName, operandsType));
        }
        res = ImmutableMap.copyOf(supportedFunctions);
        supportedJethroFunctions.put(productVersion, res);
      } catch (Exception e) {
        LOGGER.error("Jethro server failed to execute 'show functions extended'", e);
        throw new SQLException("Jethro server failed to execute 'show functions extended',"
                               + " make sure your Jethro server is up to date");
      } finally {
        try {
          if (jethroStatement != null) {
            jethroStatement.close();
          }

          if (functionsTupleSet != null) {
            functionsTupleSet.close();
          }
        } catch (Exception e) {
          LOGGER.error("Failed to close jethro connection resources", e);
        }
      }
    }
    return res;
  }

  public SqlDialect create(DatabaseMetaData databaseMetaData) throws SQLException {
    String databaseProductName;
    int databaseMajorVersion;
    int databaseMinorVersion;
    String databaseVersion;
    try {
      databaseProductName = databaseMetaData.getDatabaseProductName();
      databaseMajorVersion = databaseMetaData.getDatabaseMajorVersion();
      databaseMinorVersion = databaseMetaData.getDatabaseMinorVersion();
      databaseVersion = databaseMetaData.getDatabaseProductVersion();
    } catch (SQLException e) {
      throw new RuntimeException("while detecting database product", e);
    }
    final String upperProductName =
        databaseProductName.toUpperCase(Locale.ROOT).trim();
    final String quoteString = getIdentifierQuoteString(databaseMetaData);
    final NullCollation nullCollation = getNullCollation(databaseMetaData);
    final SqlDialect.Context c = SqlDialect.EMPTY_CONTEXT
        .withDatabaseProductName(databaseProductName)
        .withDatabaseMajorVersion(databaseMajorVersion)
        .withDatabaseMinorVersion(databaseMinorVersion)
        .withDatabaseVersion(databaseVersion)
        .withIdentifierQuoteString(quoteString)
        .withNullCollation(nullCollation);
    switch (upperProductName) {
    case "ACCESS":
      return new AccessSqlDialect(c);
    case "APACHE DERBY":
      return new DerbySqlDialect(c);
    case "DBMS:CLOUDSCAPE":
      return new DerbySqlDialect(c);
    case "HIVE":
      return new HiveSqlDialect(c);
    case "INGRES":
      return new IngresSqlDialect(c);
    case "INTERBASE":
      return new InterbaseSqlDialect(c);
    case "JETHRODATA":
    {
      Connection connection = null;
      JethrodataSqlDialect jethroDialect = null;
      try {
        connection = databaseMetaData.getConnection();
        jethroDialect = new JethrodataSqlDialect(c, getSupportedFunctions(connection));
      } catch (SQLException e) {
        LOGGER.error("Failed to create JethroDataDialect", e);
        throw e;
      } finally {
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException e) {
            LOGGER.error("Failed to close jethro connection", e);
          }
        }
      }
      return jethroDialect;
    }
    case "LUCIDDB":
      return new LucidDbSqlDialect(c);
    case "ORACLE":
      return new OracleSqlDialect(c);
    case "PHOENIX":
      return new PhoenixSqlDialect(c);
    case "MYSQL (INFOBRIGHT)":
      return new InfobrightSqlDialect(c);
    case "MYSQL":
      return new MysqlSqlDialect(c);
    case "REDSHIFT":
      return new RedshiftSqlDialect(c);
    }
    // Now the fuzzy matches.
    if (databaseProductName.startsWith("DB2")) {
      return new Db2SqlDialect(c);
    } else if (upperProductName.contains("FIREBIRD")) {
      return new FirebirdSqlDialect(c);
    } else if (databaseProductName.startsWith("Informix")) {
      return new InformixSqlDialect(c);
    } else if (upperProductName.contains("NETEZZA")) {
      return new NetezzaSqlDialect(c);
    } else if (upperProductName.contains("PARACCEL")) {
      return new ParaccelSqlDialect(c);
    } else if (databaseProductName.startsWith("HP Neoview")) {
      return new NeoviewSqlDialect(c);
    } else if (upperProductName.contains("POSTGRE")) {
      return new PostgresqlSqlDialect(c);
    } else if (upperProductName.contains("SQL SERVER")) {
      return new MssqlSqlDialect(c);
    } else if (upperProductName.contains("SYBASE")) {
      return new SybaseSqlDialect(c);
    } else if (upperProductName.contains("TERADATA")) {
      return new TeradataSqlDialect(c);
    } else if (upperProductName.contains("HSQL")) {
      return new HsqldbSqlDialect(c);
    } else if (upperProductName.contains("H2")) {
      return new H2SqlDialect(c);
    } else if (upperProductName.contains("VERTICA")) {
      return new VerticaSqlDialect(c);
    } else {
      return new AnsiSqlDialect(c);
    }
  }

  private NullCollation getNullCollation(DatabaseMetaData databaseMetaData) {
    try {
      if (databaseMetaData.nullsAreSortedAtEnd()) {
        return NullCollation.LAST;
      } else if (databaseMetaData.nullsAreSortedAtStart()) {
        return NullCollation.FIRST;
      } else if (databaseMetaData.nullsAreSortedLow()) {
        return NullCollation.LOW;
      } else if (databaseMetaData.nullsAreSortedHigh()) {
        return NullCollation.HIGH;
      } else {
        throw new IllegalArgumentException("cannot deduce null collation");
      }
    } catch (SQLException e) {
      throw new IllegalArgumentException("cannot deduce null collation", e);
    }
  }

  private String getIdentifierQuoteString(DatabaseMetaData databaseMetaData) {
    try {
      return databaseMetaData.getIdentifierQuoteString();
    } catch (SQLException e) {
      throw new IllegalArgumentException("cannot deduce identifier quote string", e);
    }
  }

  /** Returns a basic dialect for a given product, or null if none is known. */
  static SqlDialect simple(SqlDialect.DatabaseProduct databaseProduct) {
    switch (databaseProduct) {
    case ACCESS:
      return AccessSqlDialect.DEFAULT;
    case CALCITE:
      return CalciteSqlDialect.DEFAULT;
    case DB2:
      return Db2SqlDialect.DEFAULT;
    case DERBY:
      return DerbySqlDialect.DEFAULT;
    case FIREBIRD:
      return FirebirdSqlDialect.DEFAULT;
    case H2:
      return H2SqlDialect.DEFAULT;
    case HIVE:
      return HiveSqlDialect.DEFAULT;
    case HSQLDB:
      return HsqldbSqlDialect.DEFAULT;
    case INFOBRIGHT:
      return InfobrightSqlDialect.DEFAULT;
    case INFORMIX:
      return InformixSqlDialect.DEFAULT;
    case INGRES:
      return IngresSqlDialect.DEFAULT;
    case INTERBASE:
      return InterbaseSqlDialect.DEFAULT;
    case JETHRO:
      throw new RuntimeException("Jethro does not support simple creation");
    case LUCIDDB:
      return LucidDbSqlDialect.DEFAULT;
    case MSSQL:
      return MssqlSqlDialect.DEFAULT;
    case MYSQL:
      return MysqlSqlDialect.DEFAULT;
    case NEOVIEW:
      return NeoviewSqlDialect.DEFAULT;
    case NETEZZA:
      return NetezzaSqlDialect.DEFAULT;
    case ORACLE:
      return OracleSqlDialect.DEFAULT;
    case PARACCEL:
      return ParaccelSqlDialect.DEFAULT;
    case PHOENIX:
      return PhoenixSqlDialect.DEFAULT;
    case POSTGRESQL:
      return PostgresqlSqlDialect.DEFAULT;
    case REDSHIFT:
      return RedshiftSqlDialect.DEFAULT;
    case SYBASE:
      return SybaseSqlDialect.DEFAULT;
    case TERADATA:
      return TeradataSqlDialect.DEFAULT;
    case VERTICA:
      return VerticaSqlDialect.DEFAULT;
    case SQLSTREAM:
    case UNKNOWN:
    default:
      return null;
    }
  }
}

// End SqlDialectFactoryImpl.java
