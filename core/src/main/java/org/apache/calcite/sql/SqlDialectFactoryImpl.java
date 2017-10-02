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
import org.apache.calcite.sql.dialect.Db2SqlDialect;
import org.apache.calcite.sql.dialect.DerbySqlDialect;
import org.apache.calcite.sql.dialect.FirebirdSqlDialect;
import org.apache.calcite.sql.dialect.H2SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.apache.calcite.sql.dialect.InfobrightSqlDialect;
import org.apache.calcite.sql.dialect.InformixSqlDialect;
import org.apache.calcite.sql.dialect.IngressSqlDialect;
import org.apache.calcite.sql.dialect.InterbaseSqlDialect;
import org.apache.calcite.sql.dialect.LucidbSqlDialect;
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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;

/**
 * The default implementation of a <code>SqlDialectFactory</code>.
 */
public class SqlDialectFactoryImpl implements SqlDialectFactory {

  /**
   * {@inheritDoc}
   */
  public SqlDialect create(DatabaseMetaData databaseMetaData) {
    String databaseProductName;
    String databaseVersion;
    try {
      databaseProductName = databaseMetaData.getDatabaseProductName();
      databaseVersion = databaseMetaData.getDatabaseProductVersion();
    } catch (SQLException e) {
      throw new RuntimeException("while detecting database product", e);
    }
    final String upperProductName =
        databaseProductName.toUpperCase(Locale.ROOT).trim();
    String quoteString = getIdentifierQuoteString(databaseMetaData);
    NullCollation nullCollation = getNullCollation(databaseMetaData);
    switch (upperProductName) {
    case "ACCESS":
      return new AccessSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "APACHE DERBY":
      return new DerbySqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "DBMS:CLOUDSCAPE":
      return new DerbySqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "HIVE":
      return new HiveSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "INGRES":
      return new IngressSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "INTERBASE":
      return new InterbaseSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "LUCIDDB":
      return new LucidbSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "ORACLE":
      return new OracleSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "PHOENIX":
      return new PhoenixSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "MYSQL (INFOBRIGHT)":
      return new InfobrightSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "MYSQL":
      return new MysqlSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    case "REDSHIFT":
      return new RedshiftSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    }
    // Now the fuzzy matches.
    if (databaseProductName.startsWith("DB2")) {
      return new Db2SqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (upperProductName.contains("FIREBIRD")) {
      return new FirebirdSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (databaseProductName.startsWith("Informix")) {
      return new InformixSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (upperProductName.contains("NETEZZA")) {
      return new NetezzaSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (upperProductName.contains("PARACCEL")) {
      return new ParaccelSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (databaseProductName.startsWith("HP Neoview")) {
      return new NeoviewSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (upperProductName.contains("POSTGRE")) {
      return new PostgresqlSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (upperProductName.contains("SQL SERVER")) {
      return new MssqlSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (upperProductName.contains("SYBASE")) {
      return new SybaseSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (upperProductName.contains("TERADATA")) {
      return new TeradataSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (upperProductName.contains("HSQL")) {
      return new HsqldbSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (upperProductName.contains("H2")) {
      return new H2SqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else if (upperProductName.contains("VERTICA")) {
      return new VerticaSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    } else {
      return new AnsiSqlDialect(databaseProductName, databaseVersion,
          quoteString, nullCollation);
    }

  }

  protected static NullCollation getNullCollation(DatabaseMetaData databaseMetaData) {
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

  protected static String getIdentifierQuoteString(DatabaseMetaData databaseMetaData) {
    try {
      return databaseMetaData.getIdentifierQuoteString();
    } catch (SQLException e) {
      throw new IllegalArgumentException("cannot deduce identifier quote string", e);
    }
  }

}

// End SqlDialectFactoryImpl.java
