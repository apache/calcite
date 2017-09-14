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
    try {
      databaseProductName = databaseMetaData.getDatabaseProductName();
    } catch (SQLException e) {
      throw new RuntimeException("while detecting database product", e);
    }
    final String upperProductName =
        databaseProductName.toUpperCase(Locale.ROOT).trim();
    switch (upperProductName) {
    case "ACCESS":
      return new AccessSqlDialect(databaseMetaData);
    case "APACHE DERBY":
      return new DerbySqlDialect(databaseMetaData);
    case "DBMS:CLOUDSCAPE":
      return new DerbySqlDialect(databaseMetaData);
    case "HIVE":
      return new HiveSqlDialect(databaseMetaData);
    case "INGRES":
      return new IngressSqlDialect(databaseMetaData);
    case "INTERBASE":
      return new InterbaseSqlDialect(databaseMetaData);
    case "LUCIDDB":
      return new LucidbSqlDialect(databaseMetaData);
    case "ORACLE":
      return new OracleSqlDialect(databaseMetaData);
    case "PHOENIX":
      return new PhoenixSqlDialect(databaseMetaData);
    case "MYSQL (INFOBRIGHT)":
      return new InfobrightSqlDialect(databaseMetaData);
    case "MYSQL":
      return new MysqlSqlDialect(databaseMetaData);
    case "REDSHIFT":
      return new RedshiftSqlDialect(databaseMetaData);
    }
    // Now the fuzzy matches.
    if (databaseProductName.startsWith("DB2")) {
      return new Db2SqlDialect(databaseMetaData);
    } else if (upperProductName.contains("FIREBIRD")) {
      return new FirebirdSqlDialect(databaseMetaData);
    } else if (databaseProductName.startsWith("Informix")) {
      return new InformixSqlDialect(databaseMetaData);
    } else if (upperProductName.contains("NETEZZA")) {
      return new NetezzaSqlDialect(databaseMetaData);
    } else if (upperProductName.contains("PARACCEL")) {
      return new ParaccelSqlDialect(databaseMetaData);
    } else if (databaseProductName.startsWith("HP Neoview")) {
      return new NeoviewSqlDialect(databaseMetaData);
    } else if (upperProductName.contains("POSTGRE")) {
      return new PostgresqlSqlDialect(databaseMetaData);
    } else if (upperProductName.contains("SQL SERVER")) {
      return new MssqlSqlDialect(databaseMetaData);
    } else if (upperProductName.contains("SYBASE")) {
      return new SybaseSqlDialect(databaseMetaData);
    } else if (upperProductName.contains("TERADATA")) {
      return new TeradataSqlDialect(databaseMetaData);
    } else if (upperProductName.contains("HSQL")) {
      return new HsqldbSqlDialect(databaseMetaData);
    } else if (upperProductName.contains("H2")) {
      return new H2SqlDialect(databaseMetaData);
    } else if (upperProductName.contains("VERTICA")) {
      return new VerticaSqlDialect(databaseMetaData);
    } else {
      return new AnsiSqlDialect(databaseMetaData);
    }

  }

}

// End SqlDialectFactoryImpl.java
