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
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.sql.dialect.Db2SqlDialect;
import org.apache.calcite.sql.dialect.DerbySqlDialect;
import org.apache.calcite.sql.dialect.ExasolSqlDialect;
import org.apache.calcite.sql.dialect.FirebirdSqlDialect;
import org.apache.calcite.sql.dialect.FireboltSqlDialect;
import org.apache.calcite.sql.dialect.H2SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.HsqldbSqlDialect;
import org.apache.calcite.sql.dialect.InfobrightSqlDialect;
import org.apache.calcite.sql.dialect.InformixSqlDialect;
import org.apache.calcite.sql.dialect.IngresSqlDialect;
import org.apache.calcite.sql.dialect.InterbaseSqlDialect;
import org.apache.calcite.sql.dialect.JethroDataSqlDialect;
import org.apache.calcite.sql.dialect.LucidDbSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.NeoviewSqlDialect;
import org.apache.calcite.sql.dialect.NetezzaSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.ParaccelSqlDialect;
import org.apache.calcite.sql.dialect.PhoenixSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.dialect.PrestoSqlDialect;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.dialect.SybaseSqlDialect;
import org.apache.calcite.sql.dialect.TeradataSqlDialect;
import org.apache.calcite.sql.dialect.VerticaSqlDialect;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;

/**
 * The default implementation of a <code>SqlDialectFactory</code>.
 */
public class SqlDialectFactoryImpl implements SqlDialectFactory {
  public static final SqlDialectFactoryImpl INSTANCE = new SqlDialectFactoryImpl();

  private final JethroDataSqlDialect.JethroInfoCache jethroCache =
      JethroDataSqlDialect.createCache();

  @Override public SqlDialect create(DatabaseMetaData databaseMetaData) {
    SqlDialect.Context c = SqlDialects.createContext(databaseMetaData);

    String databaseProductName = c.databaseProductName();
    try {
      if (databaseProductName == null) {
        databaseProductName = databaseMetaData.getDatabaseProductName();
      }
    } catch (SQLException e) {
      throw new RuntimeException("while detecting database product", e);
    }
    final String upperProductName =
        databaseProductName.toUpperCase(Locale.ROOT).trim();
    switch (upperProductName) {
    case "ACCESS":
      return new AccessSqlDialect(c);
    case "APACHE DERBY":
      return new DerbySqlDialect(c);
    case "CLICKHOUSE":
      return new ClickHouseSqlDialect(c);
    case "DBMS:CLOUDSCAPE":
      return new DerbySqlDialect(c);
    case "EXASOL":
      return new ExasolSqlDialect(c);
    case "FIREBOLT":
      return new FireboltSqlDialect(c);
    case "HIVE":
      return new HiveSqlDialect(c);
    case "INGRES":
      return new IngresSqlDialect(c);
    case "INTERBASE":
      return new InterbaseSqlDialect(c);
    case "JETHRODATA":
      return new JethroDataSqlDialect(
          c.withJethroInfo(jethroCache.get(databaseMetaData)));
    case "LUCIDDB":
      return new LucidDbSqlDialect(c);
    case "ORACLE":
      return new OracleSqlDialect(c);
    case "PHOENIX":
      return new PhoenixSqlDialect(c);
    case "MYSQL (INFOBRIGHT)":
      return new InfobrightSqlDialect(c);
    case "MYSQL":
      return new MysqlSqlDialect(
          c.withDataTypeSystem(MysqlSqlDialect.MYSQL_TYPE_SYSTEM));
    case "REDSHIFT":
      return new RedshiftSqlDialect(
          c.withDataTypeSystem(RedshiftSqlDialect.TYPE_SYSTEM));
    case "SNOWFLAKE":
      return new SnowflakeSqlDialect(c);
    case "SPARK":
      return new SparkSqlDialect(c);
    default:
      break;
    }
    // Now the fuzzy matches.
    if (upperProductName.startsWith("DB2")) {
      return new Db2SqlDialect(c);
    } else if (upperProductName.contains("FIREBIRD")) {
      return new FirebirdSqlDialect(c);
    } else if (upperProductName.contains("FIREBOLT")) {
      return new FireboltSqlDialect(c);
    } else if (upperProductName.contains("GOOGLE BIGQUERY")
        || upperProductName.contains("GOOGLE BIG QUERY")) {
      return new BigQuerySqlDialect(c);
    } else if (upperProductName.startsWith("INFORMIX")) {
      return new InformixSqlDialect(c);
    } else if (upperProductName.contains("NETEZZA")) {
      return new NetezzaSqlDialect(c);
    } else if (upperProductName.contains("PARACCEL")) {
      return new ParaccelSqlDialect(c);
    } else if (upperProductName.startsWith("HP NEOVIEW")) {
      return new NeoviewSqlDialect(c);
    } else if (upperProductName.contains("POSTGRE")) {
      return new PostgresqlSqlDialect(
          c.withDataTypeSystem(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM));
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
    } else if (upperProductName.contains("SNOWFLAKE")) {
      return new SnowflakeSqlDialect(c);
    } else if (upperProductName.contains("SPARK")) {
      return new SparkSqlDialect(c);
    } else {
      return new AnsiSqlDialect(c);
    }
  }

  /** Returns a basic dialect for a given product, or null if none is known. */
  static @Nullable SqlDialect simple(SqlDialect.DatabaseProduct databaseProduct) {
    switch (databaseProduct) {
    case ACCESS:
      return AccessSqlDialect.DEFAULT;
    case BIG_QUERY:
      return BigQuerySqlDialect.DEFAULT;
    case CALCITE:
      return CalciteSqlDialect.DEFAULT;
    case CLICKHOUSE:
      return ClickHouseSqlDialect.DEFAULT;
    case DB2:
      return Db2SqlDialect.DEFAULT;
    case DERBY:
      return DerbySqlDialect.DEFAULT;
    case EXASOL:
      return ExasolSqlDialect.DEFAULT;
    case FIREBIRD:
      return FirebirdSqlDialect.DEFAULT;
    case FIREBOLT:
      return FireboltSqlDialect.DEFAULT;
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
    case PRESTO:
      return PrestoSqlDialect.DEFAULT;
    case REDSHIFT:
      return RedshiftSqlDialect.DEFAULT;
    case SNOWFLAKE:
      return SnowflakeSqlDialect.DEFAULT;
    case SPARK:
      return SparkSqlDialect.DEFAULT;
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
