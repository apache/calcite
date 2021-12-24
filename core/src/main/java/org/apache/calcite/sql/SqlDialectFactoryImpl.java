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
    SqlDialect.Context context = SqlDialects.createContext(databaseMetaData);

    String databaseProductName = context.databaseProductName();
    try {
      if (databaseProductName == null) {
        databaseProductName = databaseMetaData.getDatabaseProductName();
      }
    } catch (SQLException e) {
      throw new RuntimeException("while detecting database product", e);
    }
    String upperProductName = databaseProductName.toUpperCase(Locale.ROOT).trim();

    switch (upperProductName) {
    case "ACCESS":
      return new AccessSqlDialect(context);
    case "APACHE DERBY":
      return new DerbySqlDialect(context);
    case "CLICKHOUSE":
      return new ClickHouseSqlDialect(context);
    case "DBMS:CLOUDSCAPE":
      return new DerbySqlDialect(context);
    case "EXASOL":
      return new ExasolSqlDialect(context);
    case "HIVE":
      return new HiveSqlDialect(context);
    case "INGRES":
      return new IngresSqlDialect(context);
    case "INTERBASE":
      return new InterbaseSqlDialect(context);
    case "JETHRODATA":
      return new JethroDataSqlDialect(
          context.withJethroInfo(jethroCache.get(databaseMetaData)));
    case "LUCIDDB":
      return new LucidDbSqlDialect(context);
    case "ORACLE":
      return new OracleSqlDialect(context);
    case "PHOENIX":
      return new PhoenixSqlDialect(context);
    case "MYSQL (INFOBRIGHT)":
      return new InfobrightSqlDialect(context);
    case "MYSQL":
      return new MysqlSqlDialect(
          context.withDataTypeSystem(MysqlSqlDialect.MYSQL_TYPE_SYSTEM));
    case "REDSHIFT":
      return new RedshiftSqlDialect(
          context.withDataTypeSystem(RedshiftSqlDialect.TYPE_SYSTEM));
    case "SNOWFLAKE":
      return new SnowflakeSqlDialect(context);
    case "SPARK":
      return new SparkSqlDialect(context);
    default:
      break;
    }
    // Now the fuzzy matches.
    if (databaseProductName.startsWith("DB2")) {
      return new Db2SqlDialect(context);
    } else if (upperProductName.contains("FIREBIRD")) {
      return new FirebirdSqlDialect(context);
    } else if (databaseProductName.startsWith("Informix")) {
      return new InformixSqlDialect(context);
    } else if (upperProductName.contains("NETEZZA")) {
      return new NetezzaSqlDialect(context);
    } else if (upperProductName.contains("PARACCEL")) {
      return new ParaccelSqlDialect(context);
    } else if (databaseProductName.startsWith("HP Neoview")) {
      return new NeoviewSqlDialect(context);
    } else if (upperProductName.contains("POSTGRE")) {
      return new PostgresqlSqlDialect(
          context.withDataTypeSystem(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM));
    } else if (upperProductName.contains("SQL SERVER")) {
      return new MssqlSqlDialect(context);
    } else if (upperProductName.contains("SYBASE")) {
      return new SybaseSqlDialect(context);
    } else if (upperProductName.contains("TERADATA")) {
      return new TeradataSqlDialect(context);
    } else if (upperProductName.contains("HSQL")) {
      return new HsqldbSqlDialect(context);
    } else if (upperProductName.contains("H2")) {
      return new H2SqlDialect(context);
    } else if (upperProductName.contains("VERTICA")) {
      return new VerticaSqlDialect(context);
    } else if (upperProductName.contains("SNOWFLAKE")) {
      return new SnowflakeSqlDialect(context);
    } else if (upperProductName.contains("SPARK")) {
      return new SparkSqlDialect(context);
    } else {
      return new AnsiSqlDialect(context);
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
    case SYBASE:
      return SybaseSqlDialect.DEFAULT;
    case TERADATA:
      return TeradataSqlDialect.DEFAULT;
    case VERTICA:
      return VerticaSqlDialect.DEFAULT;
    case SPARK:
      return SparkSqlDialect.DEFAULT;
    case SQLSTREAM:
    case UNKNOWN:
    default:
      return null;
    }
  }
}
