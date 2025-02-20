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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.JethroDataSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Util;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

import static org.apache.calcite.util.Util.first;

/** Utilities for {@link DialectTestConfig}. */
class DialectTestConfigs {
  private DialectTestConfigs() {
  }

  static final Supplier<DialectTestConfig> INSTANCE_SUPPLIER =
      Suppliers.memoize(() -> {
        final ImmutableList.Builder<DialectTestConfig.Dialect> b =
            ImmutableList.builder();
        for (DialectCode dialectCode : DialectCode.values()) {
          b.add(dialectCode.toDialect());
        }
        final ImmutableList<DialectTestConfig.Dialect> list = b.build();
        final Iterable<String> dialectNames =
            Util.transform(list, dialect -> dialect.name);
        if (!Ordering.natural().isOrdered(dialectNames)) {
          throw new AssertionError("not ordered: " + dialectNames);
        }
        return DialectTestConfig.of(list);
      })::get;


  @SuppressWarnings("SameParameterValue")
  static HiveSqlDialect hiveDialect(int majorVersion, int minorVersion) {
    return new HiveSqlDialect(HiveSqlDialect.DEFAULT_CONTEXT
        .withDatabaseMajorVersion(majorVersion)
        .withDatabaseMinorVersion(minorVersion)
        .withNullCollation(NullCollation.LOW));
  }

  static SqlDialect mysqlDialect(@Nullable Integer majorVersion,
      @Nullable NullCollation nullCollation) {
    final SqlDialect d = SqlDialect.DatabaseProduct.MYSQL.getDialect();
    SqlDialect.Context context =
        MysqlSqlDialect.DEFAULT_CONTEXT
            .withIdentifierQuoteString(d.quoteIdentifier("").substring(0, 1))
            .withNullCollation(first(nullCollation, d.getNullCollation()));
    if (majorVersion != null) {
      context = context.withDatabaseMajorVersion(majorVersion);
    }
    if (nullCollation == null) {
      // Historically, the MYSQL_8 dialect used in tests was an instance of
      // SqlDialect, not MysqlSqlDialect. Preserve that behavior for now.
      return new SqlDialect(context);
    }
    return new MysqlSqlDialect(context);
  }

  static SqlDialect oracleDialect(final @Nullable Integer majorVersion,
      final @Nullable Integer maxVarcharLength) {
    final SqlDialect oracleDialect = OracleSqlDialect.DEFAULT;
    SqlDialect.Context context =
        OracleSqlDialect.DEFAULT_CONTEXT
            .withIdentifierQuoteString(oracleDialect.quoteIdentifier("")
                .substring(0, 1))
            .withNullCollation(oracleDialect.getNullCollation());
    if (maxVarcharLength != null) {
      context = context.withDataTypeSystem(new RelDataTypeSystemImpl() {
        @Override public int getMaxPrecision(SqlTypeName typeName) {
          switch (typeName) {
          case VARCHAR:
            return maxVarcharLength;
          default:
            return super.getMaxPrecision(typeName);
          }
        }
      });
    }

    if (majorVersion != null) {
      context =
          context.withDatabaseMajorVersion(majorVersion);
    }
    return new OracleSqlDialect(context);
  }

  static SqlDialect postgresqlDialect(final @Nullable Integer maxVarcharLength,
      final boolean modifyDecimal) {
    SqlDialect.Context context = PostgresqlSqlDialect.DEFAULT_CONTEXT;
    if (maxVarcharLength != null) {
      context =
          context
              .withDataTypeSystem(new RelDataTypeSystemImpl() {
                @Override public int getMaxPrecision(SqlTypeName typeName) {
                  switch (typeName) {
                  case VARCHAR:
                    return maxVarcharLength;
                  default:
                    return super.getMaxPrecision(typeName);
                  }
                }
              });
    }
    if (modifyDecimal) {
      context =
          context.withDataTypeSystem(
              new RelDataTypeSystemImpl() {
                @Override public int getMaxNumericScale() {
                  return getMaxScale(SqlTypeName.DECIMAL);
                }

                @Override public int getMaxScale(SqlTypeName typeName) {
                  switch (typeName) {
                  case DECIMAL:
                    return 10;
                  default:
                    return super.getMaxScale(typeName);
                  }
                }

                @Override public int getMaxNumericPrecision() {
                  return getMaxPrecision(SqlTypeName.DECIMAL);
                }

                @Override public int getMaxPrecision(SqlTypeName typeName) {
                  switch (typeName) {
                  case DECIMAL:
                    return 39;
                  default:
                    return super.getMaxPrecision(typeName);
                  }
                }
              });
    }
    return new PostgresqlSqlDialect(context);
  }

  /** Creates a dialect for Microsoft SQL Server.
   *
   * <p>MSSQL 2008 has version 10.0, 2012 has 11.0, 2017 has 14.0. */
  static SqlDialect mssqlDialect(int majorVersion) {
    final SqlDialect mssqlDialect =
        SqlDialect.DatabaseProduct.MSSQL.getDialect();
    return new MssqlSqlDialect(MssqlSqlDialect.DEFAULT_CONTEXT
        .withDatabaseMajorVersion(majorVersion)
        .withIdentifierQuoteString(mssqlDialect.quoteIdentifier("")
            .substring(0, 1))
        .withNullCollation(mssqlDialect.getNullCollation()));
  }

  /** Creates a dialect that doesn't treat integer literals in the ORDER BY as
   * field references. */
  static SqlDialect nonOrdinalDialect() {
    return new SqlDialect(SqlDialect.EMPTY_CONTEXT) {
      @Override public SqlConformance getConformance() {
        return SqlConformanceEnum.STRICT_99;
      }
    };
  }

  static final Supplier<SqlDialect> JETHRO_DIALECT_SUPPLIER =
      Suppliers.memoize(() ->
          new JethroDataSqlDialect(
              SqlDialect.EMPTY_CONTEXT
                  .withDatabaseProduct(SqlDialect.DatabaseProduct.JETHRO)
                  .withDatabaseMajorVersion(1)
                  .withDatabaseMinorVersion(0)
                  .withDatabaseVersion("1.0")
                  .withIdentifierQuoteString("\"")
                  .withNullCollation(NullCollation.HIGH)
                  .withJethroInfo(JethroDataSqlDialect.JethroInfo.EMPTY)));
}
