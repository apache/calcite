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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.lookup.IgnoreCaseLookup;
import org.apache.calcite.schema.lookup.LikePattern;
import org.apache.calcite.schema.lookup.LoadingCacheLookup;
import org.apache.calcite.schema.lookup.Lookup;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.sql.DataSource;

import static java.util.Objects.requireNonNull;

/**
 * Schema based upon a JDBC catalog (database).
 *
 * <p>This schema does not directly contain tables, but contains a sub-schema
 * for each schema in the catalog in the back-end. Each of those sub-schemas is
 * an instance of {@link JdbcSchema}.
 *
 * <p>This schema is lazy: it does not compute the list of schema names until
 * the first call to {@link #subSchemas()} and {@link Lookup#get(String)}. Then it creates a
 * {@link JdbcSchema} for this schema name. Each JdbcSchema will populate its
 * tables on demand.
 */
public class JdbcCatalogSchema extends JdbcBaseSchema implements Wrapper {
  final DataSource dataSource;
  public final SqlDialect dialect;
  final JdbcConvention convention;
  final String catalog;
  private final Lookup<JdbcSchema> subSchemas;

  /** default schema name, lazily initialized. */
  @SuppressWarnings({"method.invocation.invalid", "Convert2MethodRef"})
  private final Supplier<Optional<String>> defaultSchemaName =
      Suppliers.memoize(() -> Optional.ofNullable(computeDefaultSchemaName()));

  /** Creates a JdbcCatalogSchema. */
  public JdbcCatalogSchema(DataSource dataSource, SqlDialect dialect,
      JdbcConvention convention, String catalog) {
    this.dataSource = requireNonNull(dataSource, "dataSource");
    this.dialect = requireNonNull(dialect, "dialect");
    this.convention = requireNonNull(convention, "convention");
    this.catalog = catalog;
    this.subSchemas = new LoadingCacheLookup<>(new IgnoreCaseLookup<JdbcSchema>() {
      @Override public @Nullable JdbcSchema get(String name) {
        try (Connection connection = dataSource.getConnection();
            ResultSet resultSet =
                connection.getMetaData().getSchemas(catalog, name)) {
          while (resultSet.next()) {
            final String schemaName =
                requireNonNull(resultSet.getString(1),
                    "got null schemaName from the database");
            return new JdbcSchema(dataSource, dialect, convention, catalog, schemaName);
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return null;
      }

      @Override public Set<String> getNames(LikePattern pattern) {
        final ImmutableSet.Builder<String> builder =
            ImmutableSet.builder();
        try (Connection connection = dataSource.getConnection();
            ResultSet resultSet =
                connection.getMetaData().getSchemas(catalog, pattern.pattern)) {
          while (resultSet.next()) {
            builder.add(
                requireNonNull(resultSet.getString(1),
                    "got null schemaName from the database"));
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return builder.build();
      }
    });
  }

  public static JdbcCatalogSchema create(
      @Nullable SchemaPlus parentSchema,
      String name,
      DataSource dataSource,
      String catalog) {
    return create(parentSchema, name, dataSource,
        SqlDialectFactoryImpl.INSTANCE, catalog);
  }

  public static JdbcCatalogSchema create(
      @Nullable SchemaPlus parentSchema,
      String name,
      DataSource dataSource,
      SqlDialectFactory dialectFactory,
      String catalog) {
    final Expression expression =
        parentSchema != null
            ? Schemas.subSchemaExpression(parentSchema, name,
                JdbcCatalogSchema.class)
            : Expressions.call(DataContext.ROOT,
                BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);
    final SqlDialect dialect =
        JdbcSchema.createDialect(dialectFactory, dataSource);
    final JdbcConvention convention =
        JdbcConvention.of(dialect, expression, name);
    return new JdbcCatalogSchema(dataSource, dialect, convention, catalog);
  }

  @Override public Lookup<Table> tables() {
    return Lookup.empty();
  }

  @Override public Lookup<? extends Schema> subSchemas() {
    return subSchemas;
  }

  private @Nullable String computeDefaultSchemaName() {
    try (Connection connection = dataSource.getConnection()) {
      return connection.getSchema();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns the name of the default sub-schema. */
  public @Nullable String getDefaultSubSchemaName() {
    return defaultSchemaName.get().orElse(null);
  }

  /** Returns the data source. */
  public DataSource getDataSource() {
    return dataSource;
  }


  @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
    if (clazz.isInstance(this)) {
      return clazz.cast(this);
    }
    if (clazz == DataSource.class) {
      return clazz.cast(getDataSource());
    }
    return null;
  }
}
