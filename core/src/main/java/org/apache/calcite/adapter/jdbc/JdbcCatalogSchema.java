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
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDialectFactory;
import org.apache.calcite.sql.SqlDialectFactoryImpl;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * Schema based upon a JDBC catalog (database).
 *
 * <p>This schema does not directly contain tables, but contains a sub-schema
 * for each schema in the catalog in the back-end. Each of those sub-schemas is
 * an instance of {@link JdbcSchema}.
 *
 * <p>This schema is lazy: it does not compute the list of schema names until
 * the first call to {@link #getSubSchemaMap()}. Then it creates a
 * {@link JdbcSchema} for each schema name. Each JdbcSchema will populate its
 * tables on demand.
 */
public class JdbcCatalogSchema extends AbstractSchema {
  final DataSource dataSource;
  public final SqlDialect dialect;
  final JdbcConvention convention;
  final String catalog;

  /** Sub-schemas by name, lazily initialized. */
  final Supplier<SubSchemaMap> subSchemaMapSupplier =
      Suppliers.memoize(() -> computeSubSchemaMap());

  /** Creates a JdbcCatalogSchema. */
  public JdbcCatalogSchema(DataSource dataSource, SqlDialect dialect,
      JdbcConvention convention, String catalog) {
    this.dataSource = Objects.requireNonNull(dataSource);
    this.dialect = Objects.requireNonNull(dialect);
    this.convention = Objects.requireNonNull(convention);
    this.catalog = catalog;
  }

  public static JdbcCatalogSchema create(
      SchemaPlus parentSchema,
      String name,
      DataSource dataSource,
      String catalog) {
    return create(parentSchema, name, dataSource,
        SqlDialectFactoryImpl.INSTANCE, catalog);
  }

  public static JdbcCatalogSchema create(
      SchemaPlus parentSchema,
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

  private SubSchemaMap computeSubSchemaMap() {
    final ImmutableMap.Builder<String, Schema> builder =
        ImmutableMap.builder();
    String defaultSchemaName;
    try (Connection connection = dataSource.getConnection();
         ResultSet resultSet =
             connection.getMetaData().getSchemas(catalog, null)) {
      defaultSchemaName = connection.getSchema();
      while (resultSet.next()) {
        final String schemaName = resultSet.getString(1);
        builder.put(schemaName,
            new JdbcSchema(dataSource, dialect, convention, catalog, schemaName));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return new SubSchemaMap(defaultSchemaName, builder.build());
  }

  @Override protected Map<String, Schema> getSubSchemaMap() {
    return subSchemaMapSupplier.get().map;
  }

  /** Returns the name of the default sub-schema. */
  public String getDefaultSubSchemaName() {
    return subSchemaMapSupplier.get().defaultSchemaName;
  }

  /** Returns the data source. */
  public DataSource getDataSource() {
    return dataSource;
  }

  /** Contains sub-schemas by name, and the name of the default schema. */
  private static class SubSchemaMap {
    final String defaultSchemaName;
    final ImmutableMap<String, Schema> map;

    private SubSchemaMap(String defaultSchemaName,
        ImmutableMap<String, Schema> map) {
      this.defaultSchemaName = defaultSchemaName;
      this.map = map;
    }
  }
}

// End JdbcCatalogSchema.java
