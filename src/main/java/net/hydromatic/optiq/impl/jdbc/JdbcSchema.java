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
package net.hydromatic.optiq.impl.jdbc;

import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.expressions.Expression;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlDialect;
import org.eigenbase.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 * Implementation of {@link Schema} that is backed by a JDBC data source.
 *
 * <p>The tables in the JDBC data source appear to be tables in this schema;
 * queries against this schema are executed against those tables, pushing down
 * as much as possible of the query logic to SQL.</p>
 *
 * @author jhyde
 */
public class JdbcSchema implements Schema {
    final QueryProvider queryProvider;
    final DataSource dataSource;
    private final String catalog;
    private final String schema;
    final JavaTypeFactory typeFactory;
    private final Expression expression;
    final SqlDialect dialect;

    /**
     * Creates a JDBC schema.
     *
     * @param queryProvider Query provider
     * @param dataSource Data source
     * @param dialect SQL dialect
     * @param catalog Catalog name, or null
     * @param schema Schema name pattern
     * @param typeFactory Type factory
     */
    public JdbcSchema(
        QueryProvider queryProvider,
        DataSource dataSource,
        SqlDialect dialect,
        String catalog,
        String schema,
        JavaTypeFactory typeFactory,
        Expression expression)
    {
        super();
        this.queryProvider = queryProvider;
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.catalog = catalog;
        this.schema = schema;
        this.typeFactory = typeFactory;
        this.expression = expression;
        assert expression != null;
        assert typeFactory != null;
        assert dialect != null;
        assert queryProvider != null;
        assert dataSource != null;
    }

    /**
     * Creates a JdbcSchema within another schema.
     *
     * @param optiqConnection Connection to Optiq (also a query provider)
     * @param parentSchema Parent schema
     * @param dataSource Data source
     * @param jdbcCatalog Catalog name, or null
     * @param jdbcSchema Schema name pattern
     * @param name Name of new schema
     * @return New JdbcSchema
     */
    public static JdbcSchema create(
        OptiqConnection optiqConnection,
        MutableSchema parentSchema,
        DataSource dataSource,
        String jdbcCatalog,
        String jdbcSchema,
        String name)
    {
        JdbcSchema schema =
            new JdbcSchema(
                optiqConnection,
                dataSource,
                JdbcSchema.createDialect(dataSource),
                jdbcCatalog,
                jdbcSchema,
                optiqConnection.getTypeFactory(),
                parentSchema.getSubSchemaExpression(
                    name, Schema.class));
        parentSchema.addSchema(name, schema);
        return schema;
    }

    /** Returns a suitable SQL dialect for the given data source. */
    public static SqlDialect createDialect(DataSource dataSource) {
        return JdbcUtils.DialectPool.INSTANCE.get(dataSource);
    }

    public Expression getExpression() {
        return expression;
    }

    public QueryProvider getQueryProvider() {
        return queryProvider;
    }

    public List<TableFunction> getTableFunctions(String name) {
        return Collections.emptyList();
    }

    public <T> Table<T> getTable(String name, Class<T> elementType) {
        assert elementType != null;
        //noinspection unchecked
        return getTable(name);
    }

    public Table getTable(String name) {
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getTables(
                catalog,
                schema,
                name,
                null);
            if (!resultSet.next()) {
                return null;
            }
            final String catalogName = resultSet.getString(1);
            final String schemaName = resultSet.getString(2);
            final String tableName = resultSet.getString(3);
            resultSet.close();
            resultSet = metaData.getColumns(
                catalogName,
                schemaName,
                tableName,
                null);
            final RelDataTypeFactory.FieldInfoBuilder fieldInfo =
                new RelDataTypeFactory.FieldInfoBuilder();
            while (resultSet.next()) {
                final String columnName = resultSet.getString(4);
                final int dataType = resultSet.getInt(5);
                final int size = resultSet.getInt(7);
                final int scale = resultSet.getInt(9);
                RelDataType sqlType = zzz(dataType, size, scale);
                boolean nullable = resultSet.getBoolean(11);
                if (nullable) {
                    sqlType =
                        typeFactory.createTypeWithNullability(sqlType,  true);
                }
                fieldInfo.add(columnName, sqlType);
            }
            final RelDataType type =
                typeFactory.createStructType(fieldInfo);
            Type javaType = typeFactory.getJavaClass(type);
            return new JdbcTable<Object>(javaType, this, name);
        } catch (SQLException e) {
            throw new RuntimeException(
                "Exception while reading definition of table '" + name + "'",
                e);
        } finally {
            close(connection, null, resultSet);
        }
    }

    private RelDataType zzz(int dataType, int precision, int scale) {
        SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(dataType);
        if (precision >= 0
            && scale >= 0
            && sqlTypeName.allowsPrecScale(true, true))
        {
            return typeFactory.createSqlType(sqlTypeName, precision, scale);
        } else if (precision >= 0 && sqlTypeName.allowsPrecNoScale()) {
            return typeFactory.createSqlType(sqlTypeName, precision);
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            return typeFactory.createSqlType(sqlTypeName);
        }
    }

    public Map<String, List<TableFunction>> getTableFunctions() {
        // TODO: populate map from JDBC metadata
        return Collections.emptyMap();
    }

    public Schema getSubSchema(String name) {
        // JDBC does not support sub-schemas.
        return null;
    }

    private static void close(
        Connection connection, Statement statement, ResultSet resultSet)
    {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }
}

// End JdbcSchema.java
