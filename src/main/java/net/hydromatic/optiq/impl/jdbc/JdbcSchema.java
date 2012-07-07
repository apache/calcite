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

import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.optiq.Member;
import net.hydromatic.optiq.Parameter;
import net.hydromatic.optiq.Schema;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeName;

import java.sql.*;
import java.util.Collections;
import java.util.List;
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
    private final DataSource dataSource;
    private final String catalog;
    private final String schema;
    private final RelDataTypeFactory typeFactory;

    /**
     * Creates a JDBC schema.
     *
     * @param dataSource Data source
     * @param catalog Catalog name, or null
     * @param schema Schema name pattern
     * @param typeFactory Type factory
     */
    public JdbcSchema(
        DataSource dataSource,
        String catalog,
        String schema,
        RelDataTypeFactory typeFactory)
    {
        super();
        this.dataSource = dataSource;
        this.catalog = catalog;
        this.schema = schema;
        this.typeFactory = typeFactory;
    }

    public List<Member> getMembers(final String name) {
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
                return Collections.emptyList();
            }
            final String tableName = resultSet.getString(2);
            resultSet.close();
            resultSet = metaData.getColumns(
                connection.getCatalog(),
                connection.getSchema(),
                tableName,
                null);
            final RelDataTypeFactory.FieldInfoBuilder fieldInfo =
                new RelDataTypeFactory.FieldInfoBuilder();
            while (resultSet.next()) {
                final String columnName = resultSet.getString(3);
                final int dataType = resultSet.getInt(5);
                final int size = resultSet.getInt(7);
                final int scale = resultSet.getInt(9);
                RelDataType sqlType = typeFactory.createSqlType(
                    SqlTypeName.getNameForJdbcType(dataType), size, scale);
                boolean nullable = resultSet.getBoolean(11);
                if (nullable) {
                    sqlType =
                        typeFactory.createTypeWithNullability(sqlType,  true);
                }
                fieldInfo.add(columnName, sqlType);
            }
            final RelDataType type = typeFactory.createStructType(fieldInfo);
            return Collections.<Member>singletonList(
                new Member() {
                    public List<Parameter> getParameters() {
                        return Collections.emptyList();
                    }

                    public RelDataType getType() {
                        return type;
                    }

                    public Queryable evaluate(
                        Object target, List<Object> arguments)
                    {
                        assert arguments.isEmpty();
                        return getTableQueryable(tableName);
                    }

                    public String getName() {
                        return tableName;
                    }
                });
        } catch (SQLException e) {
            throw new RuntimeException(
                "Exception while reading definition of table '" + name + "'",
                e);
        } finally {
            close(connection, null, resultSet);
        }
    }

    private Queryable getTableQueryable(String tableName) {
        throw new UnsupportedOperationException(); // TODO:
    }

    public Expression getSubSchemaExpression(
        Expression schemaExpression, Schema schema, String name)
    {
        // JDBC has no sub-schemas.
        throw new UnsupportedOperationException();
    }

    public Expression getMemberExpression(
        Expression schemaExpression, Member member, List<Expression> arguments)
    {
        throw new UnsupportedOperationException(
            "schemaExpression=" + schemaExpression
            + ", schemaObject=" + member
            + ", arguments=" + arguments);
    }

    public Schema getSubSchema(String name) {
        // JDBC does not support sub-schemas.
        return null;
    }

    public Object getSubSchemaInstance(
        Object schemaInstance, String subSchemaName, Schema subSchema)
    {
        // JDBC does not support sub-schemas.
        throw new UnsupportedOperationException();
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
