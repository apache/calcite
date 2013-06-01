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

import net.hydromatic.linq4j.expressions.Primitive;
import net.hydromatic.linq4j.function.*;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.sql.SqlDialect;

import java.sql.*;
import java.util.*;
import javax.sql.DataSource;

/**
 * Utilities for the JDBC provider.
 *
 * @author jhyde
 */
final class JdbcUtils {
    private JdbcUtils() {
        throw new AssertionError("no instances!");
    }

    static List<Primitive> getPrimitives(
        JavaTypeFactory typeFactory, RelDataType rowType)
    {
        final List<RelDataTypeField> fields = rowType.getFieldList();
        final List<Primitive> primitiveList = new ArrayList<Primitive>();
        for (RelDataTypeField field : fields) {
            Class clazz = (Class) typeFactory.getJavaClass(field.getType());
            primitiveList.add(
                Primitive.of(clazz) != null
                    ? Primitive.of(clazz)
                    : Primitive.OTHER);
        }
        return primitiveList;
    }

    public static class DialectPool {
        final Map<List, SqlDialect> map = new HashMap<List, SqlDialect>();

        public static final DialectPool INSTANCE = new DialectPool();

        SqlDialect get(DataSource dataSource) {
            Connection connection = null;
            try {
                connection = dataSource.getConnection();
                DatabaseMetaData metaData = connection.getMetaData();
                String productName = metaData.getDatabaseProductName();
                String productVersion = metaData.getDatabaseProductVersion();
                List key = Arrays.asList(productName, productVersion);
                SqlDialect dialect = map.get(key);
                if (dialect == null) {
                    dialect =
                        new SqlDialect(
                            SqlDialect.getProduct(productName, productVersion),
                            productName,
                            metaData.getIdentifierQuoteString());
                    map.put(key, dialect);
                }
                connection.close();
                connection = null;
                return dialect;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        // ignore
                    }
                }
            }
        }
    }

    /** Builder that calls {@link ResultSet#getObject(int)} for every column,
     * or {@code getXxx} if the result type is a primitive {@code xxx},
     * and returns an array of objects for each row. */
    public static class ObjectArrayRowBuilder implements Function0<Object[]> {
        private final ResultSet resultSet;
        private final int columnCount;
        private final Primitive[] primitives;

        public ObjectArrayRowBuilder(
            ResultSet resultSet, Primitive[] primitives) throws SQLException
        {
            this.resultSet = resultSet;
            this.primitives = primitives;
            this.columnCount = resultSet.getMetaData().getColumnCount();
        }

        public static Function1<ResultSet, Function0<Object[]>> factory(
            List<Primitive> primitiveList)
        {
            final Primitive[] primitives =
                primitiveList.toArray(new Primitive[primitiveList.size()]);
            return new Function1<ResultSet, Function0<Object[]>>() {
                public Function0<Object[]> apply(ResultSet resultSet) {
                    try {
                        return new ObjectArrayRowBuilder(resultSet, primitives);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        public Object[] apply() {
            try {
                final Object[] values = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    values[i] = value(i);
                }
                return values;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        private Object value(int i) throws SQLException {
            return primitives[i].jdbcGet(resultSet, i + 1);
        }
    }
}

// End JdbcUtils.java
