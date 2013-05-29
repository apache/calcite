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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.function.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.TableInSchemaImpl;
import net.hydromatic.optiq.impl.java.MapSchema;

import net.hydromatic.optiq.runtime.*;

import org.eigenbase.reltype.*;
import org.eigenbase.util.Pair;

import java.lang.reflect.Field;
import java.sql.*;
import java.sql.Types;
import java.util.*;

/**
 * Helper for implementing the {@code getXxx} methods such as
 * {@link OptiqDatabaseMetaData#getTables}.
 */
public class Meta {
    final OptiqConnectionImpl connection;

    public Meta(OptiqConnectionImpl connection) {
        this.connection = connection;
    }

    static <T extends Named> Predicate1<T> namedMatcher(final String pattern) {
        return new Predicate1<T>() {
            public boolean apply(T v1) {
                return matches(v1.getName(), pattern);
            }
        };
    }

    static Predicate1<String> matcher(final String pattern) {
        return new Predicate1<String>() {
            public boolean apply(String v1) {
                return matches(v1, pattern);
            }
        };
    }

    public static boolean matches(String element, String pattern) {
        return pattern == null
               || pattern.equals("%")
               || element.equals(pattern); // TODO: better wildcard
    }

    /** Creates the data dictionary, also called the information schema. It is a
     * schema called "metadata" that contains tables "TABLES", "COLUMNS" etc. */
    MapSchema createInformationSchema() {
        final MapSchema mapSchema =
            MapSchema.create(connection.getRootSchema(), "metadata");
        mapSchema.addTable(
            new TableInSchemaImpl(
                mapSchema,
                "TABLES",
                Schema.TableType.SYSTEM_TABLE,
                new MetadataTable<MetaTable>(
                    connection,
                    mapSchema,
                    "TABLES",
                    MetaTable.class)
                {
                    public Enumerator<MetaTable> enumerator() {
                        return schemas(connection.getCatalog())
                            .selectMany(
                                new Function1<
                                        MetaSchema, Enumerable<MetaTable>>() {
                                    public Enumerable<MetaTable> apply(
                                        MetaSchema a0)
                                    {
                                        return tables(a0);
                                    }
                                })
                            .enumerator();
                    }
                }));
        mapSchema.addTable(
            new TableInSchemaImpl(
                mapSchema,
                "COLUMNS",
                Schema.TableType.SYSTEM_TABLE,
                new MetadataTable<MetaColumn>(
                    connection,
                    mapSchema,
                    "COLUMNS",
                    MetaColumn.class)
                {
                    public Enumerator<MetaColumn> enumerator() {
                        return schemas(connection.getCatalog())
                            .selectMany(
                                new Function1<
                                        MetaSchema, Enumerable<MetaTable>>() {
                                    public Enumerable<MetaTable> apply(
                                        MetaSchema a0)
                                    {
                                        return tables(a0);
                                    }
                                })
                            .selectMany(
                                new Function1<
                                        MetaTable, Enumerable<MetaColumn>>() {
                                    public Enumerable<MetaColumn> apply(
                                        MetaTable a0)
                                    {
                                        return columns(a0);
                                    }
                                })
                            .enumerator();
                    }
                }));
        return mapSchema;
    }

    private ResultSet createResultSet(
        final Enumerable<?> enumerable,
        final NamedFieldGetter columnGetter)
    {
        try {
            return connection.driver.factory.newResultSet(
                connection.createStatement(),
                columnGetter.columnNames,
                new Function0<Cursor>() {
                    public Cursor apply() {
                        return columnGetter.cursor(
                            ((Enumerable) enumerable).enumerator());
                    }
                }).execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    ResultSet getTables(
        String catalog,
        final String schemaPattern,
        final String tableNamePattern,
        String[] types) throws SQLException
    {
        final Predicate1<MetaTable> typeFilter;
        if (types == null) {
            typeFilter = Functions.truePredicate1();
        } else {
            final List<String> typeList = Arrays.asList(types);
            typeFilter = new Predicate1<MetaTable>() {
                public boolean apply(MetaTable v1) {
                    return typeList.contains(v1.tableType);
                }
            };
        }
        return createResultSet(
            schemas(catalog)
                .where(Meta.<MetaSchema>namedMatcher(schemaPattern))
                .selectMany(
                    new Function1<MetaSchema, Enumerable<MetaTable>>() {
                        public Enumerable<MetaTable> apply(MetaSchema a0) {
                            return tables(a0)
                                .where(
                                    Meta.<MetaTable>namedMatcher(
                                        tableNamePattern))
                                .concat(
                                    tableFunctions(
                                        a0, matcher(tableNamePattern)));
                        }
                    })
                .where(typeFilter),
            new NamedFieldGetter(
                MetaTable.class,
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "TABLE_TYPE",
                "REMARKS",
                "TYPE_CAT",
                "TYPE_SCHEM",
                "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION"));
    }

    ResultSet getColumns(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String columnNamePattern)
    {
        return createResultSet(
            schemas(catalog)
                .where(Meta.<MetaSchema>namedMatcher(schemaPattern))
                .selectMany(
                    new Function1<MetaSchema, Enumerable<MetaTable>>() {
                        public Enumerable<MetaTable> apply(MetaSchema a0) {
                            return tables(a0);
                        }
                    })
                .where(
                    Meta.<MetaTable>namedMatcher(tableNamePattern))
                .selectMany(
                    new Function1<MetaTable, Enumerable<MetaColumn>>() {
                        public Enumerable<MetaColumn> apply(MetaTable a0) {
                            return columns(a0);
                        }
                    })
                .where(Meta.<MetaColumn>namedMatcher(columnNamePattern)),
            new NamedFieldGetter(
                MetaColumn.class,
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "COLUMN_NAME",
                "DATA_TYPE",
                "TYPE_NAME",
                "COLUMN_SIZE",
                "BUFFER_LENGTH",
                "DECIMAL_DIGITS",
                "NUM_PREC_RADIX",
                "NULLABLE",
                "REMARKS",
                "COLUMN_DEF",
                "SQL_DATA_TYPE",
                "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION",
                "IS_NULLABLE",
                "SCOPE_CATALOG",
                "SCOPE_TABLE",
                "SOURCE_DATA_TYPE",
                "IS_AUTOINCREMENT",
                "IS_GENERATEDCOLUMN"));
    }

    Enumerable<MetaSchema> schemas(String catalog) {
        Collection<String> schemaNames =
            connection.rootSchema.getSubSchemaNames();
        return Linq4j.asEnumerable(schemaNames)
            .select(
                new Function1<String, MetaSchema>() {
                    public MetaSchema apply(String name) {
                        return new MetaSchema(
                            connection.rootSchema.getSubSchema(name),
                            connection.getCatalog(),
                            name);
                    }
                });
    }

    Enumerable<MetaTable> tables(final MetaSchema schema) {
        return Linq4j.asEnumerable(schema.optiqSchema.getTables())
            .select(
                new Function1<Schema.TableInSchema, MetaTable>() {
                    public MetaTable apply(Schema.TableInSchema tableInSchema) {
                        return new MetaTable(
                            tableInSchema.getTable(Object.class),
                            schema.catalogName, schema.schemaName,
                            tableInSchema.name, tableInSchema.tableType.name());
                    }
                });
    }

    Enumerable<MetaTable> tableFunctions(
        final MetaSchema schema,
        final Predicate1<String> matcher)
    {
        final List<Pair<String, TableFunction>> list =
            new ArrayList<Pair<String, TableFunction>>();
        for (Map.Entry<String, List<TableFunction>> entry
            : schema.optiqSchema.getTableFunctions().entrySet())
        {
            if (matcher.apply(entry.getKey())) {
                for (TableFunction tableFunction : entry.getValue()) {
                    if (tableFunction.getParameters().isEmpty()) {
                        list.add(Pair.of(entry.getKey(), tableFunction));
                    }
                }
            }
        }
        return Linq4j
            .asEnumerable(list)
            .select(
                new Function1<Pair<String, TableFunction>, MetaTable>() {
                    public MetaTable apply(Pair<String, TableFunction> a0) {
                        final Table table =
                            a0.right.apply(Collections.emptyList());
                        return new MetaTable(
                            table,
                            schema.catalogName,
                            schema.schemaName,
                            a0.left,
                            Schema.TableType.VIEW.name());
                    }
                });
    }

    public Enumerable<MetaColumn> columns(final MetaTable table) {
        return Linq4j.asEnumerable(table.optiqTable.getRowType().getFieldList())
            .select(
                new Function1<RelDataTypeField, MetaColumn>() {
                    public MetaColumn apply(RelDataTypeField a0) {
                        final int precision =
                            a0.getType().getSqlTypeName().allowsPrec()
                            && !(a0.getType()
                                 instanceof RelDataTypeFactoryImpl.JavaType)
                                ? a0.getType().getPrecision()
                                : -1;
                        return new MetaColumn(
                            table.tableCat,
                            table.tableSchem,
                            table.tableName,
                            a0.getName(),
                            a0.getType().getSqlTypeName().getJdbcOrdinal(),
                            a0.getType().getFullTypeString(),
                            precision,
                            a0.getType().getSqlTypeName().allowsScale()
                                ? a0.getType().getScale()
                                : null,
                            10,
                            a0.getType().isNullable()
                                ? DatabaseMetaData.columnNullable
                                : DatabaseMetaData.columnNoNulls,
                            precision,
                            a0.getIndex() + 1,
                            a0.getType().isNullable() ? "YES" : "NO");
                    }
                }
            );
    }

    interface Named {
        String getName();
    }

    public static class MetaColumn implements Named {
        public final String tableCat;
        public final String tableSchem;
        public final String tableName;
        public final String columnName;
        public final int dataType;
        public final String typeName;
        public final int columnSize;
        public final String bufferLength = null;
        public final Integer decimalDigits;
        public final int numPrecRadix;
        public final int nullable;
        public final String remarks = null;
        public final String columnDef = null;
        public final String sqlDataType = null;
        public final String sqlDatetimeSub = null;
        public final int charOctetLength;
        public final int ordinalPosition;
        public final String isNullable;
        public final String scopeCatalog = null;
        public final String scopeTable = null;
        public final String sourceDataType = null;
        public final String isAutoincrement = null;
        public final String isGeneratedcolumn = null;

        MetaColumn(
            String tableCat,
            String tableSchem,
            String tableName,
            String columnName,
            int dataType,
            String typeName,
            int columnSize,
            Integer decimalDigits,
            int numPrecRadix,
            int nullable,
            int charOctetLength,
            int ordinalPosition,
            String isNullable)
        {
            this.tableCat = tableCat;
            this.tableSchem = tableSchem;
            this.tableName = tableName;
            this.columnName = columnName;
            this.dataType = dataType;
            this.typeName = typeName;
            this.columnSize = columnSize;
            this.decimalDigits = decimalDigits;
            this.numPrecRadix = numPrecRadix;
            this.nullable = nullable;
            this.charOctetLength = charOctetLength;
            this.ordinalPosition = ordinalPosition;
            this.isNullable = isNullable;
        }

        public String getName() {
            return columnName;
        }
    }

    public static class MetaTable implements Named {
        private final Table optiqTable;
        public final String tableCat;
        public final String tableSchem;
        public final String tableName;
        public final String tableType;
        public final String remarks = null;
        public final String typeCat = null;
        public final String typeSchem = null;
        public final String typeName = null;
        public final String selfReferencingColName = null;
        public final String refGeneration = null;

        public MetaTable(
            Table optiqTable,
            String tableCat,
            String tableSchem,
            String tableName,
            String tableType)
        {
            this.optiqTable = optiqTable;
            this.tableCat = tableCat;
            this.tableSchem = tableSchem;
            this.tableName = tableName;
            this.tableType = tableType;
        }

        public String getName() {
            return tableName;
        }
    }

    static class MetaSchema implements Named {
        private final Schema optiqSchema;
        public final String catalogName;
        public final String schemaName;

        public MetaSchema(
            Schema optiqSchema,
            String catalogName,
            String schemaName)
        {
            this.optiqSchema = optiqSchema;
            this.catalogName = catalogName;
            this.schemaName = schemaName;
        }

        public String getName() {
            return schemaName;
        }
    }

    private static class NamedFieldGetter
    {
        private final List<Field> fields = new ArrayList<Field>();
        private final List<ColumnMetaData> columnNames =
            new ArrayList<ColumnMetaData>();

        public NamedFieldGetter(Class clazz, String... names) {
            for (String name : names) {
                final int index = fields.size();
                final String fieldName = uncamel(name);
                final Field field;
                try {
                    field = clazz.getField(fieldName);
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }
                columnNames.add(
                    new ColumnMetaData(
                        index, false, true, false, false,
                        Primitive.is(field.getType())
                            ? DatabaseMetaData.columnNullable
                            : DatabaseMetaData.columnNoNulls,
                        true, -1, name, name, null,
                        0, 0, null, null, Types.VARCHAR, "VARCHAR", true,
                        false, false, null, field.getType()));
                fields.add(field);
            }
        }

        private String uncamel(String name) {
            StringBuilder buf = new StringBuilder();
            int nextUpper = -1;
            for (int i = 0; i < name.length(); i++) {
                char c = name.charAt(i);
                if (c == '_') {
                    nextUpper = i + 1;
                    continue;
                }
                if (nextUpper == i) {
                    c = Character.toUpperCase(c);
                } else {
                    c = Character.toLowerCase(c);
                }
                buf.append(c);
            }
            return buf.toString();
        }

        Object get(Object o, int columnIndex) {
            try {
                return fields.get(columnIndex).get(o);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        public Cursor cursor(final Enumerator<Object> enumerator) {
            return new AbstractCursor() {
                protected Getter createGetter(final int ordinal) {
                    return new Getter() {
                        public Object getObject() {
                            return get(enumerator.current(), ordinal);
                        }

                        public boolean wasNull() {
                            return getObject() == null;
                        }
                    };
                }

                public boolean next() {
                    return enumerator.moveNext();
                }
            };
        }
    }

    private static abstract class MetadataTable<E>
        extends AbstractQueryable<E>
        implements Table<E>
    {
        private final MapSchema schema;
        private final String tableName;
        private final Class<E> clazz;
        private final OptiqConnectionImpl connection;

        public MetadataTable(
            OptiqConnectionImpl connection, MapSchema schema, String tableName,
            Class<E> clazz)
        {
            super();
            this.schema = schema;
            this.tableName = tableName;
            this.clazz = clazz;
            this.connection = connection;
        }

        public DataContext getDataContext() {
            return schema;
        }

        public RelDataType getRowType() {
            return connection.typeFactory.createType(getElementType());
        }

        public Class<E> getElementType() {
            return clazz;
        }

        public Expression getExpression() {
            return Expressions.call(
                schema.getExpression(),
                "getTable",
                Expressions.<Expression>list()
                    .append(Expressions.constant(tableName))
                    .append(Expressions.constant(getElementType())));
        }

        public QueryProvider getProvider() {
            return connection;
        }

        public Statistic getStatistic() {
            return Statistics.UNKNOWN;
        }

        public Iterator<E> iterator() {
            return Linq4j.enumeratorIterator(enumerator());
        }
    }
}

// End Meta.java
