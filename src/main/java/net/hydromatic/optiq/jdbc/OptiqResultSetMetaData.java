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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of {@link ResultSetMetaData}
 * for the Optiq engine.
 */
class OptiqResultSetMetaData implements ResultSetMetaData {
    final OptiqStatement statement;
    final Object query;
    final List<ColumnMetaData> columnMetaDataList;

    OptiqResultSetMetaData(
        OptiqStatement statement,
        Object query,
        List<ColumnMetaData> columnMetaDataList)
    {
        this.statement = statement;
        this.query = query;
        this.columnMetaDataList = columnMetaDataList;
    }

    // implement ResultSetMetaData

    public int getColumnCount() throws SQLException {
        return columnMetaDataList.size();
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        return getColumnMetaData(column).autoIncrement;
    }

    private ColumnMetaData getColumnMetaData(int column) {
        return columnMetaDataList.get(column - 1);
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        return getColumnMetaData(column).caseSensitive;
    }

    public boolean isSearchable(int column) throws SQLException {
        return getColumnMetaData(column).searchable;
    }

    public boolean isCurrency(int column) throws SQLException {
        return getColumnMetaData(column).currency;
    }

    public int isNullable(int column) throws SQLException {
        return getColumnMetaData(column).nullable;
    }

    public boolean isSigned(int column) throws SQLException {
        return getColumnMetaData(column).signed;
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        return getColumnMetaData(column).displaySize;
    }

    public String getColumnLabel(int column) throws SQLException {
        return getColumnMetaData(column).label;
    }

    public String getColumnName(int column) throws SQLException {
        return getColumnMetaData(column).columnName;
    }

    public String getSchemaName(int column) throws SQLException {
        return getColumnMetaData(column).schemaName;
    }

    public int getPrecision(int column) throws SQLException {
        return getColumnMetaData(column).precision;
    }

    public int getScale(int column) throws SQLException {
        return getColumnMetaData(column).scale;
    }

    public String getTableName(int column) throws SQLException {
        return getColumnMetaData(column).tableName;
    }

    public String getCatalogName(int column) throws SQLException {
        return getColumnMetaData(column).catalogName;
    }

    public int getColumnType(int column) throws SQLException {
        return getColumnMetaData(column).type;
    }

    public String getColumnTypeName(int column) throws SQLException {
        return getColumnMetaData(column).typeName;
    }

    public boolean isReadOnly(int column) throws SQLException {
        return getColumnMetaData(column).readOnly;
    }

    public boolean isWritable(int column) throws SQLException {
        return getColumnMetaData(column).writable;
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        return getColumnMetaData(column).definitelyWritable;
    }

    public String getColumnClassName(int column) throws SQLException {
        return getColumnMetaData(column).columnClassName;
    }

    // implement Wrapper

    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw statement.connection.helper.createException(
            "does not implement '" + iface + "'");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    static class ColumnMetaData {
        final int ordinal; // 0-based
        final boolean autoIncrement;
        final boolean caseSensitive;
        final boolean searchable;
        final boolean currency;
        final int nullable;
        final boolean signed;
        final int displaySize;
        final String label;
        final String columnName;
        final String schemaName;
        final int precision;
        final int scale;
        final String tableName;
        final String catalogName;
        final int type;
        final String typeName;
        final boolean readOnly;
        final boolean writable;
        final boolean definitelyWritable;
        final String columnClassName;

        public ColumnMetaData(
            int ordinal,
            boolean autoIncrement,
            boolean caseSensitive,
            boolean searchable,
            boolean currency,
            int nullable,
            boolean signed,
            int displaySize,
            String label,
            String columnName,
            String schemaName,
            int precision,
            int scale,
            String tableName,
            String catalogName,
            int type,
            String typeName,
            boolean readOnly,
            boolean writable,
            boolean definitelyWritable,
            String columnClassName)
        {
            this.ordinal = ordinal;
            this.autoIncrement = autoIncrement;
            this.caseSensitive = caseSensitive;
            this.searchable = searchable;
            this.currency = currency;
            this.nullable = nullable;
            this.signed = signed;
            this.displaySize = displaySize;
            this.label = label;
            this.columnName = columnName;
            this.schemaName = schemaName;
            this.precision = precision;
            this.scale = scale;
            this.tableName = tableName;
            this.catalogName = catalogName;
            this.type = type;
            this.typeName = typeName;
            this.readOnly = readOnly;
            this.writable = writable;
            this.definitelyWritable = definitelyWritable;
            this.columnClassName = columnClassName;
        }
    }
}

// End OptiqResultSetMetaData.java
