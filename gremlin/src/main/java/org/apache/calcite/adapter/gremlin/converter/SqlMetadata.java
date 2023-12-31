package org.apache.calcite.adapter.gremlin.converter;

import org.apache.calcite.sql.*;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import org.apache.calcite.adapter.gremlin.converter.schema.calcite.GremlinSchema;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinEdgeTable;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinProperty;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinTableBase;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinVertexTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

import lombok.Getter;

/**
 * This module contains traversal and query metadata used by the adapter.
 */
@Getter
public class SqlMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlMetadata.class);
    private final GraphTraversalSource g;
    private final GremlinSchema gremlinSchema;
    private final Map<String, String> tableRenameMap = new HashMap<>();
    private final Map<String, String> columnRenameMap = new HashMap<>();
    private final Map<String, List<String>> columnOutputListMap = new HashMap<>();
    private boolean isAggregate = false;

    public SqlMetadata(final GraphTraversalSource g, final GremlinSchema gremlinSchema) {
        this.g = g;
        this.gremlinSchema = gremlinSchema;
    }

    private static boolean isAggregate(final SqlNode sqlNode) {
        if (sqlNode instanceof SqlCall) {
            final SqlCall sqlCall = (SqlCall) sqlNode;
            if (isAggregate(sqlCall.getOperator())) {
                return true;
            }
            for (final SqlNode tmpSqlNode : sqlCall.getOperandList()) {
                if (isAggregate(tmpSqlNode)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isAggregate(final SqlOperator sqlOperator) {
        return sqlOperator instanceof SqlAggFunction;
    }

    public boolean getIsColumnEdge(final String tableName, final String columnName) throws SQLException {
        return getGremlinTable(tableName).getIsVertex() &&
                (columnName.endsWith(GremlinTableBase.IN_ID) || columnName.endsWith(GremlinTableBase.OUT_ID));
    }

    public String getColumnEdgeLabel(final String column) throws SQLException {
        final String columnName = getRenamedColumn(column);
        final GremlinTableBase gremlinTableBase;
        if (columnName.endsWith(GremlinTableBase.IN_ID)) {
            gremlinTableBase = getGremlinTable(column.substring(0, column.length() - GremlinTableBase.IN_ID.length()));
        } else if (columnName.endsWith(GremlinTableBase.OUT_ID)) {
            gremlinTableBase = getGremlinTable(column.substring(0, column.length() - GremlinTableBase.OUT_ID.length()));
        } else {
            throw new SQLException(String.format("Error: Edge labels must end with %s or %s.", GremlinTableBase.IN_ID,
                    GremlinTableBase.OUT_ID));
        }

        if (gremlinTableBase.getIsVertex()) {
            throw new SQLException("Error: Expected edge table.");
        }
        return gremlinTableBase.getLabel();
    }

    public boolean isLeftInRightOut(final String leftVertexLabel, final String rightVertexLabel) {
        for (final GremlinVertexTable gremlinVertexTable : gremlinSchema.getVertices()) {
            if (gremlinVertexTable.hasInEdge(leftVertexLabel) && gremlinVertexTable.hasOutEdge(rightVertexLabel)) {
                return true;
            }
        }
        return false;
    }

    public boolean isRightInLeftOut(final String leftVertexLabel, final String rightVertexLabel) {
        for (final GremlinVertexTable gremlinVertexTable : gremlinSchema.getVertices()) {
            if (gremlinVertexTable.hasInEdge(rightVertexLabel) && gremlinVertexTable.hasOutEdge(leftVertexLabel)) {
                return true;
            }
        }
        return false;
    }

    public Set<String> getRenamedColumns() {
        return new HashSet<>(columnRenameMap.keySet());
    }

    public void setColumnOutputList(final String table, final List<String> columnOutputList) {
        columnOutputListMap.put(table, new ArrayList<>(columnOutputList));
    }

    public Set<GremlinTableBase> getTables() throws SQLException {
        final Set<GremlinTableBase> tables = new HashSet<>();
        for (final String table : tableRenameMap.values()) {
            tables.add(getGremlinTable(table));
        }
        return tables;
    }

    public boolean isVertex(final String table) throws SQLException {
        final String renamedTableName = getRenamedTable(table);
        for (final GremlinVertexTable gremlinVertexTable : gremlinSchema.getVertices()) {
            if (gremlinVertexTable.getLabel().equalsIgnoreCase(renamedTableName)) {
                return true;
            }
        }
        for (final GremlinEdgeTable gremlinEdgeTable : gremlinSchema.getEdges()) {
            if (gremlinEdgeTable.getLabel().equalsIgnoreCase(renamedTableName)) {
                return false;
            }
        }
        throw new SQLException("Error: Table {} does not exist.", renamedTableName);
    }

    public GremlinTableBase getGremlinTable(final String table) throws SQLException {
        final String renamedTableName = getRenamedTable(table);
        for (final GremlinTableBase gremlinTableBase : gremlinSchema.getAllTables()) {
            if (gremlinTableBase.getLabel().equalsIgnoreCase(renamedTableName)) {
                return gremlinTableBase;
            }
        }
        throw new SQLException(String.format("Error: Table %s does not exist.", renamedTableName));
    }

    public void addRenamedTable(final String actualName, final String renameName) {
        tableRenameMap.put(renameName, actualName);
    }

    public String getRenamedTable(final String table) {
        return tableRenameMap.getOrDefault(table, table);
    }

    public void addRenamedColumn(final String actualName, final String renameName) {
        columnRenameMap.put(renameName, actualName);
    }

    public String getRenamedColumn(final String column) {
        return columnRenameMap.getOrDefault(column, column);
    }

    public String getRenameFromActual(final String actual) {
        final Optional<Map.Entry<String, String>>
                rename = tableRenameMap.entrySet().stream().filter(t -> t.getValue().equals(actual)).findFirst();
        if (rename.isPresent()) {
            return rename.get().getKey();
        }
        return actual;
    }

    public String getActualColumnName(final GremlinTableBase table, final String column) throws SQLException {
        final String actualColumnName = getRenamedColumn(column);
        for (final GremlinProperty gremlinProperty : table.getColumns().values()) {
            if (gremlinProperty.getName().equalsIgnoreCase(actualColumnName)) {
                return gremlinProperty.getName();
            }
        }
        throw new SQLException(
                String.format("Error: Column %s does not exist in table %s.", actualColumnName, table.getLabel()));
    }

    public boolean getTableHasColumn(final GremlinTableBase table, final String column) {
        final String actualColumnName = getRenamedColumn(column);
        for (final GremlinProperty gremlinProperty : table.getColumns().values()) {
            if (gremlinProperty.getName().equalsIgnoreCase(actualColumnName)) {
                return true;
            }
        }
        return false;
    }

    public String getActualTableName(final String table) throws SQLException {
        final String renamedTableName = getRenamedTable(table);
        for (final GremlinVertexTable gremlinVertexTable : gremlinSchema.getVertices()) {
            if (gremlinVertexTable.getLabel().equalsIgnoreCase(renamedTableName)) {
                return gremlinVertexTable.getLabel();
            }
        }
        for (final GremlinEdgeTable gremlinEdgeTable : gremlinSchema.getEdges()) {
            if (gremlinEdgeTable.getLabel().equalsIgnoreCase(renamedTableName)) {
                return gremlinEdgeTable.getLabel();
            }
        }
        throw new SQLException(String.format("Error: Table %s.", table));
    }

    public void checkAggregate(final SqlNodeList sqlNodeList) {
        isAggregate = sqlNodeList.getList().stream().allMatch(SqlMetadata::isAggregate);
    }

    public boolean getIsAggregate() {
        return isAggregate;
    }

    public GremlinProperty getGremlinProperty(final String table, final String column) throws SQLException {
        final String actualColumnName = getActualColumnName(getGremlinTable(table), column);
        return getGremlinTable(table).getColumn(actualColumnName);
    }
}
