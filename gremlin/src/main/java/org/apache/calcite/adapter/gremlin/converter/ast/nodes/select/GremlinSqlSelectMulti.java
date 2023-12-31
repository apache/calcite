package org.apache.calcite.adapter.gremlin.converter.ast.nodes.select;

import org.apache.calcite.sql.*;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;
import org.apache.calcite.adapter.gremlin.converter.SqlTraversalEngine;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlFactory;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlNode;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlAsOperator;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlBasicCall;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.select.join.GremlinSqlJoinComparison;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinTableBase;
import org.apache.calcite.adapter.gremlin.results.SqlGremlinQueryResult;
import org.apache.calcite.adapter.gremlin.results.pagination.JoinDataReader;
import org.apache.calcite.adapter.gremlin.results.pagination.Pagination;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlSelect for a JOIN operation.
 */
public class GremlinSqlSelectMulti extends GremlinSqlSelect {
    // Multi is a JOIN.
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlSelectMulti.class);
    private final SqlSelect sqlSelect;
    private final SqlMetadata sqlMetadata;
    private final GraphTraversalSource g;
    private final SqlJoin sqlJoin;

    public GremlinSqlSelectMulti(final SqlSelect sqlSelect, final SqlJoin sqlJoin,
                                 final SqlMetadata sqlMetadata, final GraphTraversalSource g) {
        super(sqlSelect, sqlMetadata);
        this.sqlMetadata = sqlMetadata;
        this.sqlSelect = sqlSelect;
        this.g = g;
        this.sqlJoin = sqlJoin;
    }

    @Override
    protected void runTraversalExecutor(final GraphTraversal<?, ?> graphTraversal,
                                        final SqlGremlinQueryResult sqlGremlinQueryResult) throws SQLException {
        // Launch thread to continue grabbing results.
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("Data-Insert-Thread-%d").setDaemon(true).build());
        final Map<String, List<String>> tableColumns = sqlMetadata.getColumnOutputListMap();
        if (tableColumns.keySet().size() > 2) {
            throw new SQLException("Error: Join expects one or two tables only.");
        }
        executor.execute(new Pagination(new JoinDataReader(tableColumns), graphTraversal, sqlGremlinQueryResult));
        executor.shutdown();
    }

    @Override
    public GraphTraversal<?, ?> generateTraversal() throws SQLException {
        final JoinType joinType = sqlJoin.getJoinType();
        final JoinConditionType conditionType = sqlJoin.getConditionType();

        final GremlinSqlBasicCall left =
                GremlinSqlFactory.createNodeCheckType(sqlJoin.getLeft(), GremlinSqlBasicCall.class);
        final GremlinSqlBasicCall right =
                GremlinSqlFactory.createNodeCheckType(sqlJoin.getRight(), GremlinSqlBasicCall.class);
        final GremlinSqlJoinComparison gremlinSqlJoinComparison =
                GremlinSqlFactory.createJoinEquality(sqlJoin.getCondition());

        if (!joinType.name().equals(JoinType.INNER.name())) {
            throw new SQLException("Error, only INNER joins are supported.");
        }
        if (!conditionType.equals(JoinConditionType.ON)) {
            throw new SQLException("Error, only joins with ON conditions are supported.");
        }
        if ((left.getGremlinSqlNodes().size() != 2) || (right.getGremlinSqlNodes().size() != 2)) {
            throw new SQLException("Error: Expected 2 operands for left, right, and condition.");
        }
        if (!(left.getGremlinSqlOperator() instanceof GremlinSqlAsOperator) ||
                !(right.getGremlinSqlOperator() instanceof GremlinSqlAsOperator)) {
            throw new SQLException("Error: Expected left and right to have AS operators.");
        }
        final GremlinSqlAsOperator leftAsOperator = (GremlinSqlAsOperator) left.getGremlinSqlOperator();
        final String leftTableName = leftAsOperator.getActual();
        final String leftTableRename = leftAsOperator.getRename();
        sqlMetadata.addRenamedTable(leftTableName, leftTableRename);
        final String leftColumn = gremlinSqlJoinComparison.getColumn(leftTableRename);

        final GremlinSqlAsOperator rightAsOperator = (GremlinSqlAsOperator) right.getGremlinSqlOperator();
        final String rightTableName = rightAsOperator.getActual();
        final String rightTableRename = rightAsOperator.getRename();
        sqlMetadata.addRenamedTable(rightTableName, rightTableRename);
        final String rightColumn = gremlinSqlJoinComparison.getColumn(rightTableRename);

        if (!sqlMetadata.getIsColumnEdge(leftTableRename, leftColumn) ||
                !sqlMetadata.getIsColumnEdge(rightTableRename, rightColumn)) {
            throw new SQLException("Error: Joins can only be performed on vertices that are connected by an edge.");
        }

        if (rightColumn.endsWith(GremlinTableBase.IN_ID)) {
            if (!leftColumn.endsWith(GremlinTableBase.OUT_ID)) {
                throw new SQLException(
                        "Error: Joins can only be performed on vertices that are connected by a mutual edge.");
            }
        } else if (rightColumn.endsWith(GremlinTableBase.OUT_ID)) {
            if (!leftColumn.endsWith(GremlinTableBase.IN_ID)) {
                throw new SQLException(
                        "Error: Joins can only be performed on vertices that are connected by a mutual edge.");
            }
        } else {
            throw new SQLException(
                    "Error: Joins can only be performed on vertices that are connected by a mutual edge.");
        }

        final String edgeLabel = sqlMetadata.getColumnEdgeLabel(leftColumn);
        // Cases to consider:
        //  1. rightLabel == leftLabel
        //  2. rightLabel != leftLabel, rightLabel->leftLabel
        //  3. rightLabel != leftLabel, leftLabel->rightLabel
        //  4. rightLabel != leftLabel, rightLabel->leftLabel, leftLabel->rightLabel
        // Case 1 & 4 are logically equivalent.

        // Determine which is in and which is out.
        final boolean leftInRightOut = sqlMetadata.isLeftInRightOut(leftColumn, rightColumn);
        final boolean rightInLeftOut = sqlMetadata.isRightInLeftOut(leftColumn, rightColumn);

        final String inVLabel;
        final String outVLabel;
        final String inVRename;
        final String outVRename;
        if (leftInRightOut && rightInLeftOut &&
                (leftTableName.replace(GremlinTableBase.IN_ID, "").replace(GremlinTableBase.OUT_ID, "")
                        .equals(rightTableName.replace(GremlinTableBase.IN_ID, "")
                                .replace(GremlinTableBase.OUT_ID, "")))) {
            // Vertices of same label connected by an edge.
            // Doesn't matter how we assign these, but renames need to be different.
            inVLabel = leftTableName;
            outVLabel = leftTableName;
            inVRename = leftTableRename;
            outVRename = rightTableRename;
        } else if (leftInRightOut) {
            // Left vertex is in, right vertex is out
            inVLabel = leftTableName;
            outVLabel = rightTableName;
            inVRename = leftTableRename;
            outVRename = rightTableRename;
        } else if (rightInLeftOut) {
            // Right vertex is in, left vertex is out
            inVLabel = rightTableName;
            outVLabel = leftTableName;
            inVRename = rightTableRename;
            outVRename = leftTableRename;
        } else {
            inVLabel = "";
            outVLabel = "";
            inVRename = "";
            outVRename = "";
        }

        final List<GremlinSqlNode> gremlinSqlNodesIn = new ArrayList<>();
        final List<GremlinSqlNode> gremlinSqlNodesOut = new ArrayList<>();
        for (final SqlNode sqlNode : sqlSelect.getSelectList().getList()) {
            if (GremlinSqlFactory.isTable(sqlNode, inVRename)) {
                gremlinSqlNodesIn.add(GremlinSqlFactory.createNode(sqlNode));
            } else if (GremlinSqlFactory.isTable(sqlNode, outVRename)) {
                gremlinSqlNodesOut.add(GremlinSqlFactory.createNode(sqlNode));
            }
        }

        final GraphTraversal<?, ?> graphTraversal = g.E().hasLabel(edgeLabel)
                .where(__.inV().hasLabel(inVLabel))
                .where(__.outV().hasLabel(outVLabel));
        applyGroupBy(graphTraversal, edgeLabel, inVRename, outVRename);
        applySelectValues(graphTraversal);
        applyOrderBy(graphTraversal, edgeLabel, inVRename, outVRename);
        applyHaving(graphTraversal);
        SqlTraversalEngine.applyAggregateFold(sqlMetadata, graphTraversal);
        graphTraversal.project(inVRename, outVRename);
        applyColumnRetrieval(graphTraversal, inVRename, gremlinSqlNodesIn, StepDirection.In);
        applyColumnRetrieval(graphTraversal, outVRename, gremlinSqlNodesOut, StepDirection.Out);
        return graphTraversal;
    }

    private void applySelectValues(final GraphTraversal<?, ?> graphTraversal) {
        graphTraversal.select(Column.values);
    }

    // TODO: Fill in group by and place in correct position of traversal.
    protected void applyGroupBy(final GraphTraversal<?, ?> graphTraversal, final String edgeLabel,
                                final String inVRename, final String outVRename) throws SQLException {
        if ((sqlSelect.getGroup() == null) || (sqlSelect.getGroup().getList().isEmpty())) {
            // If we group bys but we have aggregates, we need to shove things into groups by ourselves.-
            graphTraversal.group().unfold();
        } else {
            final List<GremlinSqlIdentifier> gremlinSqlIdentifiers = new ArrayList<>();
            for (final SqlNode sqlNode : sqlSelect.getGroup().getList()) {
                gremlinSqlIdentifiers.add(GremlinSqlFactory.createNodeCheckType(sqlNode, GremlinSqlIdentifier.class));
            }
            graphTraversal.group();
            final List<GraphTraversal> byUnion = new ArrayList<>();
            for (final GremlinSqlIdentifier gremlinSqlIdentifier : gremlinSqlIdentifiers) {
                final String table = sqlMetadata.getRenamedTable(gremlinSqlIdentifier.getName(0));
                final String column = sqlMetadata
                        .getActualColumnName(sqlMetadata.getGremlinTable(table), gremlinSqlIdentifier.getName(1));
                if (column.replace(GremlinTableBase.ID, "").equalsIgnoreCase(edgeLabel)) {
                    byUnion.add(__.id());
                } else if (column.endsWith(GremlinTableBase.ID)) {
                    // TODO: Grouping edges that are not the edge that the vertex are connected - needs to be implemented.
                    throw new SQLException("Error, cannot group by edges.");
                } else {
                    if (inVRename.equals(table)) {
                        byUnion.add(__.inV().hasLabel(table)
                                .values(sqlMetadata.getActualColumnName(sqlMetadata.getGremlinTable(table), column)));
                    } else if (outVRename.equals(table)) {
                        byUnion.add(__.outV().hasLabel(table)
                                .values(sqlMetadata.getActualColumnName(sqlMetadata.getGremlinTable(table), column)));
                    } else {
                        throw new SQLException(String.format("Error, unable to group table %s.", table));
                    }
                }
            }
            graphTraversal.by(__.union(byUnion.toArray(new GraphTraversal[0])).fold()).unfold();
        }
    }


    protected void applyOrderBy(final GraphTraversal<?, ?> graphTraversal, final String edgeLabel,
                                final String inVRename, final String outVRename) throws SQLException {
        graphTraversal.order();
        if (sqlSelect.getOrderList() == null || sqlSelect.getOrderList().getList().isEmpty()) {
            graphTraversal.by(__.unfold().id());
            return;
        }
        final List<GremlinSqlIdentifier> gremlinSqlIdentifiers = new ArrayList<>();
        for (final SqlNode sqlNode : sqlSelect.getOrderList().getList()) {
            gremlinSqlIdentifiers.add(GremlinSqlFactory.createNodeCheckType(sqlNode, GremlinSqlIdentifier.class));
        }
        final GremlinTableBase outVTable = sqlMetadata.getGremlinTable(outVRename);
        final GremlinTableBase inVTable = sqlMetadata.getGremlinTable(inVRename);
        for (final GremlinSqlIdentifier gremlinSqlIdentifier : gremlinSqlIdentifiers) {
            final String column = gremlinSqlIdentifier.getColumn();
            if (column.endsWith(GremlinTableBase.IN_ID) || column.endsWith(GremlinTableBase.OUT_ID)) {
                // TODO: Grouping edges that are not the edge that the vertex are connected - needs to be implemented.
                throw new SQLException("Error, cannot group by edges.");
            } else {
                if (sqlMetadata.getTableHasColumn(inVTable, column)) {
                    graphTraversal.by(__.unfold().inV().hasLabel(inVTable.getLabel())
                            .values(sqlMetadata.getActualColumnName(inVTable, column)));
                } else if (sqlMetadata.getTableHasColumn(outVTable, column)) {
                    graphTraversal.by(__.unfold().outV().hasLabel(outVTable.getLabel())
                            .values(sqlMetadata.getActualColumnName(outVTable, column)));
                } else {
                    throw new SQLException(String.format("Error, unable to group column %s.", column));
                }
            }
        }
    }

    protected void applyHaving(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlSelect.getHaving() == null) {
            return;
        }
        throw new SQLException("Error: HAVING is not currently supported for JOIN.");
    }
}
