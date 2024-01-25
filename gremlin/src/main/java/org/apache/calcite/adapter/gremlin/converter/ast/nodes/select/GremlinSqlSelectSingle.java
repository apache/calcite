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
package org.apache.calcite.adapter.gremlin.converter.ast.nodes.select;

import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;
import org.apache.calcite.adapter.gremlin.converter.SqlTraversalEngine;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlFactory;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlNode;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlAsOperator;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlBasicCall;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlOperator;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlPostFixOperator;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinTableBase;
import org.apache.calcite.adapter.gremlin.results.SqlGremlinQueryResult;
import org.apache.calcite.adapter.gremlin.results.pagination.Pagination;
import org.apache.calcite.adapter.gremlin.results.pagination.SimpleDataReader;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlSelect for a non-JOIN operation.
 */
public class GremlinSqlSelectSingle extends GremlinSqlSelect {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlSelectSingle.class);
    private final SqlSelect sqlSelect;
    private final SqlMetadata sqlMetadata;
    private final GraphTraversalSource g;
    private final SqlBasicCall sqlBasicCall;

    public GremlinSqlSelectSingle(final SqlSelect sqlSelect,
                                  final SqlBasicCall sqlBasicCall,
                                  final SqlMetadata sqlMetadata, final GraphTraversalSource g) {
        super(sqlSelect, sqlMetadata);
        this.sqlSelect = sqlSelect;
        this.sqlMetadata = sqlMetadata;
        this.g = g;
        this.sqlBasicCall = sqlBasicCall;
    }

    @Override protected void runTraversalExecutor(final GraphTraversal<?, ?> graphTraversal,
                                        final SqlGremlinQueryResult sqlGremlinQueryResult) throws SQLException {
        // Launch thread to continue grabbing results.
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("Data-Insert-Thread-%d").setDaemon(true).build());
        final List<List<String>> columns = new ArrayList<>(sqlMetadata.getColumnOutputListMap().values());
        if (columns.size() != 1) {
            throw new SQLException("Error: Single select has multi-table return.");
        }
        executor.execute(
            new Pagination(
                new SimpleDataReader(
                sqlMetadata.getRenameFromActual(sqlMetadata.getTables().iterator().next().getLabel()), columns.get(0)),
                graphTraversal, sqlGremlinQueryResult));
        executor.shutdown();
    }

    @Override public GraphTraversal<?, ?> generateTraversal() throws SQLException {
        if (sqlSelect.getSelectList() == null) {
            throw new SQLException("Error: GremlinSqlSelect expects select list component.");
        }

        final GremlinSqlOperator gremlinSqlOperator =
                GremlinSqlFactory.createOperator(sqlBasicCall.getOperator(), sqlBasicCall.getOperandList());
        if (!(gremlinSqlOperator instanceof GremlinSqlAsOperator)) {
            throw new SQLException("Unexpected format for FROM.");
        }
        final List<GremlinSqlNode> gremlinSqlOperands = GremlinSqlFactory.createNodeList(sqlBasicCall.getOperandList());
        final List<GremlinSqlIdentifier> gremlinSqlIdentifiers = new ArrayList<>();
        for (final GremlinSqlNode gremlinSqlOperand : gremlinSqlOperands) {
            if (!(gremlinSqlOperand instanceof GremlinSqlIdentifier)) {
                throw new SQLException("Unexpected format for FROM.");
            }
            gremlinSqlIdentifiers.add((GremlinSqlIdentifier) gremlinSqlOperand);
        }

        final GraphTraversal<?, ?> graphTraversal =
                SqlTraversalEngine.generateInitialSql(gremlinSqlIdentifiers, sqlMetadata, g);
        final String label = sqlMetadata.getActualTableName(gremlinSqlIdentifiers.get(0).getName(1));
        applyGroupBy(graphTraversal, label);
        applySelectValues(graphTraversal);
        applyOrderBy(graphTraversal, label);
        applyHaving(graphTraversal);
        applyWhere(graphTraversal);
        SqlTraversalEngine.applyAggregateFold(sqlMetadata, graphTraversal);
        SqlTraversalEngine.addProjection(gremlinSqlIdentifiers, sqlMetadata, graphTraversal);
        final String projectLabel = gremlinSqlIdentifiers.get(1).getName(0);
        applyColumnRetrieval(graphTraversal, projectLabel,
                GremlinSqlFactory.createNodeList(sqlSelect.getSelectList().getList()));

        if (sqlMetadata.getRenamedColumns() == null) {
            throw new SQLException("Error: Column rename list is empty.");
        }
        if (sqlMetadata.getTables().size() != 1) {
            throw new SQLException("Error: Expected one table for traversal execution.");
        }
        return graphTraversal;
    }

    public String getStringTraversal() throws SQLException {
        return GroovyTranslator.of("g").translate(generateTraversal().asAdmin().getBytecode());
    }

    private void applySelectValues(final GraphTraversal<?, ?> graphTraversal) {
        graphTraversal.select(Column.values);
    }

    protected void applyGroupBy(final GraphTraversal<?, ?> graphTraversal, final String table) throws SQLException {
        if ((sqlSelect.getGroup() == null) || (sqlSelect.getGroup().getList().isEmpty())) {
            // If we group bys but we have aggregates, we need to shove things into groups by ourselves.-
            graphTraversal.group().unfold();
        } else {
            final List<GremlinSqlNode> gremlinSqlNodes = new ArrayList<>();
            for (final SqlNode sqlNode : sqlSelect.getGroup().getList()) {
                gremlinSqlNodes.add(GremlinSqlFactory.createNodeCheckType(sqlNode, GremlinSqlIdentifier.class));
            }
            graphTraversal.group();
            final List<GraphTraversal> byUnion = new ArrayList<>();
            for (final GremlinSqlNode gremlinSqlNode : gremlinSqlNodes) {
                final GraphTraversal graphTraversal1 = __.__();
                toAppendToByGraphTraversal(gremlinSqlNode, table, graphTraversal1);
                byUnion.add(graphTraversal1);
            }
            graphTraversal.by(__.union(byUnion.toArray(new GraphTraversal[0])).fold()).unfold();
        }
    }

    protected void applyOrderBy(final GraphTraversal<?, ?> graphTraversal, final String table) throws SQLException {
        graphTraversal.order();
        if (sqlSelect.getOrderList() == null || sqlSelect.getOrderList().getList().isEmpty()) {
            graphTraversal.by(__.unfold().id());
            return;
        }
        final List<GremlinSqlNode> gremlinSqlIdentifiers = new ArrayList<>();
        for (final SqlNode sqlNode : sqlSelect.getOrderList().getList()) {
            gremlinSqlIdentifiers.add(GremlinSqlFactory.createNode(sqlNode));
        }
        for (final GremlinSqlNode gremlinSqlNode : gremlinSqlIdentifiers) {
            appendByGraphTraversal(gremlinSqlNode, table, graphTraversal);
        }
    }

    private void toAppendToByGraphTraversal(final GremlinSqlNode gremlinSqlNode, final String table,
                                            final GraphTraversal graphTraversal)
            throws SQLException {
        if (gremlinSqlNode instanceof GremlinSqlIdentifier) {
            final String column = sqlMetadata
                    .getActualColumnName(sqlMetadata.getGremlinTable(table),
                            ((GremlinSqlIdentifier) gremlinSqlNode).getColumn());
            if (column.endsWith(GremlinTableBase.IN_ID) || column.endsWith(GremlinTableBase.OUT_ID)) {
                // TODO: Grouping edges that are not the edge that the vertex are connected - needs to be implemented.
                throw new SQLException("Error, cannot group by edges.");
            } else {
                graphTraversal.values(sqlMetadata.getActualColumnName(sqlMetadata.getGremlinTable(table), column));
            }
        } else if (gremlinSqlNode instanceof GremlinSqlBasicCall) {
            final GremlinSqlBasicCall gremlinSqlBasicCall = (GremlinSqlBasicCall) gremlinSqlNode;
            gremlinSqlBasicCall.generateTraversal(graphTraversal);
        }
    }

    private void appendByGraphTraversal(final GremlinSqlNode gremlinSqlNode, final String table,
                                        final GraphTraversal graphTraversal)
            throws SQLException {
        final GraphTraversal graphTraversal1 = __.unfold();
        if (gremlinSqlNode instanceof GremlinSqlIdentifier) {
            final String column = sqlMetadata
                    .getActualColumnName(sqlMetadata.getGremlinTable(table),
                            ((GremlinSqlIdentifier) gremlinSqlNode).getColumn());
            if (column.endsWith(GremlinTableBase.IN_ID) || column.endsWith(GremlinTableBase.OUT_ID)) {
                // TODO: Grouping edges that are not the edge that the vertex are connected - needs to be implemented.
                throw new SQLException("Error, cannot group by edges.");
            } else {
                graphTraversal1.values(sqlMetadata.getActualColumnName(sqlMetadata.getGremlinTable(table), column));
            }
            graphTraversal.by(graphTraversal1);
        } else if (gremlinSqlNode instanceof GremlinSqlBasicCall) {
            final GremlinSqlBasicCall gremlinSqlBasicCall = (GremlinSqlBasicCall) gremlinSqlNode;
            gremlinSqlBasicCall.generateTraversal(graphTraversal1);
            if (gremlinSqlBasicCall.getGremlinSqlOperator() instanceof GremlinSqlPostFixOperator) {
                final GremlinSqlPostFixOperator gremlinSqlPostFixOperator =
                        (GremlinSqlPostFixOperator) gremlinSqlBasicCall.getGremlinSqlOperator();
                graphTraversal.by(graphTraversal1, gremlinSqlPostFixOperator.getOrder());
            } else {
                graphTraversal.by(graphTraversal1);
            }
        }
    }

    protected void applyHaving(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlSelect.getHaving() == null) {
            return;
        }
        final GremlinSqlBasicCall gremlinSqlBasicCall = GremlinSqlFactory.createNodeCheckType(sqlSelect.getHaving(),
                GremlinSqlBasicCall.class);
        gremlinSqlBasicCall.generateTraversal(graphTraversal);
    }

    protected void applyWhere(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlSelect.getWhere() == null) {
            return;
        }
        final GremlinSqlBasicCall gremlinSqlBasicCall = GremlinSqlFactory.createNodeCheckType(sqlSelect.getWhere(),
                GremlinSqlBasicCall.class);
        gremlinSqlBasicCall.generateTraversal(graphTraversal);
    }
}
