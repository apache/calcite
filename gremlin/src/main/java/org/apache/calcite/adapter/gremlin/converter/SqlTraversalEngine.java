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
package org.apache.calcite.adapter.gremlin.converter;

import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.select.StepDirection;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinTableBase;
import org.apache.calcite.adapter.gremlin.results.SqlGremlinQueryResult;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinTableBase.IN_ID;
import static org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinTableBase.OUT_ID;

/**
 * Traversal engine for SQL-Gremlin. This module is responsible for generating the gremlin traversals.
 */
public class SqlTraversalEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTraversalEngine.class);

    public static GraphTraversal<?, ?> generateInitialSql(final List<GremlinSqlIdentifier> gremlinSqlIdentifiers,
                                                          final SqlMetadata sqlMetadata,
                                                          final GraphTraversalSource g) throws SQLException {
        if (gremlinSqlIdentifiers.size() != 2) {
            throw new SQLException("Expected GremlinSqlIdentifier List size to be 2.");
        }
        final String label = sqlMetadata.getActualTableName(gremlinSqlIdentifiers.get(0).getName(1));
        final GraphTraversal<?, ?> graphTraversal = sqlMetadata.isVertex(label) ? g.V() : g.E();
        graphTraversal.hasLabel(label);
        return graphTraversal;
    }

    public static void applyAggregateFold(final SqlMetadata sqlMetadata, final GraphTraversal<?, ?> graphTraversal) {
        if (sqlMetadata.getIsAggregate()) {
            graphTraversal.fold();
        }
    }

    public static GraphTraversal<?, ?> getEmptyTraversal(final StepDirection direction, final SqlMetadata sqlMetadata) {
        final GraphTraversal<?, ?> graphTraversal = __.unfold();
        if (sqlMetadata.getIsAggregate()) {
            graphTraversal.unfold();
        }
        switch (direction) {
            case Out:
                return graphTraversal.outV();
            case In:
                return graphTraversal.inV();
        }
        return graphTraversal;
    }

    public static void addProjection(final List<GremlinSqlIdentifier> gremlinSqlIdentifiers,
                                     final SqlMetadata sqlMetadata,
                                     final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (gremlinSqlIdentifiers.size() != 2) {
            throw new SQLException("Expected GremlinSqlIdentifier List size to be 2.");
        }
        final String label = sqlMetadata.getActualTableName(gremlinSqlIdentifiers.get(0).getName(1));
        final String projectLabel = gremlinSqlIdentifiers.get(1).getName(0);

        graphTraversal.project(projectLabel);
        sqlMetadata.addRenamedTable(label, projectLabel);
    }

    public static GraphTraversal<?, ?> getEmptyTraversal(final SqlMetadata sqlMetadata) {
        return getEmptyTraversal(StepDirection.None, sqlMetadata);
    }


    public static void applyTraversal(final GraphTraversal<?, ?> graphTraversal,
                                      final GraphTraversal<?, ?> subGraphTraversal) {
        graphTraversal.by(subGraphTraversal);
    }

    public static void applySqlIdentifier(final GremlinSqlIdentifier sqlIdentifier,
                                          final SqlMetadata sqlMetadata,
                                          final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        // Format of identifier is 'table'.'column => ['table', 'column']
        appendGraphTraversal(sqlIdentifier.getName(0), sqlIdentifier.getName(1), sqlMetadata, graphTraversal);
    }

    public static GraphTraversal<?, ?> applyColumnRenames(final List<String> columnsRenamed) throws SQLException {
        final String firstColumn = columnsRenamed.remove(0);
        final String[] remaining = columnsRenamed.toArray(new String[] {});
        return __.project(firstColumn, remaining);
    }

    private static void appendGraphTraversal(final String table, final String column,
                                             final SqlMetadata sqlMetadata,
                                             final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        final GremlinTableBase gremlinTableBase = sqlMetadata.getGremlinTable(table);
        final String columnName = sqlMetadata.getActualColumnName(gremlinTableBase, column);

        // Primary/foreign key, need to traverse appropriately.
        if (!columnName.endsWith(GremlinTableBase.ID)) {
            if (sqlMetadata.getIsAggregate()) {
                graphTraversal.values(columnName);
            } else {
                graphTraversal.choose(__.has(columnName), __.values(columnName),
                        __.constant(SqlGremlinQueryResult.NULL_VALUE));
            }
        } else {
            // It's this vertex/edge.
            if (columnName.toLowerCase().startsWith(gremlinTableBase.getLabel())) {
                graphTraversal.id();
            } else {
                if (columnName.endsWith(IN_ID)) {
                    // Vertices can have many connected, edges (thus we need to fold). Edges can only connect to 1 vertex.
                    if (gremlinTableBase.getIsVertex()) {
                        graphTraversal.coalesce(__.inE().hasLabel(columnName.replace(IN_ID, "")).id().fold(),
                                __.constant(new ArrayList<>()));
                    } else {
                        graphTraversal.coalesce(__.inV().hasLabel(columnName.replace(IN_ID, "")).id(),
                                __.constant(new ArrayList<>()));
                    }
                } else if (column.endsWith(OUT_ID)) {
                    // Vertices can have many connected, edges (thus we need to fold). Edges can only connect to 1 vertex.
                    if (gremlinTableBase.getIsVertex()) {
                        graphTraversal.coalesce(__.outE().hasLabel(columnName.replace(OUT_ID, "")).id().fold(),
                                __.constant(new ArrayList<>()));
                    } else {
                        graphTraversal.coalesce(__.outV().hasLabel(columnName.replace(IN_ID, "")).id(),
                                __.constant(new ArrayList<>()));
                    }
                } else {
                    graphTraversal.constant(new ArrayList<>());
                }
            }
        }
    }
}
