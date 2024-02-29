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
package org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator;

import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;
import org.apache.calcite.adapter.gremlin.converter.SqlTraversalEngine;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlNode;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.logic.GremlinSqlNumericLiteral;
import org.apache.calcite.sql.SqlAsOperator;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlAsOperator.
 */
public class GremlinSqlAsOperator extends GremlinSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlAsOperator.class);
    private final SqlAsOperator sqlAsOperator;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> sqlOperands;

    public GremlinSqlAsOperator(final SqlAsOperator sqlAsOperator, final List<GremlinSqlNode> gremlinSqlNodes,
                                final SqlMetadata sqlMetadata) {
        super(sqlAsOperator, gremlinSqlNodes, sqlMetadata);
        this.sqlAsOperator = sqlAsOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = gremlinSqlNodes;
    }

    @Override protected void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlOperands.get(0) instanceof GremlinSqlBasicCall) {
            ((GremlinSqlBasicCall) sqlOperands.get(0)).generateTraversal(graphTraversal);
        } else if (!(sqlOperands.get(0) instanceof GremlinSqlIdentifier) &&
                !(sqlOperands.get(0) instanceof GremlinSqlNumericLiteral)) {
            throw new SQLException(
                    "Error: expected operand to be GremlinSqlBasicCall or GremlinSqlIdentifier in GremlinSqlOperator.");
        }

        if (sqlOperands.size() == 1) {
            if (sqlOperands.get(0) instanceof GremlinSqlIdentifier) {
                SqlTraversalEngine
                        .applySqlIdentifier((GremlinSqlIdentifier) sqlOperands.get(0), sqlMetadata, graphTraversal);
            }
        }
        if (sqlOperands.size() == 2 && sqlOperands.get(0) instanceof GremlinSqlIdentifier) {
            SqlTraversalEngine
                    .applySqlIdentifier((GremlinSqlIdentifier) sqlOperands.get(0), sqlMetadata, graphTraversal);
        }
        sqlMetadata.addRenamedColumn(getActual(), getRename());
    }

    public String getName(final int operandIdx, final int nameIdx) throws SQLException {
        if (operandIdx >= sqlOperands.size() || !(sqlOperands.get(operandIdx) instanceof GremlinSqlIdentifier)) {
            throw new SQLException(
                    "Error: Expected operand idx less than number of operands and GremlinSqlIdentifier for operand");
        }
        return ((GremlinSqlIdentifier) sqlOperands.get(operandIdx)).getName(nameIdx);
    }

    public String getActual() throws SQLException {
        if (sqlOperands.size() != 2) {
            throw new SQLException("Error: Expected two operands for SQL AS statement.");
        }
        if (sqlOperands.get(0) instanceof GremlinSqlIdentifier) {
            return ((GremlinSqlIdentifier) sqlOperands.get(0)).getColumn();
        } else if (sqlOperands.get(0) instanceof GremlinSqlBasicCall) {
            return ((GremlinSqlBasicCall) sqlOperands.get(0)).getActual();
        }
        throw new SQLException("Error, unable to get actual name in GremlinSqlAsOperator.");
    }

    public String getRename() throws SQLException {
        if (sqlOperands.size() != 2) {
            throw new SQLException("Error: Expected two operands for SQL AS statement.");
        }
        if (sqlOperands.get(1) instanceof GremlinSqlIdentifier) {
            return ((GremlinSqlIdentifier) sqlOperands.get(1)).getColumn();
        } else if (sqlOperands.get(1) instanceof GremlinSqlBasicCall) {
            return ((GremlinSqlBasicCall) sqlOperands.get(1)).getRename();
        }
        throw new SQLException("Error, unable to get rename name in GremlinSqlAsOperator.");
    }
}
