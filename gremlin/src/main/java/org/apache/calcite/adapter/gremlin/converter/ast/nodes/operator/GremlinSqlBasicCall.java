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
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlFactory;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlNode;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.aggregate.GremlinSqlAggFunction;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.logic.GremlinSqlNumericLiteral;
import org.apache.calcite.sql.SqlBasicCall;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlBasicCall.
 */
public class GremlinSqlBasicCall extends GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlBasicCall.class);
    private final SqlBasicCall sqlBasicCall;
    private final GremlinSqlOperator gremlinSqlOperator;
    private final List<GremlinSqlNode> gremlinSqlNodes;

    public GremlinSqlBasicCall(final SqlBasicCall sqlBasicCall, final SqlMetadata sqlMetadata)
            throws SQLException {
        super(sqlBasicCall, sqlMetadata);
        this.sqlBasicCall = sqlBasicCall;
        gremlinSqlOperator =
                GremlinSqlFactory.createOperator(sqlBasicCall.getOperator(), sqlBasicCall.getOperandList());
        gremlinSqlNodes = GremlinSqlFactory.createNodeList(sqlBasicCall.getOperandList());
    }

  public SqlBasicCall getSqlBasicCall() {
    return sqlBasicCall;
  }

  public GremlinSqlOperator getGremlinSqlOperator() {
    return gremlinSqlOperator;
  }

  public List<GremlinSqlNode> getGremlinSqlNodes() {
    return gremlinSqlNodes;
  }

  void validate() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            if (gremlinSqlNodes.size() != 2) {
                throw new SQLException("Error, expected only two sub nodes for GremlinSqlBasicCall.");
            }
        } else if (gremlinSqlOperator instanceof GremlinSqlAggFunction) {
            if (gremlinSqlNodes.size() != 1) {
                throw new SQLException("Error, expected only one sub node for GremlinSqlAggFunction.");
            }
        }
    }

    public void generateTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        validate();
        gremlinSqlOperator.appendOperatorTraversal(graphTraversal);
    }

    public String getRename() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            return ((GremlinSqlAsOperator) gremlinSqlOperator).getRename();
        } else if (gremlinSqlOperator instanceof GremlinSqlAggFunction) {
            if (gremlinSqlNodes.size() == 1 && gremlinSqlNodes.get(0) instanceof GremlinSqlIdentifier) {
                // returns the formatted column name for aggregations
                return ((GremlinSqlAggFunction) gremlinSqlOperator).getNewName();
            }
        }
        throw new SQLException("Unable to determine column rename.");
    }

    public String getActual() throws SQLException {
        if (gremlinSqlOperator instanceof GremlinSqlAsOperator) {
            return ((GremlinSqlAsOperator) gremlinSqlOperator).getActual();
        } else if (gremlinSqlOperator instanceof GremlinSqlAggFunction) {
            if (gremlinSqlNodes.size() == 1 && gremlinSqlNodes.get(0) instanceof GremlinSqlIdentifier) {
                return ((GremlinSqlIdentifier) gremlinSqlNodes.get(0)).getColumn();
            } else if (gremlinSqlNodes.size() == 2 && gremlinSqlNodes.get(1) instanceof GremlinSqlIdentifier) {
                return ((GremlinSqlIdentifier) gremlinSqlNodes.get(1)).getColumn();
            } else if (gremlinSqlNodes.size() == 1 && gremlinSqlNodes.get(0) instanceof GremlinSqlNumericLiteral) {
                return ((GremlinSqlAggFunction) gremlinSqlOperator).getNewName();
            }
        }
        throw new SQLException("Unable to determine actual column name.");
    }
}
