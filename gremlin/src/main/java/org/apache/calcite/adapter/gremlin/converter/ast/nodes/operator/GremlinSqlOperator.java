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
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlNode;
import org.apache.calcite.sql.SqlOperator;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * This abstract class is a GremlinSql equivalent of Calcite's SqlOperator.
 */
public abstract class GremlinSqlOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlOperator.class);
    private final SqlOperator sqlOperator;
    private final List<GremlinSqlNode> sqlOperands;
    private final SqlMetadata sqlMetadata;

  public GremlinSqlOperator(SqlOperator sqlOperator, List<GremlinSqlNode> sqlOperands,
      SqlMetadata sqlMetadata) {
    this.sqlOperator = sqlOperator;
    this.sqlOperands = sqlOperands;
    this.sqlMetadata = sqlMetadata;
  }

  protected abstract void appendTraversal(GraphTraversal<?, ?> graphTraversal) throws SQLException;

    public void appendOperatorTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (sqlOperands.size() > 2) {
            throw new SQLException("Error: Expected 2 or less operands in operations.");
        } else if (sqlOperands.isEmpty()) {
            throw new SQLException("Error: Expected at least 1 operand in operations.");
        }

        appendTraversal(graphTraversal);
    }
}
