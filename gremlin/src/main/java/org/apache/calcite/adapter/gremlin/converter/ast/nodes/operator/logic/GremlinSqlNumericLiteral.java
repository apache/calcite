package org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.logic;

import org.apache.calcite.sql.SqlNumericLiteral;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlNode;

import java.sql.SQLException;

/**
 * This module is a GremlinSql equivalent of Calcite's GremlinSqlNumericLiteral.
 */
public class GremlinSqlNumericLiteral extends GremlinSqlNode {
    private final SqlNumericLiteral sqlNumericLiteral;

    public GremlinSqlNumericLiteral(final SqlNumericLiteral sqlNumericLiteral,
                                    final SqlMetadata sqlMetadata) {
        super(sqlNumericLiteral, sqlMetadata);
        this.sqlNumericLiteral = sqlNumericLiteral;
    }

    public void appendTraversal(final GraphTraversal graphTraversal) throws SQLException {
        graphTraversal.constant(getValue());
    }

    public Object getValue() throws SQLException {
        return sqlNumericLiteral.getValue();
    }
}
