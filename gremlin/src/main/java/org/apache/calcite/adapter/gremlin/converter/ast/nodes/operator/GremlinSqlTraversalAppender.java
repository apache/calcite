package org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlNode;

import java.sql.SQLException;
import java.util.List;

/**
 * Interface for traversal appending function.
 */
public interface GremlinSqlTraversalAppender {
    void appendTraversal(GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) throws SQLException;
}
