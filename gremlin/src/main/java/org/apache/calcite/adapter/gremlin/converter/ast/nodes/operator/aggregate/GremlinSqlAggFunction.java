package org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.aggregate;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;
import org.apache.calcite.adapter.gremlin.converter.SqlTraversalEngine;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlNode;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlBasicCall;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlOperator;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlTraversalAppender;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.logic.GremlinSqlNumericLiteral;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlAggFunction.
 */
public class GremlinSqlAggFunction extends GremlinSqlOperator {
    // See SqlKind.AGGREGATE for list of aggregate functions in Calcite.
    private static final Map<SqlKind, GremlinSqlTraversalAppender> AGGREGATE_APPENDERS =
            new HashMap<SqlKind, GremlinSqlTraversalAppender>() {{
                put(SqlKind.AVG, GremlinSqlAggFunctionImplementations.AVG);
                put(SqlKind.COUNT, GremlinSqlAggFunctionImplementations.COUNT);
                put(SqlKind.SUM, GremlinSqlAggFunctionImplementations.SUM);
                put(SqlKind.MIN, GremlinSqlAggFunctionImplementations.MIN);
                put(SqlKind.MAX, GremlinSqlAggFunctionImplementations.MAX);
            }};
    private final SqlAggFunction sqlAggFunction;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> sqlOperands;


    public GremlinSqlAggFunction(final SqlAggFunction sqlOperator,
                                 final List<GremlinSqlNode> gremlinSqlNodes,
                                 final SqlMetadata sqlMetadata) {
        super(sqlOperator, gremlinSqlNodes, sqlMetadata);
        this.sqlAggFunction = sqlOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = gremlinSqlNodes;
    }

    @Override
    protected void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
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
        if (AGGREGATE_APPENDERS.containsKey(sqlAggFunction.kind)) {
            AGGREGATE_APPENDERS.get(sqlAggFunction.kind).appendTraversal(graphTraversal, sqlOperands);
        } else {
            throw new SQLException(
                    String.format("Error: Aggregate function %s is not supported.", sqlAggFunction.kind.sql));
        }
    }

    /**
     * Aggregation columns will be named in the form of AGG(xxx) if no rename is specified in SQL
     */
    public String getNewName() throws SQLException {
        if (sqlOperands.size() == 1 && sqlOperands.get(0) instanceof GremlinSqlIdentifier) {
            return String.format("%s(%s)", sqlAggFunction.kind.name(),
                    ((GremlinSqlIdentifier) sqlOperands.get(0)).getColumn());
        } else if (sqlOperands.size() == 2 && sqlOperands.get(1) instanceof GremlinSqlIdentifier) {
            return String.format("%s(%s)", sqlAggFunction.kind.name(),
                    ((GremlinSqlIdentifier) sqlOperands.get(1)).getColumn());
        } else if (sqlOperands.size() == 1 && sqlOperands.get(0) instanceof GremlinSqlNumericLiteral) {
            return String.format("%s(%s)", sqlAggFunction.kind.name(),
                    ((GremlinSqlNumericLiteral) sqlOperands.get(0)).getValue().toString());
        }
        throw new SQLException("Error, unable to get rename name in GremlinSqlAggOperator.");
    }

    private static class GremlinSqlAggFunctionImplementations {
        public static final GremlinSqlTraversalAppender AVG =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                    graphTraversal.mean();
                };
        public static final GremlinSqlTraversalAppender COUNT =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                    graphTraversal.count();
                };
        public static final GremlinSqlTraversalAppender SUM =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                    graphTraversal.sum();
                };
        public static final GremlinSqlTraversalAppender MIN =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                    graphTraversal.min();
                };
        public static final GremlinSqlTraversalAppender MAX =
                (GraphTraversal<?, ?> graphTraversal, List<GremlinSqlNode> operands) -> {
                    graphTraversal.max();
                };
    }
}
