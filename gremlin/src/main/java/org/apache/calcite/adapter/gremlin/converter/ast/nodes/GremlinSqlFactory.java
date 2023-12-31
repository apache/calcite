package org.apache.calcite.adapter.gremlin.converter.ast.nodes;

import org.apache.calcite.sql.*;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlAsOperator;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlBasicCall;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlOperator;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlPostFixOperator;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.aggregate.GremlinSqlAggFunction;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.logic.GremlinSqlBinaryOperator;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.logic.GremlinSqlNumericLiteral;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.select.GremlinSqlSelect;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.select.GremlinSqlSelectMulti;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.select.GremlinSqlSelectSingle;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.select.join.GremlinSqlJoinComparison;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This factory converts different types of Calcite's SqlNode/SqlOperator's to SqlGremlin equivalents.
 */
public class GremlinSqlFactory {
    private static SqlMetadata sqlMetadata = null;

    public static void setSqlMetadata(final SqlMetadata sqlMetadata1) {
        sqlMetadata = sqlMetadata1;
    }

    public static SqlMetadata getGremlinSqlMetadata() throws SQLException {
        if (sqlMetadata == null) {
            throw new SQLException("Error: Schema must be set.");
        }
        return sqlMetadata;
    }

    public static GremlinSqlJoinComparison createJoinEquality(final SqlNode sqlNode)
            throws SQLException {
        if (sqlNode instanceof SqlBasicCall) {
            final SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
            if (sqlBasicCall.getOperator() instanceof SqlBinaryOperator) {
                return new GremlinSqlJoinComparison((SqlBasicCall) sqlNode,
                        (SqlBinaryOperator) sqlBasicCall.getOperator(), createNodeList(sqlBasicCall.getOperandList()),
                        getGremlinSqlMetadata());
            }
        }
        throw new SQLException(String.format("Error: Unknown node: %s.", sqlNode.getClass().getName()));
    }

    public static GremlinSqlOperator createOperator(final SqlOperator sqlOperator, final List<SqlNode> sqlOperands)
            throws SQLException {
        if (sqlOperator instanceof SqlAsOperator) {
            return new GremlinSqlAsOperator((SqlAsOperator) sqlOperator, createNodeList(sqlOperands),
                    getGremlinSqlMetadata());
        } else if (sqlOperator instanceof SqlAggFunction) {
            return new GremlinSqlAggFunction((SqlAggFunction) sqlOperator, createNodeList(sqlOperands),
                    getGremlinSqlMetadata());
        } else if (sqlOperator instanceof SqlBinaryOperator) {
            return new GremlinSqlBinaryOperator((SqlBinaryOperator) sqlOperator, createNodeList(sqlOperands),
                    getGremlinSqlMetadata());
        } else if (sqlOperator instanceof SqlPostfixOperator) {
            return new GremlinSqlPostFixOperator((SqlPostfixOperator) sqlOperator, createNodeList(sqlOperands),
                    getGremlinSqlMetadata());
        }
        throw new SQLException(String.format("Error: Unknown operator: %s.", sqlOperator.getKind().sql));
    }

    public static GremlinSqlNode createNode(final SqlNode sqlNode) throws SQLException {
        if (sqlNode instanceof SqlBasicCall) {
            return new GremlinSqlBasicCall((SqlBasicCall) sqlNode, getGremlinSqlMetadata());
        } else if (sqlNode instanceof SqlIdentifier) {
            return new GremlinSqlIdentifier((SqlIdentifier) sqlNode, getGremlinSqlMetadata());
        } else if (sqlNode instanceof SqlNumericLiteral) {
            return new GremlinSqlNumericLiteral((SqlNumericLiteral) sqlNode, getGremlinSqlMetadata());
        }
        throw new SQLException(String.format("Error: Unknown node: %s.", sqlNode.getClass().getName()));
    }

    public static List<GremlinSqlNode> createNodeList(final List<SqlNode> sqlNodes) throws SQLException {
        final List<GremlinSqlNode> gremlinSqlNodes = new ArrayList<>();
        for (final SqlNode sqlNode : sqlNodes) {
            gremlinSqlNodes.add(createNode(sqlNode));
        }
        return gremlinSqlNodes;
    }

    @SuppressWarnings("unchecked")
    public static <T> T createNodeCheckType(final SqlNode sqlNode, final Class<T> clazz) throws SQLException {
        final GremlinSqlNode gremlinSqlNode = createNode(sqlNode);
        if (!gremlinSqlNode.getClass().equals(clazz)) {
            throw new SQLException("Error: Type mismatch.");
        }
        return (T) gremlinSqlNode;
    }

    public static GremlinSqlSelect createSelect(final SqlSelect selectRoot, final GraphTraversalSource g)
            throws SQLException {
        if (selectRoot.getFrom() instanceof SqlJoin) {
            return new GremlinSqlSelectMulti(selectRoot, (SqlJoin) selectRoot.getFrom(), sqlMetadata, g);
        } else if (selectRoot.getFrom() instanceof SqlBasicCall) {
            return new GremlinSqlSelectSingle(selectRoot, (SqlBasicCall) selectRoot.getFrom(), sqlMetadata, g);
        }
        throw new SQLException(String.format("Error: Unknown node for getFrom: %s.", selectRoot.getFrom().getClass().getName()));
    }

    public static boolean isTable(final SqlNode sqlNode, final String renamedTable) throws SQLException {
        if (sqlNode instanceof SqlIdentifier) {
            return (((SqlIdentifier) sqlNode).names.get(0).equalsIgnoreCase(renamedTable));
        } else if (sqlNode instanceof SqlCall) {
            for (final SqlNode tmpSqlNode : ((SqlCall) sqlNode).getOperandList()) {
                if (isTable(tmpSqlNode, renamedTable)) {
                    return true;
                }
            }
        } else {
            throw new SQLException("Error: Unknown node type for isTable.");
        }
        return false;
    }
}
