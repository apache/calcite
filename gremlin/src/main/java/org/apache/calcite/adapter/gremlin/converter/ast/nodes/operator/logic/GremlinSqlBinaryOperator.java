package org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.logic;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlNode;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operands.GremlinSqlIdentifier;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlBasicCall;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlOperator;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operator.GremlinSqlTraversalAppender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * This module is a GremlinSql equivalent of Calcite's GremlinSqlBinaryOperator.
 */
public class GremlinSqlBinaryOperator extends GremlinSqlOperator {
    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlBinaryOperator.class);
    private static final Map<SqlKind, GremlinSqlTraversalAppender> BINARY_APPENDERS =
            new HashMap<SqlKind, GremlinSqlTraversalAppender>() {
                {
                    put(SqlKind.EQUALS, new GremlinSqlBinaryOperatorAppender.GremlinSqlBinaryOperatorAppenderEquals());
                    put(SqlKind.NOT_EQUALS,
                            new GremlinSqlBinaryOperatorAppender.GremlinSqlBinaryOperatorAppenderNotEquals());
                    put(SqlKind.GREATER_THAN,
                            new GremlinSqlBinaryOperatorAppender.GremlinSqlBinaryOperatorAppenderGreater());
                    put(SqlKind.GREATER_THAN_OR_EQUAL,
                            new GremlinSqlBinaryOperatorAppender.GremlinSqlBinaryOperatorAppenderGreaterEquals());
                    put(SqlKind.LESS_THAN, new GremlinSqlBinaryOperatorAppender.GremlinSqlBinaryOperatorAppenderLess());
                    put(SqlKind.LESS_THAN_OR_EQUAL,
                            new GremlinSqlBinaryOperatorAppender.GremlinSqlBinaryOperatorAppenderLessEquals());
                    put(SqlKind.AND, new GremlinSqlBinaryOperatorAppender.GremlinSqlBinaryOperatorAppenderAnd());
                    put(SqlKind.OR, new GremlinSqlBinaryOperatorAppender.GremlinSqlBinaryOperatorAppenderOr());
                }
            };
    private final SqlBinaryOperator sqlBinaryOperator;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> sqlOperands;

    public GremlinSqlBinaryOperator(final SqlBinaryOperator sqlBinaryOperator,
                                    final List<GremlinSqlNode> sqlOperands,
                                    final SqlMetadata sqlMetadata) {
        super(sqlBinaryOperator, sqlOperands, sqlMetadata);
        this.sqlBinaryOperator = sqlBinaryOperator;
        this.sqlMetadata = sqlMetadata;
        this.sqlOperands = sqlOperands;
    }

    private static String getRandomString() {
        final StringBuilder salt = new StringBuilder();
        final Random rnd = new Random();
        while (salt.length() < 10) { // length of the random string.
            final int index = (int) (rnd.nextFloat() * CHARS.length());
            salt.append(CHARS.charAt(index));
        }
        return salt.toString();
    }

    @Override
    protected void appendTraversal(final GraphTraversal<?, ?> graphTraversal) throws SQLException {
        if (BINARY_APPENDERS.containsKey(sqlBinaryOperator.kind)) {
            BINARY_APPENDERS.get(sqlBinaryOperator.kind).appendTraversal(graphTraversal, sqlOperands);
        } else {
            throw new SQLException(
                    String.format("Error: Aggregate function %s is not supported.", sqlBinaryOperator.kind.sql));
        }
    }

    private static class GremlinSqlBinaryOperatorAppender {
        private static GraphTraversal[] getTraversalsFromOperands(final List<GremlinSqlNode> operands)
                throws SQLException {
            if (operands.size() != 2) {
                throw new SQLException("Error: Binary operator without 2 operands received.");
            }
            final GraphTraversal[] graphTraversals = new GraphTraversal[2];
            for (int i = 0; i < operands.size(); i++) {
                graphTraversals[i] = __.unfold();
                if (operands.get(i) instanceof GremlinSqlIdentifier) {
                    ((GraphTraversal<?, ?>) graphTraversals[i])
                            .values(((GremlinSqlIdentifier) operands.get(i)).getColumn());
                } else if (operands.get(i) instanceof GremlinSqlBasicCall) {
                    ((GremlinSqlBasicCall) operands.get(i))
                            .generateTraversal((GraphTraversal<?, ?>) graphTraversals[i]);
                } else if (operands.get(i) instanceof GremlinSqlNumericLiteral) {
                    ((GremlinSqlNumericLiteral) operands.get(i)).appendTraversal(graphTraversals[i]);
                }
            }
            return graphTraversals;
        }

        public static class GremlinSqlBinaryOperatorAppenderEquals implements GremlinSqlTraversalAppender {
            public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                    throws SQLException {
                final String randomString = getRandomString();
                final GraphTraversal[] graphTraversals = getTraversalsFromOperands(operands);
                graphTraversal.as(randomString).where(randomString, P.eq(randomString))
                        .by(graphTraversals[0]).by(graphTraversals[1]);
            }
        }

        public static class GremlinSqlBinaryOperatorAppenderNotEquals implements GremlinSqlTraversalAppender {
            public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                    throws SQLException {
                final String randomString = getRandomString();
                final GraphTraversal[] graphTraversals = getTraversalsFromOperands(operands);
                graphTraversal.as(randomString).where(randomString, P.neq(randomString))
                        .by(graphTraversals[0]).by(graphTraversals[1]);
            }
        }

        public static class GremlinSqlBinaryOperatorAppenderGreater implements GremlinSqlTraversalAppender {
            public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                    throws SQLException {
                final String randomString = getRandomString();
                final GraphTraversal[] graphTraversals = getTraversalsFromOperands(operands);
                graphTraversal.as(randomString).where(randomString, P.gt(randomString))
                        .by(graphTraversals[0]).by(graphTraversals[1]);
            }
        }

        public static class GremlinSqlBinaryOperatorAppenderGreaterEquals implements GremlinSqlTraversalAppender {
            public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                    throws SQLException {
                final String randomString = getRandomString();
                final GraphTraversal[] graphTraversals = getTraversalsFromOperands(operands);
                graphTraversal.as(randomString).where(randomString, P.gte(randomString))
                        .by(graphTraversals[0]).by(graphTraversals[1]);
            }
        }

        public static class GremlinSqlBinaryOperatorAppenderLess implements GremlinSqlTraversalAppender {
            public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                    throws SQLException {
                final String randomString = getRandomString();
                final GraphTraversal[] graphTraversals = getTraversalsFromOperands(operands);
                graphTraversal.
                        as(randomString).
                        where(randomString, P.lt(randomString)).by(graphTraversals[0]).by(graphTraversals[1]);
            }
        }

        public static class GremlinSqlBinaryOperatorAppenderLessEquals implements GremlinSqlTraversalAppender {
            public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                    throws SQLException {
                final String randomString = getRandomString();
                final GraphTraversal[] graphTraversals = getTraversalsFromOperands(operands);
                graphTraversal.as(randomString).where(randomString, P.lte(randomString))
                        .by(graphTraversals[0]).by(graphTraversals[1]);
            }
        }

        public static class GremlinSqlBinaryOperatorAppenderAnd implements GremlinSqlTraversalAppender {
            public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                    throws SQLException {
                final GraphTraversal[] graphTraversals = getTraversalsFromOperands(operands);
                __.and(graphTraversals[0], graphTraversals[1]);
            }
        }

        public static class GremlinSqlBinaryOperatorAppenderOr implements GremlinSqlTraversalAppender {
            public void appendTraversal(final GraphTraversal<?, ?> graphTraversal, final List<GremlinSqlNode> operands)
                    throws SQLException {
                final GraphTraversal[] graphTraversals = getTraversalsFromOperands(operands);
                __.or(graphTraversals[0], graphTraversals[1]);
            }
        }
    }
}
