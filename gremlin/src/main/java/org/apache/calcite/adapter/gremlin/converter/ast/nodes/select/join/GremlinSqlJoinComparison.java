package org.apache.calcite.adapter.gremlin.converter.ast.nodes.select.join;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;

import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.GremlinSqlNode;
import org.apache.calcite.adapter.gremlin.converter.ast.nodes.operands.GremlinSqlIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * This module is a GremlinSql equivalent of Calcite's SqlBinaryOperator in the context of a comparison of a JOIN.
 */
public class GremlinSqlJoinComparison {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlJoinComparison.class);
    // See SqlKind.BINARY_COMPARISON for list of aggregate functions in Calcite.

    private final SqlBasicCall sqlBasicCall;
    private final SqlBinaryOperator sqlBinaryOperator;
    private final SqlMetadata sqlMetadata;
    private final List<GremlinSqlNode> gremlinSqlNodes;


    public GremlinSqlJoinComparison(final SqlBasicCall sqlBasicCall,
                                    final SqlBinaryOperator sqlBinaryOperator,
                                    final List<GremlinSqlNode> gremlinSqlNodes,
                                    final SqlMetadata sqlMetadata) {
        this.sqlBasicCall = sqlBasicCall;
        this.sqlBinaryOperator = sqlBinaryOperator;
        this.sqlMetadata = sqlMetadata;
        this.gremlinSqlNodes = gremlinSqlNodes;
    }

    public boolean isEquals() {
        return sqlBinaryOperator.kind.sql.equals(SqlKind.EQUALS.sql);
    }

    public String getColumn(final String renamedTable) throws SQLException {
        for (final GremlinSqlNode gremlinSqlNode : gremlinSqlNodes) {
            if (!(gremlinSqlNode instanceof GremlinSqlIdentifier)) {
                throw new SQLException("Error: Expected nodes in join comparison to be GremlinSqlIdentifiers.");
            }
            final GremlinSqlIdentifier gremlinSqlIdentifier = (GremlinSqlIdentifier) gremlinSqlNode;
            if (gremlinSqlIdentifier.getName(0).equals(renamedTable)) {
                return gremlinSqlIdentifier.getName(1);
            }
        }
        throw new SQLException("Error: Expected to find join column for renamed table.");
    }
}
