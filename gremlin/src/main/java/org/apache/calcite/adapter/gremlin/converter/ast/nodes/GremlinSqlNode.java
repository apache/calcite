package org.apache.calcite.adapter.gremlin.converter.ast.nodes;

import org.apache.calcite.sql.SqlNode;

import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;

/**
 * This abstract class in the GremlinSql equivalent of SqlNode.
 */
@AllArgsConstructor
public abstract class GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlNode.class);
    private final SqlNode sqlNode;
    private final SqlMetadata sqlMetadata;
}
