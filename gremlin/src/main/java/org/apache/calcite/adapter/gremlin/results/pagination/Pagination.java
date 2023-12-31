package org.apache.calcite.adapter.gremlin.results.pagination;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GroovyTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import org.apache.calcite.adapter.gremlin.results.SqlGremlinQueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Pagination implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Pagination.class);
    private static final int DEFAULT_PAGE_SIZE = 1000;
    private final int pageSize = DEFAULT_PAGE_SIZE;
    private final GetRowFromMap getRowFromMap;
    private final GraphTraversal<?, ?> traversal;
    private final SqlGremlinQueryResult sqlGremlinQueryResult;

    @Override
    public void run() {
        try {
            LOGGER.info("Graph traversal: " +
                    GroovyTranslator.of("g").translate(traversal.asAdmin().getBytecode()));
            while (traversal.hasNext()) {
                final List<Object> rows = new ArrayList<>();
                traversal.next(pageSize).forEach(map -> rows.add(getRowFromMap.execute((Map<String, Object>) map)));
                convertAndInsertResult(sqlGremlinQueryResult, rows);
            }
            // If we run out of traversal data (or hit our limit), stop and signal to the result that it is done.
            sqlGremlinQueryResult.assertIsEmpty();
        } catch (final Exception e) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            LOGGER.error("Encountered exception", e);
            sqlGremlinQueryResult.setPaginationException(new SQLException(e + pw.toString()));
        }
    }

    /**
     * converts input row results and insert them into sqlGremlinQueryResult
     */
    void convertAndInsertResult(final SqlGremlinQueryResult sqlGremlinQueryResult, final List<Object> rows) {
        final List<List<Object>> finalRowResult = new ArrayList<>();
        for (final Object row : rows) {
            final List<Object> convertedRow = new ArrayList<>();
            if (row instanceof Object[]) {
                convertedRow.addAll(Arrays.asList((Object[]) row));
            } else {
                convertedRow.add(row);
            }
            finalRowResult.add(convertedRow);
        }
        sqlGremlinQueryResult.addResults(finalRowResult);
    }
}
