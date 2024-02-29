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
package org.apache.calcite.adapter.gremlin;

import org.apache.calcite.adapter.gremlin.converter.SqlConverter;
import org.apache.calcite.adapter.gremlin.converter.schema.SqlSchemaGrabber;
import org.apache.calcite.adapter.gremlin.converter.schema.calcite.GremlinSchema;
import org.apache.calcite.adapter.gremlin.results.SqlGremlinQueryResult;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import org.junit.jupiter.api.Assertions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class GremlinSqlBaseTest {
    private final Graph graph;
    private final GraphTraversalSource g;
    private final SqlConverter converter;

    GremlinSqlBaseTest() throws SQLException {
        graph = TestGraphFactory.createGraph(getDataSet());
        g = graph.traversal();
        final GremlinSchema gremlinSchema = SqlSchemaGrabber.getSchema(g, SqlSchemaGrabber.ScanType.All);
        converter = new SqlConverter(gremlinSchema, g);
    }

    protected abstract DataSet getDataSet();

    protected void runQueryTestResults(final String query, final List<String> columnNames,
                                       final List<List<Object>> rows)
            throws SQLException {
        final SqlGremlinTestResult result = new SqlGremlinTestResult(converter.executeQuery(query));
        assertRows(result.getRows(), rows);
        assertColumns(result.getColumns(), columnNames);
    }

    public List<List<Object>> rows(final List<Object>... rows) {
        return new ArrayList<>(Arrays.asList(rows));
    }

    public List<String> columns(final String... columns) {
        return new ArrayList<>(Arrays.asList(columns));
    }

    public List<Object> r(final Object... row) {
        return new ArrayList<>(Arrays.asList(row));
    }

    public void assertRows(final List<List<Object>> actual, final List<List<Object>> expected) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertEquals(expected.get(i).size(), actual.get(i).size());
            for (int j = 0; j < actual.get(i).size(); j++) {
                Assertions.assertEquals(expected.get(i).get(j), actual.get(i).get(j));
            }
        }
    }

    public void assertColumns(final List<String> actual, final List<String> expected) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            Assertions.assertEquals(expected.get(i), actual.get(i));
        }
    }

    public enum DataSet {
        SPACE,
        DATA_TYPES
    }

    static class SqlGremlinTestResult {
        private final List<List<Object>> rows = new ArrayList<>();
        private final List<String> columns;

      public List<List<Object>> getRows() {
        return rows;
      }

      public List<String> getColumns() {
        return columns;
      }

      SqlGremlinTestResult(final SqlGremlinQueryResult sqlGremlinQueryResult) throws SQLException {
            columns = sqlGremlinQueryResult.getColumns();
            Object res;
            do {
                try {
                    res = sqlGremlinQueryResult.getResult();
                } catch (final SQLException e) {
                    if (e.getMessage().equals(SqlGremlinQueryResult.EMPTY_MESSAGE)) {
                        break;
                    } else {
                        throw e;
                    }
                }
                this.rows.add((List<Object>) res);
            } while (true);
        }
    }
}
