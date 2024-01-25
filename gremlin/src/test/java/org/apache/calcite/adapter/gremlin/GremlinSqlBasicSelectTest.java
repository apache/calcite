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

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

public class GremlinSqlBasicSelectTest extends GremlinSqlBaseTest {

    GremlinSqlBasicSelectTest() throws SQLException {
    }

    @Override protected DataSet getDataSet() {
        return DataSet.DATA_TYPES;
    }

    @Test void testStringQuery() throws SQLException {
        runQueryTestResults("SELECT \"key\" FROM stringtype", columns("key"), rows(r(GraphConstants.STRING_VALUE)));
    }

    @Test void testByteQuery() throws SQLException {
        runQueryTestResults("SELECT \"key\" FROM bytetype", columns("key"), rows(r(GraphConstants.BYTE_VALUE)));
    }

    @Test void testShortQuery() throws SQLException {
        runQueryTestResults("SELECT \"key\" FROM shorttype", columns("key"), rows(r(GraphConstants.SHORT_VALUE)));
    }

    @Test void testIntegerQuery() throws SQLException {
        runQueryTestResults("SELECT \"key\" FROM inttype", columns("key"), rows(r(GraphConstants.INTEGER_VALUE)));
    }

    @Test void testLongQuery() throws SQLException {
        runQueryTestResults("SELECT \"key\" FROM longtype", columns("key"), rows(r(GraphConstants.LONG_VALUE)));
    }

    @Test void testFloatQuery() throws SQLException {
        runQueryTestResults("SELECT \"key\" FROM floattype", columns("key"), rows(r(GraphConstants.FLOAT_VALUE)));
    }

    @Test void testDoubleQuery() throws SQLException {
        runQueryTestResults("SELECT \"key\" FROM doubletype", columns("key"), rows(r(GraphConstants.DOUBLE_VALUE)));
    }

    @Test void testDateQuery() throws SQLException {
        runQueryTestResults("SELECT \"key\" FROM datetype", columns("key"), rows(r(GraphConstants.DATE_VALUE)));
    }

    String getAsOperatorQuery(final String column, final String asColumn, final String table) {
        return String.format("SELECT %s AS %s FROM %s", column, asColumn, table);
    }

    String getAsOperatorQuery(final String column, final String asColumn, final String table, final String asTable) {
        return String.format("SELECT %s.%s AS %s FROM %s %s", asTable, column, asColumn, table, asTable);
    }

    @Test void testAsOperator() throws SQLException {
        final List<String> columns = ImmutableList.of("key", "\"key\"");
        final List<String> asColumns = ImmutableList.of("key", "\"key\"", "k", "\"k\"");
        final List<String> tables = ImmutableList.of("stringtype", "\"stringtype\"");
        final List<String> asTables = ImmutableList.of("st", "\"st\"");
        for (final String column : columns) {
            for (final String asColumn : asColumns) {
                for (final String table : tables) {
                    runQueryTestResults(getAsOperatorQuery(column, asColumn, table),
                            columns(asColumn.replace("\"", "")), rows(r(GraphConstants.STRING_VALUE)));
                    for (final String asTable : asTables) {
                        runQueryTestResults(getAsOperatorQuery(column, asColumn, table, asTable),
                                columns(asColumn.replace("\"", "")), rows(r(GraphConstants.STRING_VALUE)));
                    }
                }
            }
        }
    }
}
