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

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class GremlinSqlAggregateTest extends GremlinSqlBaseTest {

    GremlinSqlAggregateTest() throws SQLException {
    }

    @Override protected DataSet getDataSet() {
        return DataSet.SPACE;
    }

    @Test public void testAggregateFunctions() throws SQLException {
        runQueryTestResults("select count(age), min(age), max(age), avg(age) from person",
                columns("COUNT(age)", "MIN(age)", "MAX(age)", "AVG(age)"),
                rows(r(6L, 29, 50, 36.5)));
    }
}
