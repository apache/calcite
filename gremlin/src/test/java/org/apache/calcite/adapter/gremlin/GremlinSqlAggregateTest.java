package org.apache.calcite.adapter.gremlin;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class GremlinSqlAggregateTest extends GremlinSqlBaseTest {

    GremlinSqlAggregateTest() throws SQLException {
    }

    @Override
    protected DataSet getDataSet() {
        return DataSet.SPACE;
    }

    @Test
    public void testAggregateFunctions() throws SQLException {
        runQueryTestResults("select count(age), min(age), max(age), avg(age) from person",
                columns("COUNT(age)", "MIN(age)", "MAX(age)", "AVG(age)"),
                rows(r(6L, 29, 50, 36.5)));
    }
}
