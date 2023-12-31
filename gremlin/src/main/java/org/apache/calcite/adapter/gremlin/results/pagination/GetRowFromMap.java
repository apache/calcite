package org.apache.calcite.adapter.gremlin.results.pagination;

import java.util.Map;

interface GetRowFromMap {
    Object[] execute(Map<String, Object> input);
}
