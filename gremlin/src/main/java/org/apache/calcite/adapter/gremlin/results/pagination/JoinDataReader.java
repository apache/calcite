package org.apache.calcite.adapter.gremlin.results.pagination;

import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JoinDataReader implements GetRowFromMap {
    private final List<Pair<String, String>> tableColumnList = new ArrayList<>();

    public JoinDataReader(final Map<String, List<String>> tablesColumns) {
        tablesColumns.forEach((key, value) -> value.forEach(column -> tableColumnList.add(new Pair<>(key, column))));
    }

    @Override
    public Object[] execute(final Map<String, Object> map) {
        final Object[] row = new Object[tableColumnList.size()];
        int i = 0;
        for (final Pair<String, String> tableColumn : tableColumnList) {
            final Optional<String> tableKey =
                    map.keySet().stream().filter(key -> key.equalsIgnoreCase(tableColumn.left)).findFirst();
            if (!tableKey.isPresent()) {
                row[i++] = null;
                continue;
            }

            final Optional<String> columnKey = ((Map<String, Object>) map.get(tableKey.get())).keySet().stream()
                    .filter(key -> key.equalsIgnoreCase(tableColumn.right)).findFirst();
            if (!columnKey.isPresent()) {
                row[i++] = null;
                continue;
            }
            row[i++] = ((Map<String, Object>) map.get(tableKey.get())).getOrDefault(columnKey.get(), null);
        }
        return row;
    }
}
