package org.apache.calcite.adapter.gremlin.results.pagination;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SimpleDataReader implements GetRowFromMap {
    private final String label;
    private final List<String> columnNames;

    public SimpleDataReader(final String label, final List<String> columnNames) {
        this.label = label;
        this.columnNames = columnNames;
    }

    @Override
    public Object[] execute(final Map<String, Object> map) {
        final Object[] row = new Object[columnNames.size()];
        int i = 0;
        for (final String column : columnNames) {
            final Optional<String> tableKey =
                    map.keySet().stream().filter(key -> key.equalsIgnoreCase(label)).findFirst();
            if (!tableKey.isPresent()) {
                row[i++] = null;
                continue;
            }

            final Optional<String> columnKey = ((Map<String, Object>) map.get(tableKey.get())).keySet().stream()
                    .filter(key -> key.equalsIgnoreCase(column)).findFirst();
            if (!columnKey.isPresent()) {
                row[i++] = null;
                continue;
            }
            row[i++] = ((Map<String, Object>) map.get(tableKey.get())).getOrDefault(columnKey.get(), null);
        }
        return row;
    }
}
