package org.apache.calcite.runtime.inmemory;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import com.google.common.collect.ImmutableMap.Builder;

public final class InMemorySchema extends AbstractSchema {
    private final Map<String, InMemoryTable<?>> inMemoryTableMap = new HashMap<>();

    public final void addTable(final String tableName, final InMemoryTable<?> tableObject) {
        inMemoryTableMap.put(tableName, tableObject);
    }

    @Override
    protected final Map<String, Table> getTableMap() {
        Builder tableMap = new Builder<String, Table>();
        for (Entry<String, InMemoryTable<?>> tableEntry : inMemoryTableMap.entrySet()) {
            String tableName = tableEntry.getKey();
            InMemoryTable<?> inMemoryTable = tableEntry.getValue();
            if (null != tableName && null != inMemoryTable) {
                tableMap.put(tableName, inMemoryTable);
            }
        }
        return tableMap.build();
    }
}
