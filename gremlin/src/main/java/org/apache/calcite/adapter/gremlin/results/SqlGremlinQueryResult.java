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
package org.apache.calcite.adapter.gremlin.results;

import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinProperty;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinTableBase;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SqlGremlinQueryResult {
    public static final String EMPTY_MESSAGE = "No more results.";
    public static final String NULL_VALUE = "$%#NULL#%$";
    private final List<String> columns;
    private final List<String> columnTypes = new ArrayList<>();
    private final Object assertEmptyLock = new Object();
    private final BlockingQueue<List<Object>> blockingQueueRows = new LinkedBlockingQueue<>();
    private boolean isEmpty = false;
    private SQLException paginationException = null;
    private Thread currThread = null;

    public SqlGremlinQueryResult(final List<String> columns, final List<GremlinTableBase> gremlinTableBases,
                                 final SqlMetadata sqlMetadata) throws SQLException {
        this.columns = columns;
        for (final String column : columns) {
            GremlinProperty col = null;
            for (final GremlinTableBase gremlinTableBase : gremlinTableBases) {
                if (sqlMetadata.getTableHasColumn(gremlinTableBase, column)) {
                    col = sqlMetadata.getGremlinProperty(gremlinTableBase.getLabel(), column);
                    break;
                }
            }
            columnTypes.add((col == null || col.getType() == null) ? "string" : col.getType());
        }
    }

  public List<String> getColumns() {
    return columns;
  }

  public List<String> getColumnTypes() {
    return columnTypes;
  }

  public Object getAssertEmptyLock() {
    return assertEmptyLock;
  }

  public BlockingQueue<List<Object>> getBlockingQueueRows() {
    return blockingQueueRows;
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  public SQLException getPaginationException() {
    return paginationException;
  }

  public Thread getCurrThread() {
    return currThread;
  }

  public void setPaginationException(final SQLException e) {
        synchronized (assertEmptyLock) {
            paginationException = e;
            if (currThread != null && blockingQueueRows.size() == 0) {
                currThread.interrupt();
            }
        }
    }

    public boolean getIsEmpty() throws SQLException {
        if (paginationException == null) {
            return isEmpty;
        }
        throw paginationException;
    }

    public void assertIsEmpty() {
        synchronized (assertEmptyLock) {
            if (currThread != null && blockingQueueRows.size() == 0) {
                currThread.interrupt();
            }
            isEmpty = true;
        }
    }

    public void addResults(final List<List<Object>> rows) {
        for (final List<Object> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                if (row.get(i) instanceof String && row.get(i).toString().equals(NULL_VALUE)) {
                    row.set(i, null);
                }
            }
        }
        blockingQueueRows.addAll(rows);
    }

    private boolean getShouldExit() throws SQLException {
        synchronized (assertEmptyLock) {
            return (getIsEmpty() && blockingQueueRows.size() == 0);
        }
    }

    public Object getResult() throws SQLException {
        synchronized (assertEmptyLock) {
            this.currThread = Thread.currentThread();
        }
        while (!getShouldExit()) {
            try {
                return this.blockingQueueRows.take();
            } catch (final InterruptedException ignored) {
                if (paginationException != null) {
                    throw paginationException;
                }
            }
        }
        throw new SQLException(EMPTY_MESSAGE);
    }
}
