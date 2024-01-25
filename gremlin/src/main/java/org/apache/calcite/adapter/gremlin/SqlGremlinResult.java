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

import org.apache.calcite.adapter.gremlin.results.SqlGremlinQueryResult;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SqlGremlinResult {

  private final List<List<Object>> rows = new ArrayList<>();
  private final List<String> columns;

  public List<List<Object>> getRows() {
    return rows;
  }

  public List<String> getColumns() {
    return columns;
  }

  public SqlGremlinResult(final SqlGremlinQueryResult sqlGremlinQueryResult) throws SQLException {
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
