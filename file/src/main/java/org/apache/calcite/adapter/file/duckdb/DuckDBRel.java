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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Relational expression that can be implemented in DuckDB.
 */
public interface DuckDBRel extends RelNode {

  /** Calling convention for DuckDB operators */
  Convention CONVENTION = new Convention.Impl("DUCKDB", DuckDBRel.class);

  /**
   * Generates SQL for this operation.
   */
  void generateSql(SqlBuilder builder);

  /**
   * Helper class for building SQL queries.
   */
  class SqlBuilder {
    private final StringBuilder sql = new StringBuilder();
    private final List<String> tableAliases = new ArrayList<>();
    private int aliasCounter = 0;

    public String nextAlias() {
      return "t" + (aliasCounter++);
    }

    public SqlBuilder append(String s) {
      sql.append(s);
      return this;
    }

    public SqlBuilder append(char c) {
      sql.append(c);
      return this;
    }

    public String getSql() {
      return sql.toString();
    }

    @Override public String toString() {
      return sql.toString();
    }
  }
}
