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
package org.apache.calcite.rex;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWindow;

/**
 * Representation of different kinds of exclude clause in window functions.
 */
public enum RexWindowExclusion {
  EXCLUDE_NO_OTHER("EXCLUDE NO OTHER"),
  EXCLUDE_CURRENT_ROW("EXCLUDE CURRENT ROW"),
  EXCLUDE_TIES("EXCLUDE TIES"),
  EXCLUDE_GROUP("EXCLUDE GROUP");

  private final String sql;

  RexWindowExclusion(String sql) {
    this.sql = sql;
  }

  @Override public String toString() {
    return sql;
  }

  /**
   * Creates a window exclusion from a {@link SqlNode}.
   *
   * @param node SqlLiteral of the exclusion clause
   * @return RexWindowExclusion
   */
  public static RexWindowExclusion create(SqlNode node) {
    SqlLiteral exclude = (SqlLiteral) node;
    if (SqlWindow.isExcludeCurrentRow(exclude)) {
      return EXCLUDE_CURRENT_ROW;
    }
    if (SqlWindow.isExcludeNoOthers(exclude)) {
      return EXCLUDE_NO_OTHER;
    }
    if (SqlWindow.isExcludeGroup(exclude)) {
      return EXCLUDE_GROUP;
    }
    if (SqlWindow.isExcludeTies(exclude)) {
      return EXCLUDE_TIES;
    }
    throw new AssertionError("Unexpected Exclusion clause");
  }
}
