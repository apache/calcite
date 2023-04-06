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
package org.apache.calcite.sql.babel.postgres;

import org.apache.calcite.sql.Symbolizable;

/**
 * Contains enums convertible to {@link org.apache.calcite.sql.SqlLiteral}, used for
 * {@link org.apache.calcite.sql.SqlSetOption#name} and
 * {@link org.apache.calcite.sql.SqlSetOption#value} fields.
 */
public class SqlSetOptions {
  private SqlSetOptions() {
  }

  /**
   * {@link org.apache.calcite.sql.SqlIdentifier} can not be used
   * as a name parameter. For example, in PostgreSQL, these two SQL commands
   * have different meanings:
   * <ul>
   *   <li><code>RESET ALL</code> resets all settable run-time parameters to default values.</li>
   *   <li><code>RESET "ALL"</code> resets parameter "ALL".</li>
   * </ul>
   * Using only {@link org.apache.calcite.sql.SqlIdentifier} makes
   * it impossible to distinguish which case is being referred to.
   * This enum has been introduced to avoid this problem.
   */
  public enum Names implements Symbolizable {
    ALL,
    TRANSACTION,
    TRANSACTION_SNAPSHOT,
    SESSION_CHARACTERISTICS_AS_TRANSACTION,
    TIME_ZONE,
    ROLE;

    @Override public String toString() {
      return super.toString().replace("_", " ");
    }
  }

  /**
   * Makes possible to represent NONE, LOCAL as {@link org.apache.calcite.sql.SqlLiteral}.
   * It is needed for the following SQL statements:
   * <ul>
   *   <li><code>SET TIME ZONE LOCAL</code></li>
   *   <li><code>SET ROLE NONE</code></li>
   * </ul>
   *
   * @see <a href="https://www.postgresql.org/docs/current/sql-set.html">
   * PostgreSQL SET documentation</a>
   * @see <a href="https://www.postgresql.org/docs/current/sql-set-role.html">
   * PostgreSQL SET ROLE documentation</a>
   */
  public enum Values implements Symbolizable {
    NONE,
    LOCAL
  }
}
