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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;

/**
 * A <code>SqlDialect</code> implementation for the Sybase database.
 */
public class SybaseSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.SYBASE);

  public static final SqlDialect DEFAULT = new SybaseSqlDialect(DEFAULT_CONTEXT);

  /** Creates a SybaseSqlDialect. */
  public SybaseSqlDialect(Context context) {
    super(context);
  }

  @Override public void unparseOffsetFetch(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    // No-op; see unparseTopN.
    // Sybase uses "SELECT TOP (n)" rather than "FETCH NEXT n ROWS".
  }

  @Override public void unparseTopN(SqlWriter writer, SqlNode offset,
      SqlNode fetch) {
    // Parentheses are not required, but we use them to be consistent with
    // Microsoft SQL Server, which recommends them but does not require them.
    //
    // Note that "fetch" is ignored.
    writer.keyword("TOP");
    writer.keyword("(");
    fetch.unparse(writer, -1, -1);
    writer.keyword(")");
  }
}

// End SybaseSqlDialect.java
