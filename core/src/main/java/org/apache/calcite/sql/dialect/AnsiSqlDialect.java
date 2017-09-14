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

/**
 * A <code>SqlDialect</code> implementation for an unknown ANSI compatible database.
 */
public class AnsiSqlDialect extends SqlDialect {
  /**
   * A dialect useful for generating generic SQL. If you need to do something
   * database-specific like quoting identifiers, don't rely on this dialect to
   * do what you want.
   */
  public static final SqlDialect DEFAULT =
      new AnsiSqlDialect(emptyContext()
          .withDatabaseProduct(DatabaseProduct.UNKNOWN)
          .withIdentifierQuoteString("`"));

  /** Creates an AnsiSqlDialect. */
  public AnsiSqlDialect(Context context) {
    super(context);
  }
}

// End AnsiSqlDialect.java
