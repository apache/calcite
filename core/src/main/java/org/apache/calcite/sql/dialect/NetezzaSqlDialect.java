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

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A <code>SqlDialect</code> implementation for the Netezza database.
 */
public class NetezzaSqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.NETEZZA)
      .withIdentifierQuoteString("\"");

  public static final SqlDialect DEFAULT = new NetezzaSqlDialect(DEFAULT_CONTEXT);

  /** Creates a NetezzaSqlDialect. */
  public NetezzaSqlDialect(Context context) {
    super(context);
  }

  @Override public void quoteStringLiteral(StringBuilder buf,
      @Nullable String charsetName, String val) {
    // Netezza accepts C-style backslash escape sequences inside single-quoted
    // string literals unless the session's standard-conforming-strings option
    // is enabled, so a literal backslash must be doubled before the base
    // method escapes the enclosing quote. Otherwise a value ending in a
    // backslash terminates the literal early and the trailing text is parsed
    // as SQL rather than data.
    super.quoteStringLiteral(buf, charsetName, escapeBackslash(val));
  }
}
