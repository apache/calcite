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

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;

/**
 * A <code>SqlDialect</code> implementation for the Lucidb database.
 */
public class LucidbSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT = new LucidbSqlDialect();

  @SuppressWarnings("deprecation") public LucidbSqlDialect(
      String databaseProduct, String databaseVersion,
      String identifierQuoteString, NullCollation nullCollation) {
    super(DatabaseProduct.LUCIDDB, identifierQuoteString, nullCollation);
  }

  @SuppressWarnings("deprecation") private LucidbSqlDialect() {
    super(DatabaseProduct.LUCIDDB, "\"", NullCollation.HIGH);
  }
}

// End LucidbSqlDialect.java
