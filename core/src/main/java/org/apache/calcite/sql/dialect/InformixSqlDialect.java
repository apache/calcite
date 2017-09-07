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

import java.sql.DatabaseMetaData;

/**
 * A <code>SqlDialect</code> implementation for the Informix database.
 */
public class InformixSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT = new InformixSqlDialect();

  public InformixSqlDialect(DatabaseMetaData databaseMetaData) {
    super(DatabaseProduct.INFORMIX, databaseMetaData, null);
  }

  private InformixSqlDialect() {
    super(DatabaseProduct.INFORMIX, null, NullCollation.HIGH, null);
  }
}

// End InformixSqlDialect.java
