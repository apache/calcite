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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

/**
 * A <code>SqlDialect</code> implementation for the PostGIS database that extends
 * the PostgreSQL dialect.
 *
 *
 * <p>As PostGIS is an extension of PostgreSQL that must be installed separately,
 * it makes sense to extend the PostgreSQL dialect. Having a separate dialect
 * allows for the possibility of adding PostGIS-specific features in the future.
 * It also isolates PostGIS-specific behavior from the PostgreSQL dialect.
 */
public class PostgisSqlDialect extends PostgresqlSqlDialect {

  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(DatabaseProduct.POSTGIS)
      .withConformance(SqlConformanceEnum.LENIENT)
      .withIdentifierQuoteString("\"")
      .withUnquotedCasing(Casing.TO_LOWER)
      .withDataTypeSystem(POSTGRESQL_TYPE_SYSTEM);

  public static final SqlDialect DEFAULT = new PostgisSqlDialect(DEFAULT_CONTEXT);

  /** Creates a PostgisSqlDialect. */
  public PostgisSqlDialect(Context context) {
    super(context);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.getKind() == SqlKind.ST_UNARYUNION) {
      new SqlSpecialOperator(SqlKind.ST_UNION.name(), SqlKind.ST_UNION)
          .unparse(writer, call, 0, 0);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }
}
