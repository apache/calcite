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
package org.apache.calcite.sql.ddl;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.materialize.MaterializationKey;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

/**
 * Parse tree for {@code DROP MATERIALIZED VIEW} statement.
 */
public class SqlDropMaterializedView extends SqlDropObject {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("DROP MATERIALIZED VIEW",
          SqlKind.DROP_MATERIALIZED_VIEW);

  /** Creates a SqlDropMaterializedView. */
  SqlDropMaterializedView(SqlParserPos pos, boolean ifExists,
      SqlIdentifier name) {
    super(OPERATOR, pos, ifExists, name);
  }

  @Override public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair =
        SqlDdlNodes.schema(context, true, name);
    final Table table = pair.left.plus().getTable(pair.right);
    if (table != null) {
      // Materialized view exists.
      super.execute(context);
      if (table instanceof Wrapper) {
        final MaterializationKey materializationKey =
            ((Wrapper) table).unwrap(MaterializationKey.class);
        if (materializationKey != null) {
          MaterializationService.instance()
              .removeMaterialization(materializationKey);
        }
      }
    }
  }
}

// End SqlDropMaterializedView.java
