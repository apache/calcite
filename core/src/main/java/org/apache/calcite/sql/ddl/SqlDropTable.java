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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlBasicOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import static java.util.Objects.requireNonNull;

/**
 * Parse tree for {@code DROP TABLE} statement.
 */
public class SqlDropTable extends SqlDropObject {
  private static final SqlOperator OPERATOR =
      SqlBasicOperator.create("DROP TABLE", SqlKind.DROP_TABLE)
          .withCallFactory((operator, functionQualifier, pos, operands) ->
              new SqlDropTable(
                  pos,
                  ((SqlLiteral) requireNonNull(operands[0])).booleanValue(),
                  (SqlIdentifier) requireNonNull(operands[1])));

  /** Creates a SqlDropTable. */
  SqlDropTable(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
    super(OPERATOR, pos, ifExists, name);
  }
}
