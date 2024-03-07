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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Parse tree for {@code GRANT} statement.
 */
public class SqlGrant extends SqlAuthCommand {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("GRANT", SqlKind.GRANT);

  /**
   * Creates a SqlGrant.
   */
  public SqlGrant(SqlParserPos pos, SqlNodeList accesses,
      SqlNodeList objects, ObjectType type, SqlNodeList users) {
    super(OPERATOR, pos, accesses, objects, type, users);
  }

  @Override public SqlKind getKind() {
    return SqlKind.GRANT;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("GRANT");
    accesses.unparse(writer, leftPrec, rightPrec);
    writer.keyword("ON");
    if (type == ObjectType.SCHEMA) {
      writer.keyword("ALL TABLES IN SCHEMA");
    }
    objects.unparse(writer, leftPrec, rightPrec);
    writer.keyword("TO");
    users.unparse(writer, leftPrec, rightPrec);
  }
}
