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
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.List;


/**
 * Base class for parse trees of {@code DROP TABLE}, {@code DROP VIEW},
 * {@code DROP MATERIALIZED VIEW} and {@code DROP TYPE} statements.
 */
public abstract class SqlDropObject extends SqlDrop {
  public final SqlIdentifier name;

  /** Creates a SqlDropObject. */
  SqlDropObject(SqlOperator operator, SqlParserPos pos, boolean ifExists,
      SqlIdentifier name) {
    super(operator, pos, ifExists);
    this.name = name;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getOperator().getName()); // "DROP TABLE" etc.
    if (ifExists) {
      writer.keyword("IF EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
  }

  public void execute(CalcitePrepare.Context context) {
  }
}
