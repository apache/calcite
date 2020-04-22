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
package org.apache.calcite.sql.babel;

import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Utilities concerning {@link SqlNode} for DDL.
 *
 * <p>It was created by copying {@code org.apache.calcite.ddl.DdlNodes}
 * and then removing the pieces we didn't need. Feel free to add bits back,
 * but keep them in the same order so that we can diff/merge.
 */
public class SqlDdlNodes {
  private SqlDdlNodes() {}

  /** Creates a column declaration. */
  public static SqlNode column(SqlParserPos pos, SqlIdentifier name,
      SqlDataTypeSpec dataType, SqlNode expression, ColumnStrategy strategy) {
    return new SqlColumnDeclaration(pos, name, dataType, expression, strategy);
  }
}
