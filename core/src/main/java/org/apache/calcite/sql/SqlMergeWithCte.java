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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * <code>SqlMergeWithCte</code> to handle cte in source for merge statement.
 */
public class SqlMergeWithCte extends SqlMerge {

  protected Map<String, SqlNode> cteSqlNodes = new HashMap<>();

  public SqlMergeWithCte(
      SqlParserPos pos, SqlNode targetTable, SqlNode condition, SqlNode source,
      @Nullable SqlUpdate updateCall, @Nullable SqlInsert insertCall,
      @Nullable SqlSelect sourceSelect, @Nullable SqlIdentifier alias,
      Map<String, SqlNode> cteSqlNodes) {
    super(pos, targetTable, condition, source, updateCall, insertCall, sourceSelect, alias);
    this.cteSqlNodes = cteSqlNodes;
  }

  public SqlMergeWithCte(SqlParserPos pos, SqlNode targetTable, SqlNode condition, SqlNode source,
      @Nullable SqlUpdate updateCall, @Nullable SqlDelete deleteCall,
      @Nullable SqlMergeInsert insertCall, @Nullable SqlSelect sourceSelect,
      @Nullable SqlIdentifier alias, Map<String, SqlNode> cteSqlNodes) {
    super(pos, targetTable, condition, source, updateCall, deleteCall, insertCall,
        sourceSelect, alias);
    this.cteSqlNodes = cteSqlNodes;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
  }
}
