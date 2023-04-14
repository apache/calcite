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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class SqlTruncate extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("TRUNCATE", SqlKind.TRUNCATE);

  SqlNode targetTable;
  @Nullable SqlSelect sourceSelect;

  boolean ifExists;

  //~ Constructors -----------------------------------------------------------

  public SqlTruncate(
      SqlParserPos pos,
      SqlNode targetTable,
      boolean ifExists,
      @Nullable SqlSelect sourceSelect) {
    super(pos);
    this.targetTable = targetTable;
    this.sourceSelect = sourceSelect;
    this.ifExists = ifExists;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.TRUNCATE;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTable);
  }

  /**
   * Returns the identifier for the target table of the truncate.
   */
  public SqlNode getTargetTable() {
    return targetTable;
  }

  /**
   * Gets the source SELECT expression for the data to be deleted. This
   * returns null before the condition has been expanded by
   * {@link SqlValidatorImpl#performUnconditionalRewrites(SqlNode, boolean)}.
   *
   * @return the source SELECT for the data to be inserted
   */
  public @Nullable SqlSelect getSourceSelect() {
    return sourceSelect;
  }

//  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
//    validator.validateTruncate(this);
//  }

  public void setSourceSelect(SqlSelect sourceSelect) {
    this.sourceSelect = sourceSelect;
  }

}
