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
import org.apache.calcite.sql.validate.SqlValidatorScope;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Base class for INSERT OVERWRITE TABLE and INSERT OVERWRITE DIRECTORY statements.
 */
public abstract class SqlInsertOverwrite extends SqlCall {
  protected static final SqlOperator INSERT_OVERWRITE_OPERATOR =
      new SqlSpecialOperator("INSERT_OVERWRITE", SqlKind.INSERT_OVERWRITE);

  private final SqlOperator operator;
  private final SqlNode target;
  private final SqlNode source;

  @Override public SqlOperator getOperator() {
    return operator;
  }

  public SqlNode getTarget() {
    return target;
  }

  public SqlNode getSource() {
    return source;
  }

  /** Creates a SqlInsertOverwrite. */
  protected SqlInsertOverwrite(SqlOperator operator,
      SqlNode target,
      SqlNode source,
      SqlParserPos pos) {
    super(pos);
    this.operator = requireNonNull(operator, "operator");
    this.target = requireNonNull(target, "target");
    this.source =  requireNonNull(source, "source");
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(target, source);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateInsertOverwrite(this);
  }
}
