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
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A <code>SqlDynamicParam</code> represents a named parameter marker in a
 * SQL statement that refers to a Bodo variable from a JIT function.
 * This name exactly matches a name that can be used to determine the type
 * and value of the variable in question.
 */
public class SqlNamedParam extends SqlNode {
  //~ Instance fields --------------------------------------------------------

  private final String name;

  //~ Constructors -----------------------------------------------------------

  public SqlNamedParam(
      String name,
      SqlParserPos pos) {
    super(pos);
    this.name = name;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlNode clone(SqlParserPos pos) {
    return new SqlNamedParam(name, pos);
  }

  @Override public SqlKind getKind() {
    return SqlKind.NAMED_PARAM;
  }

  public String getName() {
    return name;
  }

  // TODO: Fix Me
  @Override public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.namedParam(name);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateNamedParam(this);
  }

  @Override public SqlMonotonicity getMonotonicity(@Nullable SqlValidatorScope scope) {
    return SqlMonotonicity.CONSTANT;
  }

  @Override public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlNamedParam)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlNamedParam that = (SqlNamedParam) node;
    if (!this.name.equals(that.name)) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }
}
