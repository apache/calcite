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
package org.eigenbase.sql;

import java.util.List;

import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;

import com.google.common.collect.ImmutableList;

/**
 * SQL parse tree node to represent <code>ALTER scope SET option = value</code>
 * statement.
 *
 * <p>Example:</p>
 *
 * <blockquote>ALTER SYSTEM SET myParam = 1</blockquote>
 */
public class SqlSetOption extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SET_OPTION", SqlKind.SET_OPTION);

  /** Scope of the assignment. Values "SYSTEM" and "SESSION" are typical. */
  String scope;

  String name;

  /** Value of the option. May be a {@link org.eigenbase.sql.SqlLiteral} or
   * a {@link org.eigenbase.sql.SqlIdentifier} with one part. Reserved words
   * (currently just 'ON') are converted to identifiers by the parser. */
  SqlNode value;

  /**
   * Creates a node.
   *
   * @param pos Parser position, must not be null.
   * @param scope Scope (generally "SYSTEM" or "SESSION")
   * @param name Name of option
   * @param value Value of option, as an identifier or literal.
   */
  public SqlSetOption(SqlParserPos pos, String scope, String name,
      SqlNode value) {
    super(pos);
    this.scope = scope;
    this.name = name;
    this.value = value;
    assert scope != null;
    assert name != null;
    assert value != null;
  }

  @Override
  public SqlKind getKind() {
    return SqlKind.SET_OPTION;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(
        new SqlIdentifier(scope, SqlParserPos.ZERO),
        new SqlIdentifier(name, SqlParserPos.ZERO),
        value);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      this.scope = ((SqlIdentifier) operand).getSimple();
      break;
    case 1:
      this.name = ((SqlIdentifier) operand).getSimple();
      break;
    case 2:
      this.value = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER");
    writer.keyword(getScope());
    writer.keyword("SET");
    final SqlWriter.Frame frame =
      writer.startList(
        SqlWriter.FrameTypeEnum.SIMPLE);
    writer.identifier(getName());
    writer.sep("=");
    value.unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    validator.validate(value);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getScope() {
    return scope;
  }

  public void setScope(String scope) {
    this.scope = scope;
  }

  public SqlNode getValue() {
    return value;
  }

  public void setValue(SqlNode value) {
    this.value = value;
  }
}

// End SqlSetOption.java
