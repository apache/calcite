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
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * SQL parse tree node to represent these statements:
 * <code>
 * ALTER scope SET `option.name` = value;
 * ALTER scope RESET `option`.`name`;
 * ALTER scope RESET ALL;
 * <p/>
 * SET `option.name` = value;
 * RESET `option`.`name`;
 * RESET ALL;
 * </code>
 * <p/>
 * If {@link #scope} is null, assume a default scope.
 * <p/>
 * If {@link #value} is null, assume RESET.
 * If {@link #value} is not null, assume SET.
 * <p/>
 * <p>Examples:</p>
 * <blockquote>ALTER SYSTEM SET `my`.`param1` = 1</blockquote>
 * <blockquote>SET `my.param2` = 1</blockquote>
 * <blockquote>ALTER SYSTEM RESET `my`.`param1`</blockquote>
 * <blockquote>RESET `my.param2`</blockquote>
 */
public class SqlSetOption extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SET_OPTION", SqlKind.SET_OPTION);

  /** Scope of the assignment. Values "SYSTEM" and "SESSION" are typical. */
  String scope;

  /** Name of the option as a {@link org.apache.calcite.sql.SqlIdentifier}
   *  with one or more parts.*/
  SqlNode name;

  /** Value of the option. May be a {@link org.apache.calcite.sql.SqlLiteral} or
   * a {@link org.apache.calcite.sql.SqlIdentifier} with one
   * part. Reserved words (currently just 'ON') are converted to
   * identifiers by the parser. */
  SqlNode value;

  /**
   * Creates a node.
   *
   * @param pos Parser position, must not be null.
   * @param scope Scope (generally "SYSTEM" or "SESSION")
   * @param name Name of option, as an identifier.
   * @param value Value of option, as an identifier or literal.
   */
  public SqlSetOption(SqlParserPos pos, String scope, SqlNode name,
      SqlNode value) {
    super(pos);
    this.scope = scope;
    this.name = name;
    this.value = value;
    assert name != null;
  }

  @Override public SqlKind getKind() {
    return SqlKind.SET_OPTION;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    final List<SqlNode> operandList = Lists.newArrayList();
    if (scope == null) {
      operandList.add(null);
    } else {
      operandList.add(new SqlIdentifier(scope, SqlParserPos.ZERO));
    }
    operandList.add(name);
    operandList.add(value);
    return ImmutableNullableList.copyOf(operandList);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      if (operand != null) {
        scope = ((SqlIdentifier) operand).getSimple();
      }
      break;
    case 1:
      name = operand;
      break;
    case 2:
      value = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (scope != null) {
      writer.keyword("ALTER");
      writer.keyword(scope);
    }
    if (value != null) {
      writer.keyword("SET");
    } else {
      writer.keyword("RESET");
    }
    final SqlWriter.Frame frame =
      writer.startList(
        SqlWriter.FrameTypeEnum.SIMPLE);
    name.unparse(writer, leftPrec, rightPrec);
    if (value != null) {
      writer.sep("=");
      value.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    validator.validate(value);
  }

  public SqlNode getName() {
    return name;
  }

  public void setName(SqlNode name) {
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
