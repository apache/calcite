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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * SQL parse tree node to represent {@code SET} and {@code RESET} statements,
 * optionally preceded by {@code ALTER SYSTEM} or {@code ALTER SESSION}.
 *
 * <p>Syntax:
 *
 * <blockquote><code>
 * ALTER scope SET `option.name` = value;<br>
 * ALTER scope RESET `option`.`name`;<br>
 * ALTER scope RESET ALL;<br>
 * <br>
 * SET `option.name` = value;<br>
 * RESET `option`.`name`;<br>
 * RESET ALL;
 * </code></blockquote>
 *
 * <p>If {@link #scope} is null, assume a default scope. (The default scope
 * is defined by the project using Calcite, but is typically SESSION.)
 *
 * <p>If {@link #value} is null, assume RESET;
 * if {@link #value} is not null, assume SET.
 *
 * <p>Examples:
 *
 * <ul>
 * <li><code>ALTER SYSTEM SET `my`.`param1` = 1</code></li>
 * <li><code>SET `my.param2` = 1</code></li>
 * <li><code>SET `my.param3` = ON</code></li>
 * <li><code>ALTER SYSTEM RESET `my`.`param1`</code></li>
 * <li><code>RESET `my.param2`</code></li>
 * <li><code>ALTER SESSION RESET ALL</code></li>
 * </ul>
 */
public class SqlSetOption extends SqlAlter {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SET_OPTION", SqlKind.SET_OPTION) {
        @SuppressWarnings("argument.type.incompatible")
        @Override public SqlCall createCall(@Nullable SqlLiteral functionQualifier,
            SqlParserPos pos, @Nullable SqlNode... operands) {
          final SqlNode scopeNode = operands[0];
          return new SqlSetOption(pos,
              scopeNode == null ? null : scopeNode.toString(),
              (SqlIdentifier) operands[1], operands[2]);
        }
      };

  /** Name of the option as an {@link org.apache.calcite.sql.SqlIdentifier}
   * with one or more parts.*/
  SqlIdentifier name;

  /** Value of the option. May be a {@link org.apache.calcite.sql.SqlLiteral} or
   * a {@link org.apache.calcite.sql.SqlIdentifier} with one
   * part. Reserved words (currently just 'ON') are converted to
   * identifiers by the parser. */
  @Nullable SqlNode value;

  /**
   * Creates a node.
   *
   * @param pos Parser position, must not be null.
   * @param scope Scope (generally "SYSTEM" or "SESSION"), may be null.
   * @param name Name of option, as an identifier, must not be null.
   * @param value Value of option, as an identifier or literal, may be null.
   *              If null, assume RESET command, else assume SET command.
   */
  public SqlSetOption(SqlParserPos pos, @Nullable String scope, SqlIdentifier name,
      @Nullable SqlNode value) {
    super(pos, scope);
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

  @SuppressWarnings("nullness")
  @Override public List<SqlNode> getOperandList() {
    final List<@Nullable SqlNode> operandList = new ArrayList<>();
    if (scope == null) {
      operandList.add(null);
    } else {
      operandList.add(new SqlIdentifier(scope, SqlParserPos.ZERO));
    }
    operandList.add(name);
    operandList.add(value);
    return ImmutableNullableList.copyOf(operandList);
  }

  @Override public void setOperand(int i, @Nullable SqlNode operand) {
    switch (i) {
    case 0:
      if (operand != null) {
        scope = ((SqlIdentifier) operand).getSimple();
      } else {
        scope = null;
      }
      break;
    case 1:
      name = (SqlIdentifier) requireNonNull(operand, /**/ "name");
      break;
    case 2:
      value = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  @Override protected void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
    if (value != null) {
      writer.keyword("SET");
    } else {
      writer.keyword("RESET");
    }
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
    name.unparse(writer, leftPrec, rightPrec);
    if (value != null) {
      writer.sep("=");
      value.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    if (value != null) {
      validator.validate(value);
    }
  }

  public SqlIdentifier getName() {
    return name;
  }

  public void setName(SqlIdentifier name) {
    this.name = name;
  }

  public @Nullable SqlNode getValue() {
    return value;
  }

  public void setValue(SqlNode value) {
    this.value = value;
  }
}
