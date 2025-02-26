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

/**
 * Base class for an ALTER statements parse tree nodes. The portion of the
 * statement covered by this class is "ALTER &lt;SCOPE&gt;. Subclasses handle
 * whatever comes after the scope.
 */
public abstract class SqlAlter extends SqlCall {

  /** Scope of the operation. Values "SYSTEM" and "SESSION" are typical. */
  @Nullable String scope;

  protected SqlAlter(SqlParserPos pos) {
    this(pos, null);
  }

  protected SqlAlter(SqlParserPos pos, @Nullable String scope) {
    super(pos);
    this.scope = scope;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    String scope = this.scope;
    if (scope != null) {
      writer.keyword("ALTER");
      writer.keyword(scope);
    }
    unparseAlterOperation(writer, leftPrec, rightPrec);
  }

  protected abstract void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec);

  public @Nullable String getScope() {
    return scope;
  }

  public void setScope(@Nullable String scope) {
    this.scope = scope;
  }

}
