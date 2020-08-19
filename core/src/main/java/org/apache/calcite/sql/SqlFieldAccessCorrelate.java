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

import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A <code>SqlFieldAccesslCorrelate</code> represents a dynamic field access correlate parameter
 * marker in an SQL statement. The field access correlate created by rules like
 * {@link org.apache.calcite.adapter.enumerable.EnumerableBatchNestedLoopJoin}.
 */
public class SqlFieldAccessCorrelate extends SqlNode {

  /** field access. */
  final RexFieldAccess rexFieldAccess;

  /** Creates a SqlFieldAccessCorrelate. */
  public SqlFieldAccessCorrelate(RexFieldAccess rexFieldAccess, SqlParserPos pos) {
    super(pos);
    this.rexFieldAccess = rexFieldAccess;
  }

  @Override public String toString() {
    return rexFieldAccess.toString();
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    return new SqlFieldAccessCorrelate(rexFieldAccess, pos);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.fieldAccessCorrelate(rexFieldAccess);
  }

  @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
  }

  @Override @Nullable public <R> R accept(SqlVisitor<R> visitor) {
    return null;
  }

  @Override public boolean equalsDeep(@Nullable SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlFieldAccessCorrelate)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlFieldAccessCorrelate that = (SqlFieldAccessCorrelate) node;
    if (!rexFieldAccess.equals(that.rexFieldAccess)) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }
}
