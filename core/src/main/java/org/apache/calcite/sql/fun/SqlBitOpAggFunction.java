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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

import com.google.common.base.Preconditions;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Definition of the <code>BIT_AND</code> and <code>BIT_OR</code> aggregate functions,
 * returning the bitwise AND/OR of all non-null input values, or null if none.
 *
 * <p>INTEGER and BINARY types are supported:
 * tinyint, smallint, int, bigint, binary, varbinary
 */
public class SqlBitOpAggFunction extends SqlAggFunction {

  //~ Constructors -----------------------------------------------------------

  /** Creates a SqlBitOpAggFunction. */
  public SqlBitOpAggFunction(SqlKind kind) {
    super(kind.name(),
        null,
        kind,
        ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
        null,
        OperandTypes.INTEGER.or(OperandTypes.BINARY),
        SqlFunctionCategory.NUMERIC,
        false,
        false,
        Optionality.FORBIDDEN);
    Preconditions.checkArgument(kind == SqlKind.BIT_AND
        || kind == SqlKind.BIT_OR
        || kind == SqlKind.BIT_XOR);
  }

  @Override public <T extends Object> @Nullable T unwrap(Class<T> clazz) {
    if (clazz == SqlSplittableAggFunction.class) {
      return clazz.cast(SqlSplittableAggFunction.SelfSplitter.INSTANCE);
    }
    return super.unwrap(clazz);
  }

  @Override public Optionality getDistinctOptionality() {
    final Optionality optionality;

    switch (kind) {
    case BIT_AND:
    case BIT_OR:
      optionality = Optionality.IGNORED;
      break;
    default:
      optionality = Optionality.OPTIONAL;
      break;
    }
    return optionality;
  }
}
