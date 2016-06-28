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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Returns the first type that matches a set of given {@link SqlTypeName}s. If
 * no match could be found, null is returned.
 */
public class MatchReturnTypeInference implements SqlReturnTypeInference {
  //~ Instance fields --------------------------------------------------------

  private final int start;
  private final List<SqlTypeName> typeNames;

  //~ Constructors -----------------------------------------------------------

  /**
   * Returns the first type of typeName at or after position start (zero
   * based).
   *
   * @see #MatchReturnTypeInference(int, SqlTypeName[])
   */
  public MatchReturnTypeInference(int start, SqlTypeName... typeNames) {
    this(start, ImmutableList.copyOf(typeNames));
  }

  /**
   * Returns the first type matching any type in typeNames at or after
   * position start (zero based).
   */
  public MatchReturnTypeInference(int start, Iterable<SqlTypeName> typeNames) {
    Preconditions.checkArgument(start >= 0);
    this.start = start;
    this.typeNames = ImmutableList.copyOf(typeNames);
    Preconditions.checkArgument(!this.typeNames.isEmpty());
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    for (int i = start; i < opBinding.getOperandCount(); i++) {
      RelDataType argType = opBinding.getOperandType(i);
      if (SqlTypeUtil.isOfSameTypeName(typeNames, argType)) {
        return argType;
      }
    }
    return null;
  }
}

// End MatchReturnTypeInference.java
