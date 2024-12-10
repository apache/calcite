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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Contains static factory methods for creating instances of {@link SqlCallFactory}.
 */
public abstract class SqlCallFactories {
  private SqlCallFactories() {
  }

  /**
   * A {@link SqlCallFactory} that creates instances of {@link SqlBasicCall}.
   */
  public static final SqlCallFactory SQL_BASIC_CALL_FACTORY =
      (operator, functionQualifier, pos, operands) -> {
        pos = pos.plusAll(operands);
        return new SqlBasicCall(operator, ImmutableNullableList.copyOf(operands), pos,
            functionQualifier);
      };
}
