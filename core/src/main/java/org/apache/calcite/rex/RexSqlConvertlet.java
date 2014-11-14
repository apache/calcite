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
package org.apache.calcite.rex;

import org.apache.calcite.sql.SqlNode;

/**
 * Converts a {@link RexNode} expression into a {@link SqlNode} expression.
 */
public interface RexSqlConvertlet {
  //~ Methods ----------------------------------------------------------------

  /**
   * Converts a {@link RexCall} to a {@link SqlNode} expression.
   *
   * @param converter to use in translating
   * @param call      RexCall to translate
   * @return SqlNode, or null if translation was unavailable
   */
  SqlNode convertCall(
      RexToSqlNodeConverter converter,
      RexCall call);
}

// End RexSqlConvertlet.java
