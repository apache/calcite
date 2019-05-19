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
package org.apache.calcite.sql2rel;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

/** Converts an expression of a rex function call and its associated
 * result expression.
 */
public interface AuxiliaryConvertlet {
  /** Make a Rex to Rex conversion.
   *
   * @param rexBuilder Rex builder
   * @param call call to the rex function, e.g. "TUMBLE($2, 36000)"
   * @param resultExp Expression holding rex function call result, e.g. "$0"
   *
   * @return Expression for auxiliary function, e.g. "$0 + 36000" converts
   * the result of TUMBLE to the result of TUMBLE_END
   */
  RexNode convert(RexBuilder rexBuilder, RexNode call, RexNode resultExp);
}

// End AuxiliaryConvertlet.java
