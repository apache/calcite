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
package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOverOperator;
import org.apache.calcite.sql.util.SqlBasicVisitor;

/**
 * Visitor which throws an exception if the expression does not contain a windowed aggregation.
 */
class WindowedAggChecker extends SqlBasicVisitor<Void> {
  //~ Instance fields --------------------------------------------------------

  private boolean seenWindow;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an WindowedAggChecker.
   */
  WindowedAggChecker() {
    seenWindow = false;
  }

  //~ Methods ----------------------------------------------------------------

  private static boolean isWindowExpr(SqlCall expr) {
    return expr.getOperator() instanceof SqlOverOperator;
  }

  @Override public Void visit(SqlCall call) {
    this.seenWindow |= isWindowExpr(call);
    if (!this.seenWindow) {
      super.visit(call);
    }
    return null;
  }

  public Boolean sawWindow() {
    return this.seenWindow;
  }
}
