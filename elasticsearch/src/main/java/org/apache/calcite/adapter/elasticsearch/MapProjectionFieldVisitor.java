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
package org.apache.calcite.adapter.elasticsearch;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Visitor that extracts the actual field name from an item expression.
 */
class MapProjectionFieldVisitor extends RexVisitorImpl<String> {

  static final MapProjectionFieldVisitor INSTANCE = new MapProjectionFieldVisitor();

  private MapProjectionFieldVisitor() {
    super(true);
  }

  @Override public String visitCall(RexCall call) {
    if (call.op == SqlStdOperatorTable.ITEM) {
      return ((RexLiteral) call.getOperands().get(1)).getValueAs(String.class);
    }
    return super.visitCall(call);
  }
}

// End MapProjectionFieldVisitor.java
