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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import com.google.common.collect.ImmutableList;

/**
 * ElasticSearch can't implement {@code Filter(Project(...))} because Filter must reference
 * the original field names.
 * The only allowed place for {@code RexInputRef} is {@code ITEM($0, ...)} calls.
 */
class InputRefVerifier extends RexVisitorImpl<Object> {
  public boolean hasInvalidInput;
  protected InputRefVerifier() {
    super(true);
  }

  @Override public Object visitCall(RexCall call) {
    if (hasInvalidInput) {
      return null;
    }
    if (call.getKind() == SqlKind.ITEM) {
      ImmutableList<RexNode> operands = call.operands;
      for (int i = 1; i < operands.size(); i++) {
        RexNode op = operands.get(i);
        op.accept(this);
      }
      return null;
    }
    return super.visitCall(call);
  }

  @Override public Boolean visitInputRef(RexInputRef inputRef) {
    hasInvalidInput = true;
    return null;
  }
}
