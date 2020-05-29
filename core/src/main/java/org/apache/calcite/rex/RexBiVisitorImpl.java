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

/**
 * Default implementation of {@link RexBiVisitor}, which visits each node but
 * does nothing while it's there.
 *
 * @param <R> Return type from each {@code visitXxx} method
 * @param <P> Payload type
 */
public class RexBiVisitorImpl<R, P> implements RexBiVisitor<R, P> {
  //~ Instance fields --------------------------------------------------------

  protected final boolean deep;

  //~ Constructors -----------------------------------------------------------

  protected RexBiVisitorImpl(boolean deep) {
    this.deep = deep;
  }

  //~ Methods ----------------------------------------------------------------

  public R visitInputRef(RexInputRef inputRef, P arg) {
    return null;
  }

  public R visitLocalRef(RexLocalRef localRef, P arg) {
    return null;
  }

  public R visitLiteral(RexLiteral literal, P arg) {
    return null;
  }

  public R visitOver(RexOver over, P arg) {
    R r = visitCall(over, arg);
    if (!deep) {
      return null;
    }
    final RexWindow window = over.getWindow();
    for (RexFieldCollation orderKey : window.orderKeys) {
      orderKey.left.accept(this, arg);
    }
    for (RexNode partitionKey : window.partitionKeys) {
      partitionKey.accept(this, arg);
    }
    window.getLowerBound().accept(this, arg);
    window.getUpperBound().accept(this, arg);
    return r;
  }

  public R visitCorrelVariable(RexCorrelVariable correlVariable, P arg) {
    return null;
  }

  public R visitCall(RexCall call, P arg) {
    if (!deep) {
      return null;
    }

    R r = null;
    for (RexNode operand : call.operands) {
      r = operand.accept(this, arg);
    }
    return r;
  }

  public R visitDynamicParam(RexDynamicParam dynamicParam, P arg) {
    return null;
  }

  public R visitRangeRef(RexRangeRef rangeRef, P arg) {
    return null;
  }

  public R visitFieldAccess(RexFieldAccess fieldAccess, P arg) {
    if (!deep) {
      return null;
    }
    final RexNode expr = fieldAccess.getReferenceExpr();
    return expr.accept(this, arg);
  }

  public R visitSubQuery(RexSubQuery subQuery, P arg) {
    if (!deep) {
      return null;
    }

    R r = null;
    for (RexNode operand : subQuery.operands) {
      r = operand.accept(this, arg);
    }
    return r;
  }

  @Override public R visitTableInputRef(RexTableInputRef ref, P arg) {
    return null;
  }

  @Override public R visitPatternFieldRef(RexPatternFieldRef fieldRef, P arg) {
    return null;
  }
}
