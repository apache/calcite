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

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Default implementation of a {@link RexBiVisitor} whose payload and return
 * type are the same.
 *
 * @param <R> Return type from each {@code visitXxx} method
 */
public class RexUnaryBiVisitor<@Nullable R> extends RexBiVisitorImpl<R, R> {
  /** Creates a RexUnaryBiVisitor. */
  protected RexUnaryBiVisitor(boolean deep) {
    super(deep);
  }

  /** Called as the last action of, and providing the result for,
   * each {@code visitXxx} method; derived classes may override. */
  protected R end(RexNode e, R arg) {
    return arg;
  }

  @Override public R visitInputRef(RexInputRef inputRef, R arg) {
    return end(inputRef, arg);
  }

  @Override public R visitLocalRef(RexLocalRef localRef, R arg) {
    return end(localRef, arg);
  }

  @Override public R visitTableInputRef(RexTableInputRef ref, R arg) {
    return end(ref, arg);
  }

  @Override public R visitPatternFieldRef(RexPatternFieldRef fieldRef, R arg) {
    return end(fieldRef, arg);
  }

  @Override public R visitLiteral(RexLiteral literal, R arg) {
    return end(literal, arg);
  }

  @Override public R visitDynamicParam(RexDynamicParam dynamicParam, R arg) {
    return end(dynamicParam, arg);
  }

  @Override public R visitRangeRef(RexRangeRef rangeRef, R arg) {
    return end(rangeRef, arg);
  }

  @Override public R visitCorrelVariable(RexCorrelVariable correlVariable, R arg) {
    return end(correlVariable, arg);
  }

  @Override public R visitOver(RexOver over, R arg) {
    super.visitOver(over, arg);
    return end(over, arg);
  }

  @Override public R visitCall(RexCall call, R arg) {
    super.visitCall(call, arg);
    return end(call, arg);
  }

  @Override public R visitFieldAccess(RexFieldAccess fieldAccess, R arg) {
    super.visitFieldAccess(fieldAccess, arg);
    return end(fieldAccess, arg);
  }

  @Override public R visitSubQuery(RexSubQuery subQuery, R arg) {
    super.visitSubQuery(subQuery, arg);
    return end(subQuery, arg);
  }

  @Override public R visitNodeAndFieldIndex(RexNodeAndFieldIndex nodeAndFieldIndex, R arg) {
    return end(nodeAndFieldIndex, arg);
  }
}
