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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Visitor pattern for traversing a tree of {@link RexNode} objects.
 *
 * @see org.apache.calcite.util.Glossary#VISITOR_PATTERN
 * @see RexShuttle
 * @see RexVisitorImpl
 *
 * @param <R> Return type
 */
public interface RexVisitor<R> {
  //~ Methods ----------------------------------------------------------------

  R visitInputRef(RexInputRef inputRef);

  R visitLocalRef(RexLocalRef localRef);

  R visitLiteral(RexLiteral literal);

  R visitCall(RexCall call);

  R visitOver(RexOver over);

  R visitCorrelVariable(RexCorrelVariable correlVariable);

  R visitDynamicParam(RexDynamicParam dynamicParam);

  R visitRangeRef(RexRangeRef rangeRef);

  R visitFieldAccess(RexFieldAccess fieldAccess);

  R visitSubQuery(RexSubQuery subQuery);

  R visitTableInputRef(RexTableInputRef fieldRef);

  R visitPatternFieldRef(RexPatternFieldRef fieldRef);

  R visitLambda(RexLambda lambda);

  R visitLambdaRef(RexLambdaRef lambdaRef);

  R visitNodeAndFieldIndex(RexNodeAndFieldIndex nodeAndFieldIndex);

  /** Visits a list and writes the results to another list. */
  default void visitList(Iterable<? extends RexNode> exprs, List<R> out) {
    for (RexNode expr : exprs) {
      out.add(expr.accept(this));
    }
  }

  /** Visits a list and returns a list of the results.
   * The resulting list is immutable and does not contain nulls. */
  default List<R> visitList(Iterable<? extends RexNode> exprs) {
    final List<R> out = new ArrayList<>();
    visitList(exprs, out);
    return ImmutableList.copyOf(out);
  }

  /** Visits a list of expressions. */
  default void visitEach(Iterable<? extends RexNode> exprs) {
    for (RexNode expr : exprs) {
      expr.accept(this);
    }
  }
}
