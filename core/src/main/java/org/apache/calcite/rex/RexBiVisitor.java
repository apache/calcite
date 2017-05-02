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
 * Visitor pattern for traversing a tree of {@link RexNode} objects
 * and passing a payload to each.
 *
 * @see RexVisitor
 *
 * @param <R> Return type
 * @param <P> Payload type
 */
public interface RexBiVisitor<R, P> {
  //~ Methods ----------------------------------------------------------------

  R visitInputRef(RexInputRef inputRef, P arg);

  R visitLocalRef(RexLocalRef localRef, P arg);

  R visitLiteral(RexLiteral literal, P arg);

  R visitCall(RexCall call, P arg);

  R visitOver(RexOver over, P arg);

  R visitCorrelVariable(RexCorrelVariable correlVariable, P arg);

  R visitDynamicParam(RexDynamicParam dynamicParam, P arg);

  R visitRangeRef(RexRangeRef rangeRef, P arg);

  R visitFieldAccess(RexFieldAccess fieldAccess, P arg);

  R visitSubQuery(RexSubQuery subQuery, P arg);

  R visitTableInputRef(RexTableInputRef ref, P arg);

  R visitPatternFieldRef(RexPatternFieldRef ref, P arg);
}

// End RexBiVisitor.java
