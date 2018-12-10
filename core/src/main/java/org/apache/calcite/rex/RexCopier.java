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

import org.apache.calcite.rel.type.RelDataType;

/**
 * Shuttle which creates a deep copy of a Rex expression.
 *
 * <p>This is useful when copying objects from one type factory or builder to
 * another.
 *
 * <p>Due to the laziness of the author, not all Rex types are supported at
 * present.
 *
 * @see RexBuilder#copy(RexNode)
 */
class RexCopier extends RexShuttle {
  //~ Instance fields --------------------------------------------------------

  private final RexBuilder builder;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RexCopier.
   *
   * @param builder Builder
   */
  RexCopier(RexBuilder builder) {
    this.builder = builder;
  }

  //~ Methods ----------------------------------------------------------------

  private RelDataType copy(RelDataType type) {
    return builder.getTypeFactory().copyType(type);
  }

  public RexNode visitOver(RexOver over) {
    throw new UnsupportedOperationException();
  }

  public RexWindow visitWindow(RexWindow window) {
    throw new UnsupportedOperationException();
  }

  public RexNode visitCall(final RexCall call) {
    final boolean[] update = null;
    return builder.makeCall(copy(call.getType()),
        call.getOperator(),
        visitList(call.getOperands(), update));
  }

  public RexNode visitCorrelVariable(RexCorrelVariable variable) {
    throw new UnsupportedOperationException();
  }

  public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
    return builder.makeFieldAccess(fieldAccess.getReferenceExpr().accept(this),
        fieldAccess.getField().getIndex());
  }

  public RexNode visitInputRef(RexInputRef inputRef) {
    return builder.makeInputRef(copy(inputRef.getType()), inputRef.getIndex());
  }

  public RexNode visitLocalRef(RexLocalRef localRef) {
    throw new UnsupportedOperationException();
  }

  public RexNode visitLiteral(RexLiteral literal) {
    // Get the value as is
    return new RexLiteral(RexLiteral.value(literal), copy(literal.getType()),
        literal.getTypeName());
  }

  public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
    return new RexDynamicParam(copy(dynamicParam.getType()), dynamicParam.getIndex());
  }

  public RexNode visitRangeRef(RexRangeRef rangeRef) {
    return new RexRangeRef(copy(rangeRef.getType()), rangeRef.getOffset());
  }
}

// End RexCopier.java
