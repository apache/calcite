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
package org.eigenbase.sarg;

import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;

/**
 * SargFactory creates new instances of various sarg-related objects.
 */
public class SargFactory {
  //~ Instance fields --------------------------------------------------------

  private final RexBuilder rexBuilder;

  private final RexNode rexNull;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new SargFactory.
   *
   * @param rexBuilder factory for instances of {@link RexNode}, needed
   *                   internally in the sarg representation, and also for
   *                   recomposing sargs into equivalent rex trees
   */
  public SargFactory(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
    rexNull = rexBuilder.constantNull();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a new endpoint. Initially, the endpoint represents a lower bound
   * of negative infinity.
   *
   * @param dataType datatype for domain
   * @return new endpoint
   */
  public SargMutableEndpoint newEndpoint(RelDataType dataType) {
    return new SargMutableEndpoint(this, dataType);
  }

  /**
   * Creates a new interval expression. The interval starts out as unbounded
   * (meaning it includes every non-null value of the datatype), with
   * SqlNullSemantics.NULL_MATCHES_NOTHING.
   *
   * @param dataType datatype for domain
   */
  public SargIntervalExpr newIntervalExpr(RelDataType dataType) {
    return newIntervalExpr(
        dataType,
        SqlNullSemantics.NULL_MATCHES_NOTHING);
  }

  /**
   * Creates a new unbounded interval expression with non-default null
   * semantics.
   *
   * @param dataType      datatype for domain
   * @param nullSemantics null semantics governing searches on this interval
   */
  public SargIntervalExpr newIntervalExpr(
      RelDataType dataType,
      SqlNullSemantics nullSemantics) {
    return new SargIntervalExpr(
        this,
        dataType,
        nullSemantics);
  }

  /**
   * Creates a new set expression, initially with no children.
   *
   * @param dataType datatype for domain
   * @param setOp    set operator
   */
  public SargSetExpr newSetExpr(RelDataType dataType, SargSetOperator setOp) {
    return new SargSetExpr(this, dataType, setOp);
  }

  /**
   * @return new analyzer for rex expressions
   */
  public SargRexAnalyzer newRexAnalyzer() {
    return new SargRexAnalyzer(this, false);
  }

  /**
   * @param simpleMode if true, the analyzer restricts the types of predicates
   *                   it allows; the following are disallowed - conjuntions on
   *                   the same RexInputRef, more than one range predicate, and
   *                   all disjunctions
   * @return new analyzer for rex expressions
   */
  public SargRexAnalyzer newRexAnalyzer(boolean simpleMode) {
    return new SargRexAnalyzer(this, simpleMode);
  }

  /**
   * @param lowerRexInputIdx if &ge; 0, treat RexInputRefs whose index is within
   *                         the range [lowerRexInputIdx, upperRexInputIdx) as
   *                         coordinates in expressions
   * @param upperRexInputIdx if &ge; 0, treat RexInputRefs whose index is within
   *                         the range [lowerRexInputIdx, upperRexInputIdx) as
   *                         coordinates in expressions
   * @return new analyzer for rex expressions
   */
  public SargRexAnalyzer newRexAnalyzer(
      int lowerRexInputIdx,
      int upperRexInputIdx) {
    return new SargRexAnalyzer(
        this,
        true,
        lowerRexInputIdx,
        upperRexInputIdx);
  }

  /**
   * @return the null literal, which can be used to represent a range matching
   * the null value
   */
  public RexNode newNullLiteral() {
    return rexNull;
  }

  /**
   * @return RexBuilder used by this factory
   */
  public RexBuilder getRexBuilder() {
    return rexBuilder;
  }
}

// End SargFactory.java
