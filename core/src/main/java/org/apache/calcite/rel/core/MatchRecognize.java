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
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Relational expression that represent a match_recognize node
 *
 * <p>Each out put row has columns defined in the measure statments</p>
 */
public abstract class MatchRecognize extends SingleRel {
  //~ Instance fields ---------------------------------------------
  protected final ImmutableMap<String, RexNode> measures;
  protected final RexNode pattern;
  protected final boolean isStrictStarts;
  protected final boolean isStrictEnds;
  protected final ImmutableMap<String, RexNode> patternDefinitions;
  protected Set<RexMRAggCall> aggregateCalls = Sets.newTreeSet();

  //`~ Constructors -----------------------------------------------

  /**
   * create a MatchRecognize
   * @param cluster cluster
   * @param traits trait set
   * @param input input to MatchRecognize
   * @param pattern Regular Expression defining pattern variables
   * @param isStrictStarts Whether it is a strict start pattern
   * @param isStrictEnds Whether it is a strict end pattern
   * @param defns pattern definitions
   * @param rowType rowType
   */
  protected  MatchRecognize(
    RelOptCluster cluster,
    RelTraitSet traits,
    RelNode input,
    RexNode pattern,
    boolean isStrictStarts,
    boolean isStrictEnds,
    Map<String, RexNode> defns,
    RelDataType rowType) {
    super(cluster, traits, input);
    this.pattern = pattern;
    this.isStrictStarts = isStrictStarts;
    this.isStrictEnds = isStrictEnds;
    this.patternDefinitions = ImmutableMap.copyOf(defns);
    this.rowType = rowType;
    this.measures = ImmutableMap.of();
    assert defns.size() > 0;
    assert pattern != null;
    for (RexNode rex : patternDefinitions.values()) {
      parseAggregateCalls(rex);
    }

  }

  //~ Methods --------------------------------------------------
  private void parseAggregateCalls(RexNode rex) {
    if (!(rex instanceof RexCall)) {
      return;
    }
    new AggregateFinder().go((RexCall) rex);
  }

  public Set<RexMRAggCall> getAggregateCalls() {
    return aggregateCalls;
  }

  public ImmutableMap<String, RexNode> getMeasures() {
    return measures;
  }

  public RexNode getPattern() {
    return pattern;
  }

  public boolean isStrictStarts() {
    return isStrictStarts;
  }

  public boolean isStrictEnds() {
    return isStrictEnds;
  }

  public ImmutableMap<String, RexNode> getPatternDefinitions() {
    return patternDefinitions;
  }

  public abstract MatchRecognize copy(
    RelNode input,
    RexNode pattern,
    boolean isStrictStarts,
    boolean isStrictEnds,
    Map<String, RexNode> defns,
    RelDataType rowType);

  @Override public RelNode copy(
    RelTraitSet traitSet,
    List<RelNode> inputs) {
    if (getInputs().equals(inputs)
      && traitSet == getTraitSet()) {
      return this;
    }

    return copy(
      inputs.get(0),
      pattern,
      isStrictStarts,
      isStrictEnds,
      patternDefinitions,
      rowType);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (pw.nest()) {
      pw.item("fields", rowType.getFieldNames());
      pw.item("exprs", getMeasures().values().asList());
    } else {
      for (Ord<RelDataTypeField> field : Ord.zip(rowType.getFieldList())) {
        String fieldName = field.e.getName();
        if (fieldName == null) {
          fieldName = "Field#" + field.i;
        }
        pw.item(fieldName, getMeasures().get(field.i));
      }
    }
    return pw;
  }


  /**
   * Find aggregate functions in operands
   */
  private class AggregateFinder extends RexVisitorImpl {
    public AggregateFinder() {
      super(true);
    }

    @Override public Object visitCall(RexCall call) {
      SqlAggFunction aggFunction = null;
      switch (call.getKind()) {
      case SUM:
        aggFunction = new SqlSumAggFunction(call.getType());
        break;
      case SUM0:
        aggFunction = new SqlSumEmptyIsZeroAggFunction();
        break;
      case MAX:
      case MIN:
        aggFunction = new SqlMinMaxAggFunction(call.getKind());
        break;
      case COUNT:
        aggFunction = new SqlCountAggFunction();
        break;
      default:
        for (RexNode rex : call.getOperands()) {
          rex.accept(this);
        }
      }
      if (aggFunction != null) {
        RexMRAggCall aggCall = new RexMRAggCall(aggFunction,
          call.getType(), call.getOperands(), aggregateCalls.size());
        aggregateCalls.add(aggCall);
        Set<String> pv = new PatternVarFinder().go(call.getOperands());
      }
      return null;
    }

    public void go(RexCall call) {
      call.accept(this);
    }
  }

  /**
   * visit the operands of a aggregate call to retrieve relevant pattern variables
   */
  private class PatternVarFinder extends RexVisitorImpl {
    Set<String> patternVars;

    public PatternVarFinder() {
      super(true);
      patternVars = Sets.newHashSet();
    }

    @Override public Object visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      patternVars.add(fieldRef.getAlpha());
      return null;
    }

    @Override public Object visitCall(RexCall call) {
      for (RexNode node : call.getOperands()) {
        node.accept(this);
      }
      return null;
    }

    public Set<String> go(RexNode rex) {
      rex.accept(this);
      return patternVars;
    }

    public Set<String> go(List<RexNode> rexNodeList) {
      for (RexNode rex : rexNodeList) {
        rex.accept(this);
      }
      return patternVars;
    }
  }

  /**
   * agg calls in match recognize
   */
  public static class RexMRAggCall extends RexCall implements Comparable<RexMRAggCall> {
    public final int ordianl;
    public RexMRAggCall(
      SqlAggFunction aggFun,
      RelDataType type,
      List<RexNode> operands,
      int ordianl) {
      super(type, aggFun, operands);
      this.ordianl = ordianl;
      digest = computeDigest();
    }

    public String computeDigest() {
      return super.computeDigest(false);
    }

    @Override public int compareTo(RexMRAggCall o) {
      if (o.computeDigest() == null) {
        return 0;
      }

      if (computeDigest() == null) {
        return 1;
      }

      return o.computeDigest().compareTo(computeDigest());
    }
  }
}

// End MatchRecognize.java
