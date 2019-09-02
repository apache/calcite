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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * FilterSelectivityEstimator implementation getSelectivity for TableScan for
 * {@link RelMdSelectivity}
 */
public class FilterSelectivityEstimator extends RexVisitorImpl<Double> {
  private final RelNode childRel;
  private final double  childCardinality;
  private final RelMetadataQuery mq;

  protected FilterSelectivityEstimator(RelNode childRel, RelMetadataQuery mq) {
    super(true);
    this.mq = mq;
    this.childRel = childRel;
    this.childCardinality = mq.getRowCount(childRel);
  }

  public Double estimateSelectivity(RexNode predicate) {
    return predicate.accept(this);
  }

  public Double visitCall(RexCall call) {
    if (!deep) {
      return 1.0;
    }

    Double selectivity = null;
    SqlKind op = call.getKind();

    switch (op) {
    case AND: {
      selectivity = computeConjunctionSelectivity(call);
      break;
    }

    case OR: {
      selectivity = computeDisjunctionSelectivity(call);
      break;
    }

    case NOT:
    case NOT_EQUALS: {
      selectivity = 1 - computeNotEqualitySelectivity(call);
      break;
    }

    case LESS_THAN_OR_EQUAL:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case GREATER_THAN: {
      selectivity = 0.5;
      break;
    }

    default:
      selectivity = computeFunctionSelectivity(call);
    }

    return selectivity;
  }


  /**
   * NDV of "f1(x, y, z) != f2(p, q, r)" is
   * "(maxNDV(x,y,z,p,q,r) - 1)/maxNDV(x,y,z,p,q,r)".
   */
  private Double computeNotEqualitySelectivity(RexCall call) {
    Double tmpNDV = getMaxNDV(call);
    if (tmpNDV == null) {
      return RelMdUtil.guessSelectivity(call);
    }

    if (tmpNDV > 1) {
      return 1 - 1 / tmpNDV;
    } else {
      return 1.0;
    }
  }

  /**
   * Selectivity of f(X,y,z) is 1/maxNDV(x,y,z).
   *
   * Note that GREATER, GREATER_THAN_OR_EQUAL, LESS, LESS_THAN_OR_EQUAL, EQUAL ...
   * are considered generic functions and uses this method to find their selectivity.
   */
  private Double computeFunctionSelectivity(RexCall call) {
    Double tmpNDV = getMaxNDV(call);
    if (tmpNDV == null) {
      return RelMdUtil.guessSelectivity(call);
    }
    return 1 / tmpNDV;
  }

  /**
   * Disjunction Selectivity is (1 D(1-m1/n)(1-m2/n)) where n is the total
   * number of tuples from child and m1 and m2 is the expected number of tuples
   * from each part of the disjunction predicate.
   *
   * Note we compute m1. m2.. by applying selectivity of the disjunctive element
   * on the cardinality from child.
   */
  private Double computeDisjunctionSelectivity(RexCall call) {
    Double tmpCardinality;
    Double tmpSelectivity;
    double selectivity = 1;

    for (RexNode dje : call.getOperands()) {
      tmpSelectivity = dje.accept(this);
      if (tmpSelectivity == null) {
        tmpSelectivity = 0.99;
      }
      tmpCardinality = childCardinality * tmpSelectivity;

      if (tmpCardinality > 1 && tmpCardinality < childCardinality) {
        tmpSelectivity = 1 - tmpCardinality / childCardinality;
      } else {
        tmpSelectivity = 1.0;
      }

      selectivity *= tmpSelectivity;
    }

    if (selectivity < 0.0) {
      selectivity = 0.0;
    }

    return 1 - selectivity;
  }

  /**
   * Selectivity of conjunctive predicate is (selectivity of conjunctive
   * element1) * (selectivity of conjunctive element2)...
   */
  private Double computeConjunctionSelectivity(RexCall call) {
    Double tmpSelectivity;
    double selectivity = 1;

    for (RexNode rexNode : call.getOperands()) {
      tmpSelectivity = rexNode.accept(this);
      if (tmpSelectivity != null) {
        selectivity *= tmpSelectivity;
      }
    }

    return selectivity;
  }

  private Double getMaxNDV(RexCall call) {
    Double tmpNDV;
    double maxNDV = 1.0;
    RelOptUtil.InputReferencedVisitor irv;
    for (RexNode op : call.getOperands()) {
      if (op instanceof RexInputRef) {
        tmpNDV = getDistinctRowCount(this.childRel, mq,
            ((RexInputRef) op).getIndex());
        if (tmpNDV == null) {
          return null;
        }
        if (tmpNDV > maxNDV) {
          maxNDV = tmpNDV;
        }
      } else {
        irv = new RelOptUtil.InputReferencedVisitor();
        irv.apply(op);
        for (Integer childProjIndx : irv.inputPosReferenced) {
          tmpNDV = getDistinctRowCount(this.childRel,
              mq, childProjIndx);
          if (tmpNDV == null) {
            return null;
          }
          if (tmpNDV > maxNDV) {
            maxNDV = tmpNDV;
          }
        }
      }
    }

    return maxNDV;
  }

  public static Double getDistinctRowCount(RelNode r, RelMetadataQuery mq, int indx) {
    ImmutableBitSet bitSetOfRqdProj = ImmutableBitSet.of(indx);
    return mq.getDistinctRowCount(r, bitSetOfRqdProj, r
        .getCluster().getRexBuilder().makeLiteral(true));
  }

  public Double visitLiteral(RexLiteral literal) {
    if (literal.isAlwaysFalse()) {
      return 0.0;
    } else if (literal.isAlwaysTrue()) {
      return 1.0;
    } else {
      throw new AssertionError("Must be true or false.");
    }
  }
}

// End FilterSelectivityEstimator.java
