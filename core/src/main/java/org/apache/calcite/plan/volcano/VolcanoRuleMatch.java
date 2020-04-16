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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Litmus;

import java.util.List;
import java.util.Map;

/**
 * A match of a rule to a particular set of target relational expressions,
 * frozen in time.
 */
class VolcanoRuleMatch extends VolcanoRuleCall {
  //~ Instance fields --------------------------------------------------------

  private String digest;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>VolcanoRuleMatch</code>.
   *
   * @param operand0 Primary operand
   * @param rels     List of targets; copied by the constructor, so the client
   *                 can modify it later
   * @param nodeInputs Map from relational expressions to their inputs
   */
  VolcanoRuleMatch(VolcanoPlanner volcanoPlanner, RelOptRuleOperand operand0,
      RelNode[] rels, Map<RelNode, List<RelNode>> nodeInputs) {
    super(volcanoPlanner, operand0, rels.clone(), nodeInputs);
    assert allNotNull(rels, Litmus.THROW);

    digest = computeDigest();
  }

  //~ Methods ----------------------------------------------------------------

  public String toString() {
    return digest;
  }

  /**
   * Computes a string describing this rule match. Two rule matches are
   * equivalent if and only if their digests are the same.
   *
   * @return description of this rule match
   */
  private String computeDigest() {
    StringBuilder buf =
        new StringBuilder("rule [" + getRule() + "] rels [");
    for (int i = 0; i < rels.length; i++) {
      if (i > 0) {
        buf.append(',');
      }
      buf.append('#').append(rels[i].getId());
    }
    buf.append(']');
    return buf.toString();
  }

  /**
   * Recomputes the digest of this VolcanoRuleMatch.
   */
  @Deprecated // to be removed before 2.0
  public void recomputeDigest() {
    digest = computeDigest();
  }

  /** Returns whether all elements of a given array are not-null;
   * fails if any are null. */
  private static <E> boolean allNotNull(E[] es, Litmus litmus) {
    for (E e : es) {
      if (e == null) {
        return litmus.fail("was null", (Object) es);
      }
    }
    return litmus.succeed();
  }

}
