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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * A relational expression which computes project expressions and also filters.
 *
 * <p>This relational expression combines the functionality of
 * {@link ProjectRel} and {@link FilterRel}. It should be created in the later
 * stages of optimization, by merging consecutive {@link ProjectRel} and
 * {@link FilterRel} nodes together.
 *
 * <p>The following rules relate to <code>CalcRel</code>:</p>
 *
 * <ul>
 * <li>{@link FilterToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link ProjectToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link MergeFilterOntoCalcRule} merges this with a {@link FilterRel}</li>
 * <li>{@link MergeProjectOntoCalcRule} merges this with a
 *     {@link ProjectRel}</li>
 * <li>{@link MergeCalcRule} merges two CalcRels</li>
 * </ul>
 */
public final class CalcRel extends CalcRelBase {
  //~ Static fields/initializers ---------------------------------------------

  //~ Constructors -----------------------------------------------------------

  /** Creates a CalcRel. */
  public CalcRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelDataType rowType,
      RexProgram program,
      List<RelCollation> collationList) {
    super(cluster, traits, child, rowType, program, collationList);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public CalcRelBase copy(RelTraitSet traitSet, RelNode child,
      RexProgram program, List<RelCollation> collationList) {
    return new CalcRel(getCluster(), traitSet, child,
        program.getOutputRowType(), program, collationList);
  }

  @Override public void collectVariablesUsed(Set<String> variableSet) {
    final RelOptUtil.VariableUsedVisitor vuv =
        new RelOptUtil.VariableUsedVisitor();
    for (RexNode expr : program.getExprList()) {
      expr.accept(vuv);
    }
    variableSet.addAll(vuv.variables);
  }
}

// End CalcRel.java
