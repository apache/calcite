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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.plan.InnerJoinTrait;
import org.apache.calcite.plan.InnerJoinTraitDef;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class for rel2sql package.
 */
public class RelToSqlUtils {

  private RelToSqlUtils() {
  }

  /**
   * Checks whether the filter can be a QUALIFY clause.
   * @param filter can be WHERE/HAVING/QUALIFY
   * @return true when filter contains a window function or an alias of an window function
   */
  public static boolean isQualifyFilter(Filter filter) {
    if (filter.containsOver()) {
      return true;
    }
    if (!(filter.getInput(0) instanceof Project)) {
      return false;
    }
    InputRefCollector finder = new InputRefCollector();
    filter.getCondition().accept(finder);
    Set<Integer> fieldsUsedInFilter = finder.getRefs();
    Project inputProject = (Project) filter.getInput();
    for (Integer ref : fieldsUsedInFilter) {
      RexNode referencedNode = inputProject.getProjects().get(ref);
      if (RexOver.containsOver(referencedNode)) {
        return true;
      }
    }
    return false;
  }

  protected static boolean isAnalyticalRex(RexNode rexNode) {
    if (rexNode instanceof RexOver) {
      return true;
    } else if (rexNode instanceof RexCall) {
      for (RexNode operand : ((RexCall) rexNode).getOperands()) {
        if (isAnalyticalRex(operand)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * This function checks whether InnerJoinTrait having preserveInnerJoin status as true exist in RelTraitSet or not.
   */
  public static boolean preserveInnerJoin(RelTraitSet relTraitSet) {
    RelTrait relTrait = relTraitSet.getTrait(InnerJoinTraitDef.instance);
    return relTrait != null && relTrait instanceof InnerJoinTrait
        && ((InnerJoinTrait) relTrait).isPreserveInnerJoin();
  }

  /**
   * RexVisitor to collect all the fields used in a Rex Expression.
   */
  private static class InputRefCollector extends RexVisitorImpl<Void> {
    Set<Integer> refs = new HashSet<>();

    protected InputRefCollector() {
      super(true);
    }

    @Override public Void visitInputRef(RexInputRef inputRef) {
      refs.add(inputRef.getIndex());
      return null;
    }

    public Set<Integer> getRefs() {
      return ImmutableSet.copyOf(refs);
    }
  }

  public static boolean findInputRef(RexNode node, List<Integer> refsToFind) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            @Override public Void visitInputRef(RexInputRef inputRef) {
              if (refsToFind.contains(inputRef.getIndex())) {
                throw new Util.FoundOne(inputRef);
              }
              return null;
            }
          };
      node.accept(visitor);
      return false;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return true;
    }
  }
}
