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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * RelMdCollation supplies a default implementation of
 * {@link RelMetadataQuery#distribution}
 * for the standard logical algebra.
 */
public class RelMdDistribution {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.DISTRIBUTION.method, new RelMdDistribution());

  //~ Constructors -----------------------------------------------------------

  private RelMdDistribution() {}

  //~ Methods ----------------------------------------------------------------

  /** Fallback method to deduce distribution for any relational expression not
   * handled by a more specific method.
   *
   * @param rel Relational expression
   * @return Relational expression's distribution
   */
  public RelDistribution distribution(RelNode rel) {
    return RelDistributions.SINGLETON;
  }

  public RelDistribution distribution(SingleRel rel) {
    return RelMetadataQuery.distribution(rel.getInput());
  }

  public RelDistribution distribution(BiRel rel) {
    return RelMetadataQuery.distribution(rel.getLeft());
  }

  public RelDistribution distribution(SetOp rel) {
    return RelMetadataQuery.distribution(rel.getInputs().get(0));
  }

  public RelDistribution distribution(TableScan scan) {
    return table(scan.getTable());
  }

  public RelDistribution distribution(Project project) {
    return project(project.getInput(), project.getProjects());
  }

  public RelDistribution distribution(Values values) {
    return values(values.getRowType(), values.getTuples());
  }

  public RelDistribution distribution(Exchange exchange) {
    return exchange(exchange.distribution);
  }

  public RelDistribution distribution(HepRelVertex rel) {
    return RelMetadataQuery.distribution(rel.getCurrentRel());
  }

  // Helper methods

  /** Helper method to determine a
   * {@link TableScan}'s collation. */
  public static RelDistribution table(RelOptTable table) {
    return table.getDistribution();
  }

  /** Helper method to determine a
   * {@link Sort}'s distribution. */
  public static RelDistribution sort(RelNode input) {
    return RelMetadataQuery.distribution(input);
  }

  /** Helper method to determine a
   * {@link Filter}'s distribution. */
  public static RelDistribution filter(RelNode input) {
    return RelMetadataQuery.distribution(input);
  }

  /** Helper method to determine a
   * limit's distribution. */
  public static RelDistribution limit(RelNode input) {
    return RelMetadataQuery.distribution(input);
  }

  /** Helper method to determine a
   * {@link org.apache.calcite.rel.core.Calc}'s distribution. */
  public static RelDistribution calc(RelNode input,
      RexProgram program) {
    throw new AssertionError(); // TODO:
  }

  /** Helper method to determine a {@link Project}'s collation. */
  public static RelDistribution project(RelNode input,
      List<? extends RexNode> projects) {
    final RelDistribution inputDistribution =
        RelMetadataQuery.distribution(input);
    final Mappings.TargetMapping mapping =
        Project.getPartialMapping(input.getRowType().getFieldCount(),
                projects);
    return inputDistribution.apply(mapping);
  }

  /** Helper method to determine a
   * {@link Values}'s distribution. */
  public static RelDistribution values(RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples) {
    return RelDistributions.BROADCAST_DISTRIBUTED;
  }

  /** Helper method to determine an
   * {@link Exchange}'s
   * or {@link org.apache.calcite.rel.core.SortExchange}'s distribution. */
  public static RelDistribution exchange(RelDistribution distribution) {
    return distribution;
  }
}

// End RelMdDistribution.java
