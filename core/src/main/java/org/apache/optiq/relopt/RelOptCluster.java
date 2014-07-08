/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.relopt;

import org.apache.optiq.rel.*;
import org.apache.optiq.rel.metadata.*;
import org.apache.optiq.reltype.*;
import org.apache.optiq.rex.*;

/**
 * A <code>RelOptCluster</code> is a collection of {@link RelNode relational
 * expressions} which have the same environment.
 *
 * <p>See the comment against <code>net.sf.saffron.oj.xlat.QueryInfo</code> on
 * why you should put fields in that class, not this one.</p>
 */
public class RelOptCluster {
  //~ Instance fields --------------------------------------------------------

  private final RelDataTypeFactory typeFactory;
  private final RelOptQuery query;
  private final RelOptPlanner planner;
  private RexNode originalExpression;
  private final RexBuilder rexBuilder;
  private RelMetadataProvider metadataProvider;
  private MetadataFactory metadataFactory;
  private final RelTraitSet emptyTraitSet;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a cluster.
   */
  RelOptCluster(
      RelOptQuery query,
      RelOptPlanner planner,
      RelDataTypeFactory typeFactory,
      RexBuilder rexBuilder) {
    assert planner != null;
    assert typeFactory != null;
    this.query = query;
    this.planner = planner;
    this.typeFactory = typeFactory;
    this.rexBuilder = rexBuilder;
    this.originalExpression = rexBuilder.makeLiteral("?");

    // set up a default rel metadata provider,
    // giving the planner first crack at everything
    setMetadataProvider(new DefaultRelMetadataProvider());
    this.emptyTraitSet = planner.emptyTraitSet();
  }

  //~ Methods ----------------------------------------------------------------

  public RelOptQuery getQuery() {
    return query;
  }

  public RexNode getOriginalExpression() {
    return originalExpression;
  }

  public void setOriginalExpression(RexNode originalExpression) {
    this.originalExpression = originalExpression;
  }

  public RelOptPlanner getPlanner() {
    return planner;
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public RexBuilder getRexBuilder() {
    return rexBuilder;
  }

  public RelMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * Overrides the default metadata provider for this cluster.
   *
   * @param metadataProvider custom provider
   */
  public void setMetadataProvider(RelMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
    this.metadataFactory = new MetadataFactoryImpl(metadataProvider);
  }

  public MetadataFactory getMetadataFactory() {
    return metadataFactory;
  }

  public RelTraitSet traitSetOf(RelTrait... traits) {
    RelTraitSet traitSet = emptyTraitSet;
    assert traitSet.size() == planner.getRelTraitDefs().size();
    for (RelTrait trait : traits) {
      traitSet = traitSet.replace(trait);
    }
    return traitSet;
  }
}

// End RelOptCluster.java
