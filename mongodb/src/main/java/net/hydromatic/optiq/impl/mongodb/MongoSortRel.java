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
package net.hydromatic.optiq.impl.mongodb;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
* Implementation of {@link SortRel} relational expression in MongoDB.
*/
public class MongoSortRel
    extends SortRel
    implements MongoRel {
  public MongoSortRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RelCollation collation) {
    super(cluster, traitSet, child, collation);
    assert getConvention() == MongoRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }

  @Override
  public MongoSortRel copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation collation) {
    return new MongoSortRel(
        getCluster(),
        traitSet,
        newInput,
        collation);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getChild());
    final List<String> keys = new ArrayList<String>();
    for (int i = 0; i < collation.getFieldCollations().size(); i++) {
      final RelFieldCollation fieldCollation =
          collation.getFieldCollations().get(i);
      final String name =
          getRowType().getFieldList().get(i).getName();

      keys.add(name + ": " + direction(fieldCollation));
      if (false) {
        // TODO:
        switch (fieldCollation.nullDirection) {
        case FIRST:
          break;
        case LAST:
          break;
        }
      }
    }
    implementor.add(null,
        "{$sort: " + Util.toString(keys, "{", ", ", "}") + "}");
    if (fetch != null || offset != null) {
      // TODO: generate calls to DBCursor.skip() and limit(int).
      throw new UnsupportedOperationException();
    }
  }

  private int direction(RelFieldCollation fieldCollation) {
    switch (fieldCollation.getDirection()) {
    case DESCENDING:
    case STRICTLY_DESCENDING:
      return -1;
    case ASCENDING:
    case STRICTLY_ASCENDING:
    default:
      return 1;
    }
  }
}

// End MongoSortRel.java
