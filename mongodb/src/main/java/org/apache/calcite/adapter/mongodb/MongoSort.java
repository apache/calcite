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
package net.hydromatic.optiq.impl.mongodb;

import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SortRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
* Implementation of {@link SortRel} relational expression in MongoDB.
*/
public class MongoSortRel
    extends SortRel
    implements MongoRel {
  public MongoSortRel(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, child, collation, offset, fetch);
    assert getConvention() == MongoRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.05);
  }

  @Override public SortRel copy(RelTraitSet traitSet, RelNode input,
      RelCollation newCollation, RexNode offset, RexNode fetch) {
    return new MongoSortRel(getCluster(), traitSet, input, collation, offset,
        fetch);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getChild());
    if (!collation.getFieldCollations().isEmpty()) {
      final List<String> keys = new ArrayList<String>();
      final List<RelDataTypeField> fields = getRowType().getFieldList();
      for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
        final String name =
            fields.get(fieldCollation.getFieldIndex()).getName();
        keys.add(name + ": " + direction(fieldCollation));
        if (false) {
          // TODO: NULLS FIRST and NULLS LAST
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
    }
    if (offset != null) {
      implementor.add(null,
          "{$skip: " + ((RexLiteral) offset).getValue() + "}");
    }
    if (fetch != null) {
      implementor.add(null,
          "{$limit: " + ((RexLiteral) fetch).getValue() + "}");
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
