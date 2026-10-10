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
package org.apache.calcite.adapter.milvus.convention;

import org.apache.calcite.adapter.milvus.factory.MilvusTranslatableTable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Map;

/**
 * Relational expression that uses Milvus calling convention.
 */
public interface MilvusRel extends RelNode {
  void implement(Implementor implementor);

  Convention CONVENTION = new Convention.Impl("MILVUS", MilvusRel.class);

  /**
   * Implementor for Milvus relational expressions.
   */
  class Implementor {
    public final RexBuilder rexBuilder;

    // scan
    public RelOptTable table;
    public MilvusTranslatableTable milvusTable;
    public RelDataType rowType;
    public Map<String, String> milvusOptions;

    // filter
    public RexNode filterCondition;

    // project
    public RelDataType projectRowType;
    public List<RexNode> projects;

    // vector search
    public RexNode vectorDistanceExpr;
    public Integer vectorDistanceFieldIndex;
    public RelFieldCollation.Direction sortOrder;
    public RexNode limit;

    public Implementor(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;

      RelNode node = findMilvusRel(input);

      if (!(node instanceof MilvusRel)) {
        throw new IllegalStateException(
            "Expected MilvusRel input but got "
                + (node == null ? "null" : node.getClass().getName())
                + " (original=" + input.getClass().getName() + ")");
      }
      ((MilvusRel) node).implement(this);
    }
  }

  static RelNode findMilvusRel(RelNode input) {
    RelNode node = input;
    if (node instanceof RelSubset) {
      final RelSubset subset = (RelSubset) node;
      RelNode best = subset.getBest();
      if (best != null) {
        node = best;
      } else {
        // find first MilvusRel in the subset
        for (RelNode r : subset.getRelList()) {
          if (r instanceof MilvusRel) {
            node = r;
            break;
          }
        }
      }
    }
    return node;
  }
}
