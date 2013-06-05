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

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Pair;

import java.util.*;

/**
 * Relational expression representing a scan of a MongoDB collection.
 *
 * <p> Additional operations might be applied,
 * using the "find" or "aggregate" methods.</p>
 */
public class MongoTableScan extends TableAccessRelBase implements MongoRel {
  final MongoTable mongoTable;
  final List<Pair<String, String>> ops;

  protected MongoTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, MongoTable mongoTable, RelDataType rowType,
      List<Pair<String, String>> ops) {
    super(cluster, traitSet, table);
    this.mongoTable = mongoTable;
    this.rowType = rowType;
    this.ops =
        Collections.unmodifiableList(new ArrayList<Pair<String, String>>(ops));

    assert mongoTable != null;
    assert rowType != null;
    assert getConvention() == MongoRel.CONVENTION;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new MongoTableScan(getCluster(), traitSet, table, mongoTable,
        rowType, ops);
  }

  @Override
  public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
    return super.explainTerms(pw)
        .item("ops", ops);
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(MongoToEnumerableConverterRule.INSTANCE);
  }

  public void implement(Implementor implementor) {
    implementor.table = mongoTable;
    for (Pair<String, String> op : ops) {
      implementor.add(op.left, op.right);
    }
  }
}

// End MongoTableScan.java
