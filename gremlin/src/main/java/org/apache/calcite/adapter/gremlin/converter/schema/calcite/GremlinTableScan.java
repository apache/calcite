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
package org.apache.calcite.adapter.gremlin.converter.schema.calcite;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class GremlinTableScan extends TableScan implements GremlinRel {
    /**
     * Calling convention for relational operations that occur in Gremlin.
     */
    private final int[] fields;

    public GremlinTableScan(final RelOptCluster cluster, final RelTraitSet traitSet,
                            final RelOptTable table, final int[] fields) {
        super(cluster, traitSet, ImmutableList.of(), table);
        this.fields = fields;
    }

    @Override public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder =
                getCluster().getTypeFactory().builder();
        for (final int field : fields) {
            builder.add(fieldList.get(field));
        }
        return builder.build();
    }

    @Override public void register(final RelOptPlanner planner) {
        planner.addRule(GremlinToEnumerableConverterRule.INSTANCE);
        for (final RelOptRule rule : GremlinRules.RULES) {
            planner.addRule(rule);
        }
    }
}
