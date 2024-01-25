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

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

import java.util.List;

/**
 * Relational expression representing a scan of a table in a TinkerPop data source.
 */
public class GremlinToEnumerableConverter
        extends ConverterImpl
        implements EnumerableRel {
    protected GremlinToEnumerableConverter(
            final RelOptCluster cluster,
            final RelTraitSet traits,
            final RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return new GremlinToEnumerableConverter(
                getCluster(), traitSet, sole(inputs));
    }

    @Override public Result implement(final EnumerableRelImplementor implementor, final Prefer pref) {
        return null;
    }
}
