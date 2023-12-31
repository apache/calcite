package org.apache.calcite.adapter.gremlin.converter.schema.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

public class GremlinFilter extends Filter implements GremlinRel {
    public GremlinFilter(
            final RelOptCluster cluster,
            final RelTraitSet traitSet,
            final RelNode child,
            final RexNode condition) {
        super(cluster, traitSet, child, condition);
    }

    @Override
    public Filter copy(final RelTraitSet traitSet, final RelNode input, final RexNode condition) {
        return new GremlinFilter(getCluster(), traitSet, input, condition);
    }
}
