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

    @Override
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        return new GremlinToEnumerableConverter(
                getCluster(), traitSet, sole(inputs));
    }

    @Override
    public Result implement(final EnumerableRelImplementor implementor, final Prefer pref) {
        return null;
    }
}
