package org.apache.calcite.adapter.gremlin.converter.schema.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * Rule to convert a relational expression from
 * {@link GremlinRel#CONVENTION} to {@link EnumerableConvention}.
 */
public final class GremlinToEnumerableConverterRule extends ConverterRule {
    public static final ConverterRule INSTANCE =
            new GremlinToEnumerableConverterRule();

    private GremlinToEnumerableConverterRule() {
        super(RelNode.class, GremlinRel.CONVENTION, EnumerableConvention.INSTANCE,"GremlinToEnumerableConverterRule");
    }

    @Override
    public RelNode convert(final RelNode rel) {
        final RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
        return new GremlinToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}
