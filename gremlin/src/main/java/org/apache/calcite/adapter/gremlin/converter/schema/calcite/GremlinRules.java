package org.apache.calcite.adapter.gremlin.converter.schema.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * List of rules that get pushed down and converted into GremlinTraversals.  Right now
 * only filter is pushed down using rules.  Joins are converted, but handled the by RelWalker
 * utilities.
 */
public class GremlinRules {
    public static final RelOptRule[] RULES = {
            GremlinFilterRule.INSTANCE
    };

    abstract static class GremlinConverterRule extends ConverterRule {
        private final Convention out;

        GremlinConverterRule(
                final Class<? extends RelNode> clazz,
                final RelTrait in,
                final Convention out,
                final String description) {
            super(clazz, in, out, description);
            this.out = out;
        }

        protected Convention getOut() {
            return out;
        }
    }

    private static final class GremlinFilterRule extends GremlinConverterRule {
        private static final GremlinFilterRule INSTANCE = new GremlinFilterRule();

        private GremlinFilterRule() {
            super(LogicalFilter.class, Convention.NONE, GremlinRel.CONVENTION, "GremlinFilterRule");
        }

        public RelNode convert(final RelNode rel) {
            final LogicalFilter filter = (LogicalFilter) rel;
            final RelTraitSet traitSet = filter.getTraitSet().replace(getOut());
            return new GremlinFilter(rel.getCluster(), traitSet, convert(filter.getInput(), getOut()),
                    filter.getCondition());
        }
    }

}
