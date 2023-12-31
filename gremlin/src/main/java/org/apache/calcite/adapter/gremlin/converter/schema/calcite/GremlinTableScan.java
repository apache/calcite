package org.apache.calcite.adapter.gremlin.converter.schema.calcite;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

public class GremlinTableScan extends TableScan implements GremlinRel {
    /**
     * Calling convention for relational operations that occur in Gremlin.
     */
    private final int[] fields;

    public GremlinTableScan(final RelOptCluster cluster, final RelTraitSet traitSet,
                            final RelOptTable table, final int[] fields) {
        super(cluster, traitSet, table);
        this.fields = fields;
    }

    @Override
    public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.FieldInfoBuilder builder =
                getCluster().getTypeFactory().builder();
        for (final int field : fields) {
            builder.add(fieldList.get(field));
        }
        return builder.build();
    }

    @Override
    public void register(final RelOptPlanner planner) {
        planner.addRule(GremlinToEnumerableConverterRule.INSTANCE);
        for (final RelOptRule rule : GremlinRules.RULES) {
            planner.addRule(rule);
        }
    }
}
