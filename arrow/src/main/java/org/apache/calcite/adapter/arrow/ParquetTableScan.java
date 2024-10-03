package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

class ParquetTableScan extends TableScan implements ArrowRel {
  private final ParquetTable parquetTable;
  private final ImmutableIntList fields;

  ParquetTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable relOptTable, ParquetTable parquetTable, ImmutableIntList fields) {
    super(cluster, traitSet, ImmutableList.of(), relOptTable);
    this.parquetTable = parquetTable;
    this.fields = fields;

    assert getConvention() == ArrowRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    checkArgument(inputs.isEmpty());
    return this;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("fields", fields);
  }

  @Override public RelDataType deriveRowType() {
    final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
    final RelDataTypeFactory.Builder builder =
        getCluster().getTypeFactory().builder();
    for (int field : fields) {
      builder.add(fieldList.get(field));
    }
    return builder.build();
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(ArrowRules.TO_ENUMERABLE);
    for (RelOptRule rule : ArrowRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override public void implement(ArrowRel.Implementor implementor) {
    implementor.sourceTable = parquetTable;
    implementor.table = table;
  }
}
