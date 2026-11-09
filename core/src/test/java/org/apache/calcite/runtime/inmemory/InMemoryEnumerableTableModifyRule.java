package org.apache.calcite.runtime.inmemory;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.schema.ModifiableTable;
import org.checkerframework.checker.nullness.qual.Nullable;

public class InMemoryEnumerableTableModifyRule extends ConverterRule {
  public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalTableModify.class, Convention.NONE,
          EnumerableConvention.INSTANCE, "InMemoryEnumerableTableModificationRule")
      .withRuleFactory(InMemoryEnumerableTableModifyRule::new);

    protected InMemoryEnumerableTableModifyRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
    final TableModify modify = (TableModify) rel;
    final ModifiableTable modifiableTable =
        modify.getTable().unwrap(ModifiableTable.class);
    if (modifiableTable == null) {
      return null;
    }
    final RelTraitSet traitSet =
        modify.getTraitSet().replace(EnumerableConvention.INSTANCE);
    return new InMemoryEnumerableTableModify(
        modify.getCluster(), traitSet,
        modify.getTable(),
        modify.getCatalogReader(),
        convert(modify.getInput(), traitSet),
        modify.getOperation(),
        modify.getUpdateColumnList(),
        modify.getSourceExpressionList(),
        modify.isFlattened());
  }
    
}
