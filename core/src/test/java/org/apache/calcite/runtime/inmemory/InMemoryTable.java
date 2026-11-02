package org.apache.calcite.runtime.inmemory;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableTableModifyRule;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.rel.convert.ConverterRule.Config;

public final class InMemoryTable<T> extends AbstractQueryableTable implements ModifiableTable, TranslatableTable {
    private final List<? extends T> data;

    public InMemoryTable(final Class<T> clazz, final List<? extends T> data) {
        super(clazz);
        this.data = data;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Queryable<T> asQueryable(final QueryProvider queryProvider, final SchemaPlus schema, final String tableName) {
        return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
            @Override
            public Enumerator<T> enumerator() {
                return Linq4j.enumerator(data);
            }
        };
    }

    @Override
    public final RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        return ((JavaTypeFactory)typeFactory).createType(elementType);
    }

    @Override
    public final List<? extends T> getModifiableCollection() {
        return data;
    }

    @Override
    public final TableModify toModificationRel(final RelOptCluster cluster, final RelOptTable table, final CatalogReader catalogReader, final RelNode child, final Operation operation, final List<String> updateColumnList, final List<RexNode> sourceExpressionList, final boolean flattened) {
        return new LogicalTableModify(cluster, cluster.traitSetOf(Convention.NONE), table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        // use custom table modify rule
        RelOptCluster cluster = context.getCluster();
        RelOptPlanner planner = cluster.getPlanner();
        List<RelOptRule> rules = planner.getRules();
        for (RelOptRule rule : rules) {
            if(rule instanceof EnumerableTableModifyRule){
                planner.removeRule(rule);
                Config config= InMemoryEnumerableTableModifyRule.DEFAULT_CONFIG;
                rule = new InMemoryEnumerableTableModifyRule(config);
                planner.addRule(rule);
            }
        }
        // default table scan
        EnumerableTableScan relNode = EnumerableTableScan.create(cluster, relOptTable);
        return relNode;
    }
}
