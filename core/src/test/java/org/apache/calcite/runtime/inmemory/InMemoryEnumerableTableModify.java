package org.apache.calcite.runtime.inmemory;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.util.BuiltInMethod;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.lang.reflect.Type;

public class InMemoryEnumerableTableModify extends TableModify implements EnumerableRel {
    private final int rowCount;

    public InMemoryEnumerableTableModify(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, Operation operation, @Nullable List<String> updateColumnList, @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
        super(cluster, traits, table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened);
        assert child.getConvention() instanceof EnumerableConvention;
        assert getConvention() instanceof EnumerableConvention;
        final ModifiableTable modifiableTable = table.unwrap(ModifiableTable.class);
        if (modifiableTable == null) {
            throw new AssertionError();
        }
        // custom part
        RelDataType rowType = table.getRowType();
        rowCount = rowType.getFieldCount();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new InMemoryEnumerableTableModify(getCluster(), traitSet, getTable(), getCatalogReader(), sole(inputs), getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened());
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final Result result = implementor.visitChild(this, 0, (EnumerableRel)getInput(), pref);
        // collection parameter
        final ParameterExpression collectionParameter = Expressions.parameter(Collection.class, builder.newName("collection"));
        final Expression expression = table.getExpression(ModifiableTable.class);
        builder.add(Expressions.declare(Modifier.FINAL, collectionParameter, Expressions.call(expression, BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION.method)));
        final JavaTypeFactory typeFactory = (JavaTypeFactory)getCluster().getTypeFactory();
        final JavaRowFormat format = EnumerableTableScan.deduceFormat(table);
        PhysType physType_ = PhysTypeImpl.of(typeFactory, table.getRowType(), format);
        List<Expression> expressionList = new ArrayList<>();
        final PhysType childPhysType = result.physType;
        final ParameterExpression o_ = Expressions.parameter(childPhysType.getJavaRowType(), "o");
        for (int i = 0; i < rowCount; i++) {
            Type javaFieldType = physType_.getJavaFieldType(i);
            Expression fieldRef = childPhysType.fieldReference(o_, i, javaFieldType);
            expressionList.add(fieldRef);
        }
        // update column parameter
        List<String> updateColumnList = getUpdateColumnList();
        ConstantExpression updateColumnExpression = Expressions.constant(updateColumnList, List.class);
        Expression updateColumnListParameter = builder.append("updateColumnList", updateColumnExpression);
        // source expression parameter
        List<RexNode> rawSourceExpressionList = getSourceExpressionList();
        List<String> stringSourceExpressionList = rawSourceExpressionList.stream().map(element -> element.toString()).collect(Collectors.toList());
        ConstantExpression sourceExpressionExpression = Expressions.constant(stringSourceExpressionList, List.class);
        Expression sourceExpressionListParameter = builder.append("sourceExpressionList", sourceExpressionExpression);
        // update method statement
        try {
            Method func = InMemoryOperationHandler.class.getMethod("update", Collection.class, List.class, List.class);
            MethodCallExpression methodCallExpression = Expressions.call(func, collectionParameter, updateColumnListParameter, sourceExpressionListParameter);
            Statement statement = Expressions.statement(methodCallExpression);
            builder.add(statement);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // return statement
        builder.add(Expressions.return_(null, Expressions.call(BuiltInMethod.SINGLETON_ENUMERABLE.method, Expressions.constant(0, Integer.class))));
        // compile
        final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.ARRAY);
        return implementor.result(physType, builder.toBlock());
    }
}
