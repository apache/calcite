package org.apache.calcite.adapter.graphql;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Converter class that converts a GraphQL query to an Enumerable relational expression.
 */
public class GraphQLToEnumerableConverter
    extends ConverterImpl
    implements EnumerableRel {

  private static final Logger LOGGER = LogManager.getLogger(GraphQLToEnumerableConverter.class);

  protected GraphQLToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new GraphQLToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return requireNonNull(super.computeSelfCost(planner, mq)).multiplyBy(.1);
  }

  /**
   * Implements the enumerable relational expression by generating Java code.
   *
   * @param relImplementor The EnumerableRelImplementor to implement the relational expression
   * @param prefer The method of how to generate the code
   *
   * @return Result containing the block statement, physType, and format of the generated Java code
   */
  @Override public Result implement(EnumerableRelImplementor relImplementor, Prefer prefer) {
    final BlockBuilder block = new BlockBuilder();
    final GraphQLRel.Implementor implementor = new GraphQLRel.Implementor();
    RelNode input = getInput();
    implementor.visitInput(0, input);
    final RelDataType rowType = getRowType();
    final PhysType physType =
        PhysTypeImpl.of(relImplementor.getTypeFactory(),
            rowType, prefer.prefer(JavaRowFormat.ARRAY));

    // Get the final GraphQL query with all operations
    String graphqlQuery = implementor.getQuery(rowType);
    LOGGER.debug("Generated GraphQL query: {}", graphqlQuery);

    assert implementor.table != null;
    final Expression table =
        block.append("table",
            requireNonNull(implementor.table.getExpression(GraphQLTable.class)));

    Expression enumerable =
        block.append("enumerable",
            Expressions.call(table,
                GraphQLMethod.GRAPHQL_QUERY.method,
                Expressions.constant(graphqlQuery)));

    LOGGER.debug("Generated enumerable expression: {}", enumerable);
    block.add(Expressions.return_(null, enumerable));
    return relImplementor.result(physType, block.toBlock());
  }
}
