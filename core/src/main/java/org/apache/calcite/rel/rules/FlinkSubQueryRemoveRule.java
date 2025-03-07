package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.immutables.value.Value;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Planner rule that converts IN and EXISTS into semi-join, converts NOT IN and NOT EXISTS into
 * anti-join.
 *
 * <p>Sub-queries are represented by [[RexSubQuery]] expressions.
 *
 * <p>A sub-query may or may not be correlated. If a sub-query is correlated, the wrapped
 * [[RelNode]] will contain a [[RexCorrelVariable]] before the rewrite, and the product of the
 * rewrite will be a [[org.apache.calcite.rel.core.Join]] with SEMI or ANTI join type.
 */
@Value.Enclosing
public class FlinkSubQueryRemoveRule extends RelRule<FlinkSubQueryRemoveRule.Config> implements TransformationRule {

    public FlinkSubQueryRemoveRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        RexNode condition = filter.getCondition();

        if (hasUnsupportedSubQuery(condition)) {
            // has some unsupported subquery, such as: subquery connected with OR
            // select * from t1 where t1.a > 10 or t1.b in (select t2.c from t2)
            // TODO supports ExistenceJoin
            return;
        }

        Optional<RexCall> subQueryCall = findSubQuery(condition);
        if (subQueryCall.isEmpty()) {
            // ignore scalar query
            return;
        }

        SubQueryDecorrelator.Result decorrelate = SubQueryDecorrelator.decorrelateQuery(filter);
        if (decorrelate == null) {
            // can't handle the query
            return;
        }

        RelBuilder relBuilder = call.builder();
        relBuilder.push(filter.getInput()); // push join left

        Optional<RexNode> newCondition = handleSubQuery(subQueryCall.get(), condition, relBuilder, decorrelate);
        newCondition.ifPresent(c -> {
            if (hasCorrelatedExpressions(c)) {
                // some correlated expressions can not be replaced in this rule,
                // so we must keep the VariablesSet for decorrelating later in new filter
                // RelBuilder.filter can not create Filter with VariablesSet arg
                Filter newFilter = filter.copy(filter.getTraitSet(), relBuilder.build(), c);
                relBuilder.push(newFilter);
            } else {
                // all correlated expressions are replaced,
                // so we can create a new filter without any VariablesSet
                relBuilder.filter(c);
            }
            relBuilder.project(fields(relBuilder, filter.getRowType().getFieldCount()));
            // the sub query has been replaced with a common node,
            // so hints in it should also be resolved with the same logic in SqlToRelConverter
            RelNode newNode = relBuilder.build();
            RelNode nodeWithHint = RelOptUtil.propagateRelHints(newNode, false);
            // RelNode nodeWithCapitalizedJoinHints = FlinkHints.capitalizeJoinHints(nodeWithHint);
            // RelNode finalNode =
            // nodeWithCapitalizedJoinHints.accept(new ClearJoinHintWithInvalidPropagationShuttle());
            call.transformTo(newNode);
        });
    }

    private Optional<RexNode> handleSubQuery(
        RexCall subQueryCall,
        RexNode condition,
        RelBuilder relBuilder,
        SubQueryDecorrelator.Result decorrelate
    ) {
        RelOptUtil.Logic logic = LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(condition), subQueryCall);
        if (logic != RelOptUtil.Logic.TRUE) {
            // this should not happen, none unsupported SubQuery could not reach here
            // this is just for double-check
            return Optional.empty();
        }

        Optional<RexNode> target = apply(subQueryCall, relBuilder, decorrelate);
        if (!target.isPresent()) {
            return Optional.empty();
        }

        RexNode newCondition = replaceSubQuery(condition, subQueryCall, target.get());
        Optional<RexCall> nextSubQueryCall = findSubQuery(newCondition);
        return nextSubQueryCall.map(subQuery -> handleSubQuery(subQuery, newCondition, relBuilder, decorrelate))
            .orElse(Optional.of(newCondition));
    }

    private Optional<RexNode> apply(RexCall subQueryCall, RelBuilder relBuilder, SubQueryDecorrelator.Result decorrelate) {

        RexSubQuery subQuery;
        boolean withNot = false;
        if (subQueryCall instanceof RexSubQuery) {
            subQuery = (RexSubQuery) subQueryCall;
        } else if (subQueryCall.getOperands().get(0) instanceof RexSubQuery) {
            subQuery = (RexSubQuery) subQueryCall.getOperands().get(0);
            withNot = subQueryCall.getKind() == SqlKind.NOT;
        } else {
            return Optional.empty();
        }

        Pair<RelNode, RexNode> equivalent = decorrelate.getSubQueryEquivalent(subQuery);

        switch (subQuery.getKind()) {
            case IN:
                return handleInSubQuery(subQuery, withNot, equivalent, relBuilder);
            case EXISTS:
                return handleExistsSubQuery(subQuery, withNot, equivalent, relBuilder);
            default:
                return Optional.empty();
        }
    }

    private Optional<RexNode> handleInSubQuery(
        RexSubQuery subQuery,
        boolean withNot,
        Pair<RelNode, RexNode> equivalent,
        RelBuilder relBuilder
    ) {
        // Implement the logic for IN and NOT IN subqueries
        RelNode newRight = equivalent != null ? equivalent.getKey() : subQuery.rel;
        Optional<RexNode> joinCondition = equivalent != null ? Optional.of(equivalent.getValue()) : Optional.empty();

        Pair<List<RexNode>, Optional<RexNode>> result = handleSubQueryOperands(subQuery, joinCondition, relBuilder);
        List<RexNode> newOperands = result.getKey();
        Optional<RexNode> newCondition = result.getValue();
        int leftFieldCount = relBuilder.peek().getRowType().getFieldCount();

        relBuilder.push(newRight); // push join right

        List<RexNode> joinConditions = new ArrayList<>();
        for (int i = 0; i < newOperands.size(); i++) {
            RexNode op = newOperands.get(i);
            RexNode f = relBuilder.field(i + leftFieldCount);
            RexNode inCondition = relBuilder.equals(op, f);
            if (withNot) {
                joinConditions.add(relBuilder.or(inCondition, relBuilder.isNull(inCondition)));
            } else {
                joinConditions.add(inCondition);
            }
        }
        newCondition.ifPresent(joinConditions::add);

        if (withNot) {
            relBuilder.join(JoinRelType.ANTI, joinConditions);
        } else {
            relBuilder.join(JoinRelType.SEMI, joinConditions);
        }
        return Optional.of(relBuilder.literal(true));
    }

    private Optional<RexNode> handleExistsSubQuery(
        RexSubQuery subQuery,
        boolean withNot,
        Pair<RelNode, RexNode> equivalent,
        RelBuilder relBuilder
    ) {
        RexNode joinCondition;
        if (equivalent != null) {
            // EXISTS has correlation variables
            relBuilder.push(equivalent.getKey()); // push join right
            joinCondition = equivalent.getValue();
        } else {
            // Implement the logic for EXISTS and NOT EXISTS subqueries
            int leftFieldCount = relBuilder.peek().getRowType().getFieldCount();
            relBuilder.push(subQuery.rel); // push join right
            relBuilder.project(relBuilder.alias(relBuilder.literal(true), "i"));
            relBuilder.aggregate(relBuilder.groupKey(), relBuilder.min("m", relBuilder.field(0)));
            relBuilder.project(relBuilder.isNotNull(relBuilder.field(0)));
            joinCondition = new RexInputRef(leftFieldCount, relBuilder.peek().getRowType().getFieldList().get(0).getType());
        }

        if (withNot) {
            relBuilder.join(JoinRelType.ANTI, joinCondition);
        } else {
            relBuilder.join(JoinRelType.SEMI, joinCondition);
        }
        return Optional.of(relBuilder.literal(true));
    }

    private List<RexNode> fields(RelBuilder builder, int fieldCount) {
        List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            projects.add(builder.field(i));
        }
        return projects;
    }

    private boolean isScalarQuery(RexNode n) {
        return n.getKind() == SqlKind.SCALAR_QUERY;
    }

    private Optional<RexCall> findSubQuery(RexNode node) {
        try {
            node.accept(new RexVisitorImpl<Void>(true) {

                @Override
                public Void visitSubQuery(RexSubQuery subQuery) {
                    if (!isScalarQuery(subQuery)) {
                        throw new Util.FoundOne(subQuery);
                    }
                    return null;
                }

                @Override
                public Void visitCall(RexCall call) {
                    if (call.getKind() == SqlKind.NOT && call.getOperands().get(0) instanceof RexSubQuery) {
                        if (!isScalarQuery(call.getOperands().get(0))) {
                            throw new Util.FoundOne(call);
                        }
                    }
                    return super.visitCall(call);
                }
            });
            return Optional.empty();
        } catch (Util.FoundOne e) {
            return Optional.of((RexCall) e.getNode());
        }
    }

    private RexNode replaceSubQuery(RexNode condition, RexCall oldSubQueryCall, RexNode replacement) {
        return condition.accept(new RexShuttle() {

            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                if (oldSubQueryCall.equals(subQuery)) {
                    return replacement;
                }
                return subQuery;
            }

            @Override
            public RexNode visitCall(RexCall call) {
                if (call.getKind() == SqlKind.NOT && call.getOperands().get(0) instanceof RexSubQuery) {
                    if (oldSubQueryCall.equals(call)) {
                        return replacement;
                    }
                }
                return super.visitCall(call);
            }
        });
    }

    /**
     * Adds projection if the operands of a SubQuery contains non-RexInputRef nodes, and returns
     * SubQuery's new operands and new join condition with new index.
     *
     * e.g. SELECT * FROM l WHERE a + 1 IN (SELECT c FROM r) We will add projection as SEMI join left
     * input, the added projection will pass along fields from the input, and add `a + 1` as new
     * field.
     */
    private Pair<List<RexNode>, Optional<RexNode>> handleSubQueryOperands(
        RexSubQuery subQuery,
        Optional<RexNode> joinCondition,
        RelBuilder relBuilder
    ) {
        List<RexNode> operands = subQuery.getOperands();
        // operands is empty or all operands are RexInputRef
        if (operands.isEmpty() || operands.stream().allMatch(o -> o instanceof RexInputRef)) {
            return new Pair<>(operands, joinCondition);
        }

        RexBuilder rexBuilder = relBuilder.getRexBuilder();
        RelNode oldLeftNode = relBuilder.peek();
        int oldLeftFieldCount = oldLeftNode.getRowType().getFieldCount();
        List<RexNode> newLeftProjects = new ArrayList<>();
        List<Integer> newOperandIndices = new ArrayList<>();
        for (int i = 0; i < oldLeftFieldCount; i++) {
            newLeftProjects.add(rexBuilder.makeInputRef(oldLeftNode, i));
        }
        for (RexNode o : operands) {
            int index = newLeftProjects.indexOf(o);
            if (index < 0) {
                index = newLeftProjects.size();
                newLeftProjects.add(o);
            }
            newOperandIndices.add(index);
        }

        // adjust join condition after adds new projection
        Optional<RexNode> newJoinCondition = joinCondition.map(jc -> {
            int offset = newLeftProjects.size() - oldLeftFieldCount;
            return RexUtil.shift(jc, oldLeftFieldCount, offset);
        });

        relBuilder.project(newLeftProjects); // push new join left
        List<RexNode> newOperands = newOperandIndices.stream()
            .map(index -> rexBuilder.makeInputRef(relBuilder.peek(), index))
            .collect(Collectors.toList());

        return new Pair<>(newOperands, newJoinCondition);
    }

    private boolean hasUnsupportedSubQuery(RexNode condition) {
        try {
            condition.accept(new RexVisitorImpl<Void>(true) {

                Deque<SqlKind> stack = new ArrayDeque<>();

                private void checkAndConjunctions(RexCall call) {
                    if (stack.stream().anyMatch(kind -> kind != SqlKind.AND)) {
                        throw new Util.FoundOne(call);
                    }
                }

                @Override
                public Void visitSubQuery(RexSubQuery subQuery) {
                    if (!isScalarQuery(subQuery)) {
                        checkAndConjunctions(subQuery);
                    }
                    return null;
                }

                @Override
                public Void visitCall(RexCall call) {
                    switch (call.getKind()) {
                        case NOT:
                            if (call.getOperands().get(0) instanceof RexSubQuery) {
                                // ignore scalar query
                                if (!isScalarQuery(call.getOperands().get(0))) {
                                    checkAndConjunctions(call);
                                }
                            }
                            break;
                        default:
                            stack.push(call.getKind());
                            super.visitCall(call);
                            stack.pop();
                    }
                    return null;
                }
            });
            return false;
        } catch (Util.FoundOne e) {
            return true;
        }
    }

    private boolean hasCorrelatedExpressions(RexNode... nodes) {
        // Implementation needed
        return false;
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {

        Config FILTER = ImmutableFlinkSubQueryRemoveRule.Config.of()
            .withOperandSupplier(b -> b.operand(Filter.class).predicate(RexUtil.SubQueryFinder::containsSubQuery).anyInputs());

        @Override
        default FlinkSubQueryRemoveRule toRule() {
            return new FlinkSubQueryRemoveRule(this);
        }
    }
}
