/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.relopt;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexShuttle;
import org.eigenbase.util.Pair;

import com.google.common.collect.ImmutableList;

/**
 * Substitutes part of a tree of relational expressions with another tree.
 *
 * <p>The call {@code new SubstitutionVisitor(find, query).go(replacement))
 * will return {@code query} with every occurrence of {@code find} replaced
 * by {@code replacement}.</p>
 *
 * <p>The following example shows how {@code SubstitutionVisitor} can be used
 * for materialized view recognition.</p>
 *
 * <ul>
 *     <li>query = SELECT a, c FROM t WHERE x = 5 AND b = 4</li>
 *     <li>find = SELECT a, b, c FROM t WHERE x = 5</li>
 *     <li>replacement = SELECT * FROM mv</li>
 *     <li>result = SELECT a, c FROM mv WHERE b = 4</li>
 * </ul>
 *
 * <p>Note that {@code result} uses the materialized view table {@code mv} and a
 * simplified condition {@code b = 4}.</p>
 *
 * <p>Uses a bottom-up matching algorithm. Nodes do not need to be identical.
 * At each level, returns the residue.</p>
 *
 * <p>The inputs must only include the core relational operators:
 * {@link org.eigenbase.rel.TableAccessRel},
 * {@link org.eigenbase.rel.FilterRel},
 * {@link org.eigenbase.rel.ProjectRel},
 * {@link org.eigenbase.rel.JoinRel},
 * {@link org.eigenbase.rel.UnionRel},
 * {@link org.eigenbase.rel.AggregateRel}.</p>
 */
public class SubstitutionVisitor {
    private final RelNode query;
    private final RelNode find;

    /** Map from each node in the query and the materialization query
     * to its parent. */
    final Map<RelNode, Pair<RelNode, Integer>> parentMap =
        new IdentityHashMap<RelNode, Pair<RelNode, Integer>>();

    /** Nodes in {@link #find} that have no children. */
    final List<RelNode> findLeaves;

    /** Nodes in {@link #query} that have no children. */
    final List<RelNode> queryLeaves;

    /** Map from leaves in {@link #find} to leaves in {@link #query}. */
    final Map<RelNode, RelNode> leafMap =
        new IdentityHashMap<RelNode, RelNode>();

    final Map<RelNode, RelNode> replacementMap =
        new HashMap<RelNode, RelNode>();

    public SubstitutionVisitor(RelNode find, RelNode query) {
        this.query = query;
        this.find = find;
        final Set<RelNode> parents = new HashSet<RelNode>();
        final List<RelNode> allNodes = new ArrayList<RelNode>();
        final RelVisitor visitor =
            new RelVisitor() {
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    parentMap.put(node, Pair.of(parent, ordinal));
                    parents.add(parent);
                    allNodes.add(node);
                    super.visit(node, ordinal, parent);
                }
            };
        visitor.go(find);

        // Populate the list of leaves in the tree under "find".
        // Leaves are all nodes that are not parents.
        // For determinism, it is important that the list is in scan order.
        allNodes.removeAll(parents);
        findLeaves = ImmutableList.copyOf(allNodes);

        allNodes.clear();
        visitor.go(query);
        allNodes.removeAll(parents);
        queryLeaves = ImmutableList.copyOf(allNodes);
    }

    // TODO: move to RelOptUtil
    private static boolean contains(RelNode ancestor, final RelNode target) {
        if (ancestor == target) {
            // Short-cut common case.
            return true;
        }
        try {
            new RelVisitor() {
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    if (node == target) {
                        throw FoundRel.INSTANCE;
                    }
                    super.visit(node, ordinal, parent);
                }
            }.go(ancestor);
            return false;
        } catch (FoundRel e) {
            return true;
        }
    }

    private static RelNode replace(RelNode query, RelNode find, RelNode replace)
    {
        if (find == replace) {
            // Short-cut common case.
            return query;
        }
        assert equalType("find", find, "replace", replace);
        if (query == find) {
            // Short-cut another common case.
            return replace;
        }
        return replaceRecurse(query, find, replace);
    }

    private static RelNode replaceRecurse(
        RelNode query, RelNode find, RelNode replace)
    {
        if (query == find) {
            return replace;
        }
        final List<RelNode> inputs = query.getInputs();
        if (!inputs.isEmpty()) {
            final List<RelNode> newInputs = new ArrayList<RelNode>();
            for (RelNode input : inputs) {
                newInputs.add(replaceRecurse(input, find, replace));
            }
            if (!newInputs.equals(inputs)) {
                return query.copy(query.getTraitSet(), newInputs);
            }
        }
        return query;
    }

    public RelNode go(RelNode replacement) {
        assert equalType("find", find, "replacement", replacement);
        replacementMap.put(find, replacement);
        final UnifyResult unifyResult = matchRecurse(find);
        if (unifyResult != null) {
            final RelNode node =
                replace(query, unifyResult.query, unifyResult.result);
            System.out.println(
                "Convert: query=" + RelOptUtil.toString(query)
                + "\nnode=" + RelOptUtil.toString(node));
            return node;
        }
        return null;
    }

    private UnifyResult matchRecurse(RelNode find) {
        final List<RelNode> findInputs = find.getInputs();
        final List<RelNode> queryInputs = new ArrayList<RelNode>();
        RelNode queryParent = null;

        for (RelNode findInput : findInputs) {
            UnifyResult unifyResult = matchRecurse(findInput);
            if (unifyResult == null) {
                return null;
            }
            queryInputs.add(unifyResult.result);
            Pair<RelNode, Integer> pair = parentMap.get(unifyResult.query);
            queryParent = pair.left;
        }

        for (UnifyRule rule : RULES) {
            if (findInputs.isEmpty()) {
                for (RelNode queryLeaf : queryLeaves) {
                    final UnifyResult x = apply(rule, queryLeaf, find);
                    if (x != null) {
                        return x;
                    }
                }
            } else {
                final UnifyResult x = apply(rule, queryParent, find);
                if (x != null) {
                    return x;
                }
            }
        }
        return null;
    }

    private <Q extends RelNode, T extends RelNode> UnifyResult apply(
        UnifyRule<Q, T> rule, Q query, T find)
    {
        final Class<Q> queryClass = rule.getQueryClass();
        final Class<T> targetClass = rule.getTargetClass();
        if (queryClass.isInstance(query)
            && targetClass.isInstance(find))
        {
            return rule.apply(
                new UnifyIn<Q, T>(
                    queryClass.cast(query),
                    targetClass.cast(find)));
        }
        return null;
    }

    // First, burrow to the bottom of the replacement.
    private DownResult match(RelNode find) {
        switch (Match.of(find)) {
        case FILTER:
            return matchFilter(((FilterRel) find));
        case PROJECT:
            return matchProject(((ProjectRel) find));
        case SCAN:
            return matchScan((TableAccessRelBase) find);
        case UNION:
            return matchUnion((UnionRel) find);
        case JOIN:
            return matchJoin((JoinRel) find);
        default:
            throw new AssertionError(find);
        }
    }

    DownResult matchScan(TableAccessRelBase find) {
        // We've reached a leaf of the "find" tree. Find the corresponding
        // leaf of the "query" tree.
        final RelNode query = leafMap.get(find);
        assert query instanceof TableAccessRelBase;
        return DownResult.of(query);
    }

    DownResult matchProject(ProjectRel find) {
        DownResult in = match(find.getChild());
        switch (Match.of(in.rel)) {
        case PROJECT:
            // Decompose the project. For example, if
            //   query = Project(2, 7 from T)
            //   find = Project(2, 3, 7 from T)
            // then
            //   in.
            //   out.result = Project(0, 1, 2 from MV)
            //   out.residue = lambda R => Project(0, 2 from R)
        }
        // TODO:
        return DownResult.of(null);
    }

    DownResult matchFilter(FilterRel find) {
        return match(find.getChild());
    }

    DownResult matchUnion(UnionRel find) {
        throw new AssertionError(); // TODO:
    }

    DownResult matchJoin(JoinRel find) {
        throw new AssertionError(); // TODO:
    }

    private interface Handler<R> {
        R apply(TableAccessRelBase rel);
        R apply(ProjectRel rel);
        R apply(FilterRel rel);
        R apply(UnionRel rel);
        R apply(JoinRel rel);
    }

    private enum Match {
        SCAN {
            public <R> Dispatcher<R> dispatcher(
                final RelNode rel, final Handler<R> handler)
            {
                return new Dispatcher<R>() {
                    public R apply() {
                        return handler.apply((TableAccessRelBase) rel);
                    }
                };
            }
        },
        PROJECT {
            public <R> Dispatcher<R> dispatcher(
                final RelNode rel, final Handler<R> handler)
            {
                return new Dispatcher<R>() {
                    public R apply() {
                        return handler.apply((ProjectRel) rel);
                    }
                };
            }
        },
        FILTER {
            public <R> Dispatcher<R> dispatcher(
                final RelNode rel, final Handler<R> handler)
            {
                return new Dispatcher<R>() {
                    public R apply() {
                        return handler.apply((FilterRel) rel);
                    }
                };
            }
        },
        UNION {
            public <R> Dispatcher<R> dispatcher(
                final RelNode rel, final Handler<R> handler)
            {
                return new Dispatcher<R>() {
                    public R apply() {
                        return handler.apply((UnionRel) rel);
                    }
                };
            }
        },
        JOIN {
            public <R> Dispatcher<R> dispatcher(
                final RelNode rel, final Handler<R> handler)
            {
                return new Dispatcher<R>() {
                    public R apply() {
                        return handler.apply((JoinRel) rel);
                    }
                };
            }
        };

        static Match of(RelNode rel) {
            if (rel instanceof TableAccessRelBase) {
                return SCAN;
            }
            if (rel instanceof ProjectRel) {
                return PROJECT;
            }
            if (rel instanceof FilterRel) {
                return FILTER;
            }
            if (rel instanceof JoinRel) {
                return JOIN;
            }
            if (rel instanceof UnionRel) {
                return UNION;
            }
            throw new AssertionError("unexpected " + rel);
        }

        public abstract <R> Dispatcher<R> dispatcher(
            RelNode rel, Handler<R> handler);
    }

    private static abstract class Dispatcher<R> {
        public static <R> Dispatcher<R> of(RelNode rel, Handler<R> handler) {
            return Match.of(rel).dispatcher(rel, handler);
        }

        public abstract R apply();
    }

    private static class DownResult {
        public final RelNode rel;

        private DownResult(RelNode rel) {
            this.rel = rel;
        }

        static DownResult of(RelNode rel) {
            return new DownResult(rel);
        }
    }

    private static class MatchFailed extends RuntimeException {
        public static final MatchFailed INSTANCE = new MatchFailed();
    }

    private static class FoundRel extends RuntimeException {
        public static final FoundRel INSTANCE = new FoundRel();
    }

    private interface UnifyRule<Q extends RelNode, T extends RelNode> {
        Class<Q> getQueryClass();
        Class<T> getTargetClass();

        /** <p>Applies this rule to a particular node in a query. The goal is
         * to convert {@code query} into {@code target}. Before the rule is
         * invoked, Optiq has made sure that query's children are equivalent
         * to target's children.
         *
         * <p>There are 3 possible outcomes:</p>
         *
         * <ul>
         *
         * <li>{@code query} already exactly matches {@code target}; returns
         * {@code target}</li>
         *
         * <li>{@code query} is sufficiently close to a match for
         * {@code target}; returns {@code target}</li>
         *
         * <li>{@code query} cannot be made to match {@code target}; returns
         * null</li>
         *
         * </ul>
         *
         * <p>REVIEW: Is possible that we match query PLUS one or more of its
         * ancestors?</p>
         *
         * @param in Input parameters
         */
        UnifyResult apply(UnifyIn<Q, T> in);
    }

    /** Arguments to an application of a {@link UnifyRule}. */
    private class UnifyIn<Q extends RelNode, T extends RelNode> {
        final Q query;
        final T target;

        public UnifyIn(
            Q query, T target)
        {
            this.query = query;
            this.target = target;
        }

        public Pair<RelNode, Integer> parent(RelNode node) {
            return parentMap.get(node);
        }

        UnifyResult result(RelNode result) {
            assert contains(result, target);
            assert equalType("result", result, "query", query);
            RelNode replace = replacementMap.get(target);
            if (replace != null) {
                result = replace(result, target, replace);
            }
            return new UnifyResult(query, target, result);
        }
    }

    private static boolean equalType(
        String desc0, RelNode rel0, String desc1, RelNode rel1)
    {
        return RelOptUtil.equal(
            desc0, rel0.getRowType(), desc1, rel1.getRowType(), true);
    }

    /** Result of an application of a {@link UnifyRule} indicating that the
     * rule successfully matched {@code query} against {@code target} and
     * generated a {@code result} that is equivalent to {@code query} and
     * contains {@code target}. */
    private static class UnifyResult {
        private final RelNode query;
        private final RelNode target;
        // equivalent to "query", contains "result"
        private final RelNode result;

        UnifyResult(RelNode query, RelNode target, RelNode result) {
            this.query = query;
            this.target = target;
            this.result = result;
        }
    }

    private static abstract class AbstractUnifyRule<Q extends RelNode,
        T extends RelNode>
        implements UnifyRule<Q, T>
    {
        private final Class<Q> queryClass;
        private final Class<T> targetClass;

        public AbstractUnifyRule(Class<Q> queryClass, Class<T> targetClass) {
            this.queryClass = queryClass;
            this.targetClass = targetClass;
        }

        public Class<Q> getQueryClass() {
            return queryClass;
        }

        public Class<T> getTargetClass() {
            return targetClass;
        }
    }

    private static class ScanUnifyRule
        extends AbstractUnifyRule<TableAccessRelBase, TableAccessRelBase>
    {
        public ScanUnifyRule() {
            super(TableAccessRelBase.class, TableAccessRelBase.class);
        }

        public UnifyResult apply(
            UnifyIn<TableAccessRelBase, TableAccessRelBase> in)
        {
            if (in.query.getTable().getQualifiedName().equals(
                    in.target.getTable().getQualifiedName()))
            {
                return in.result(in.target);
            }
            return null;
        }
    }

    private static class ProjectUnifyRule
        extends AbstractUnifyRule<ProjectRel, ProjectRel>
    {
        public ProjectUnifyRule() {
            super(ProjectRel.class, ProjectRel.class);
        }

        public UnifyResult apply(UnifyIn<ProjectRel, ProjectRel> in)
        {
            final RexShuttle shuttle = getRexShuttle(in.target);
            final ProjectRel newProject =
                new ProjectRel(
                    in.target.getCluster(),
                    in.target,
                    shuttle.apply(in.query.getProjects()),
                    in.query.getRowType(),
                    in.query.getFlags(),
                    in.query.getCollationList());
            final RelNode newProject2 =
                RemoveTrivialProjectRule.strip(newProject);
            return in.result(newProject2);
        }
    }

    private static class FilterToProjectUnifyRule
        extends AbstractUnifyRule<FilterRel, ProjectRel>
    {
        public FilterToProjectUnifyRule() {
            super(FilterRel.class, ProjectRel.class);
        }

        public UnifyResult apply(UnifyIn<FilterRel, ProjectRel> in)
        {
            // Child of projectTarget is equivalent to child of filterQuery.
            try {
                // TODO: shuttle that recognizes more complex
                // expressions e,g
                //   materialized view as select x + y from t
                //
                // TODO: make sure that constants are ok
                final RexShuttle shuttle = getRexShuttle(in.target);
                final FilterRel newFilter =
                    new FilterRel(
                        in.query.getCluster(),
                        in.target,
                        in.query.getCondition().accept(shuttle));
                return in.result(newFilter);
            } catch (MatchFailed e) {
                return null;
            }
        }
    }

    private static RexShuttle getRexShuttle(ProjectRel target) {
        final Map<String, Integer> map = new HashMap<String, Integer>();
        for (RexNode e : target.getProjects()) {
            map.put(e.toString(), map.size());
        }
        return new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                final Integer integer = map.get(ref.getName());
                if (integer != null) {
                    return new RexInputRef(integer, ref.getType());
                }
                throw MatchFailed.INSTANCE;
            }
        };
    }

    private static final List<UnifyRule> RULES =
        Arrays.<UnifyRule>asList(
            new ScanUnifyRule(),
            new ProjectUnifyRule(),
            new FilterToProjectUnifyRule());
}

// End SubstitutionVisitor.java
