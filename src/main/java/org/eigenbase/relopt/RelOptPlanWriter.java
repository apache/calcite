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

import java.io.PrintWriter;
import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;

import net.hydromatic.optiq.runtime.Spacer;

import net.hydromatic.linq4j.Ord;

import com.google.common.collect.ImmutableList;


/**
 * Callback for an expression to dump itself to.
 */
public class RelOptPlanWriter {
    //~ Instance fields --------------------------------------------------------

    private boolean withIdPrefix = true;
    protected final PrintWriter pw;
    private final SqlExplainLevel detailLevel;
    protected final Spacer spacer = new Spacer();
    private final List<Pair<String, Object>> values =
        new ArrayList<Pair<String, Object>>();
    private int subsetPrefixLength;

    //~ Constructors -----------------------------------------------------------

    public RelOptPlanWriter(PrintWriter pw) {
        this(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    }

    public RelOptPlanWriter(
        PrintWriter pw,
        SqlExplainLevel detailLevel)
    {
        this.pw = pw;
        this.detailLevel = detailLevel;
    }

    //~ Methods ----------------------------------------------------------------

    public void setIdPrefix(boolean b)
    {
        withIdPrefix = b;
    }

    protected void explain_(
        RelNode rel,
        List<Pair<String, Object>> values)
    {
        List<RelNode> inputs = rel.getInputs();

        if (!RelMetadataQuery.isVisibleInExplain(
                rel,
                detailLevel))
        {
            // render children in place of this, at same level
            explainInputs(inputs);
            return;
        }

        StringBuilder s = new StringBuilder();
        spacer.spaces(s);
        if (withIdPrefix) {
            s.append(rel.getId()).append(":");
        }
        s.append(rel.getRelTypeName());
        if (detailLevel != SqlExplainLevel.NO_ATTRIBUTES) {
            int j = 0;
            for (Pair<String, Object> value : values) {
                if (value.right instanceof RelNode) {
                    continue;
                }
                if (j++ == 0) {
                    s.append("(");
                } else {
                    s.append(", ");
                }
                s.append(value.left)
                    .append("=[")
                    .append(value.right)
                    .append("]");
            }
            if (j > 0) {
                s.append(")");
            }
        }
        if (detailLevel == SqlExplainLevel.ALL_ATTRIBUTES) {
            s.append(": rowcount = ")
                .append(RelMetadataQuery.getRowCount(rel))
                .append(", cumulative cost = ")
                .append(RelMetadataQuery.getCumulativeCost(rel));
        }
        pw.println(s);
        spacer.add(2);
        explainInputs(inputs);
        spacer.subtract(2);
    }

    private void explainInputs(List<RelNode> inputs) {
        for (RelNode input : inputs) {
            input.explain(this);
        }
    }

    /**
     * Prints an explanation of a node, with a list of (term, value) pairs.
     *
     * <p>The term-value pairs are generally gathered by calling
     * {@link RelNode#explain(RelOptPlanWriter)}. Each sub-class of
     * {@link RelNode} calls {@link #input(String, org.eigenbase.rel.RelNode)}
     * and {@link #item(String, Object)} to declare term-value pairs.</p>
     *
     * @param rel Relational expression
     * @param valueList List of term-value pairs
     */
    public final void explain(
        RelNode rel,
        List<Pair<String, Object>> valueList)
    {
        explain_(rel, valueList);
    }

    /**
     * @return detail level at which plan should be generated
     */
    public SqlExplainLevel getDetailLevel()
    {
        return detailLevel;
    }

    /** Adds an input to the explanation of the current node.
     *
     * @param term Term for input, e.g. "left" or "input #1".
     * @param input Input relational expression
     */
    public RelOptPlanWriter input(String term, RelNode input) {
        values.add(Pair.of(term, (Object) input));
        return this;
    }

    /** Adds an attribute to the explanation of the current node.
     *
     * @param term Term for attribute, e.g. "joinType"
     * @param value Attribute value
     */
    public RelOptPlanWriter item(String term, Object value) {
        values.add(Pair.of(term, value));
        return this;
    }

    /** Adds an input to the explanation of the current node, if a condition
     * holds. */
    public RelOptPlanWriter itemIf(String term, Object value, boolean condition)
    {
        if (condition) {
            item(term, value);
        }
        return this;
    }

    /** Writes the completed explanation. */
    public RelOptPlanWriter done(RelNode node) {
        int i = 0;
        if (values.size() > 0 && values.get(0).left.equals("subset")) {
            ++i;
        }
        for (RelNode input : node.getInputs()) {
            assert values.get(i).right == input;
            ++i;
        }
        for (RexNode expr : node.getChildExps()) {
            assert values.get(i).right == expr;
            ++i;
        }
        final List<Pair<String, Object>> valuesCopy =
            ImmutableList.copyOf(values);
        values.clear();
        explain_(node, valuesCopy);
        pw.flush();
        return this;
    }

    /** Converts the collected terms and values to a string. Does not write to
     * the parent writer. */
    public String simple() {
        final StringBuilder buf = new StringBuilder("(");
        for (Ord<Pair<String, Object>> ord : Ord.zip(values)) {
            if (ord.i > 0) {
                buf.append(", ");
            }
            buf.append(ord.e.left).append("=[").append(ord.e.right).append("]");
        }
        buf.append(")");
        return buf.toString();
    }

    public void setSubsetPrefixLength(int subsetPrefixLength) {
        this.subsetPrefixLength = subsetPrefixLength;
    }
}

// End RelOptPlanWriter.java
