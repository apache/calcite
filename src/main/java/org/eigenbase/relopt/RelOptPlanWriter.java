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
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * Callback for an expression to dump itself to.
 */
public class RelOptPlanWriter
    extends java.io.PrintWriter
{
    //~ Instance fields --------------------------------------------------------

    private boolean withIdPrefix = true;

    private final SqlExplainLevel detailLevel;
    int level;

    //~ Constructors -----------------------------------------------------------

    public RelOptPlanWriter(java.io.PrintWriter pw)
    {
        this(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    }

    public RelOptPlanWriter(
        java.io.PrintWriter pw,
        SqlExplainLevel detailLevel)
    {
        super(pw);
        this.level = 0;
        this.detailLevel = detailLevel;
    }

    //~ Methods ----------------------------------------------------------------

    public void setIdPrefix(boolean b)
    {
        withIdPrefix = b;
    }

    /**
     * Prints the plan of a given relational expression to this writer.
     *
     * <p>The terms and values array must be specified. Individual values may
     * be null.</p>
     *
     * @param rel Relational expression
     * @param terms Names of the attributes of the plan
     * @param values Values of the attributes of the plan
     *
     * @pre rel != null
     * @pre terms.length == rel.getChildExps().length + values.length
     * @pre values != null
     */
    public void explain(
        RelNode rel,
        String [] terms,
        Object [] values)
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

        RexNode [] children = rel.getChildExps();
        assert terms.length
            == (inputs.size() + children.length
                + values.length) : "terms.length=" + terms.length
            + " inputs.length=" + inputs.size() + " children.length="
            + children.length + " values.length=" + values.length;
        String s;
        if (withIdPrefix) {
            s = rel.getId() + ":";
        } else {
            s = "";
        }
        s = s + rel.getRelTypeName();

        for (int i = 0; i < level; i++) {
            print("  ");
        }
        print(s);
        if (detailLevel != SqlExplainLevel.NO_ATTRIBUTES) {
            int j = 0;
            for (int i = 0; i < children.length; i++) {
                RexNode child = children[i];
                print(
                    ((j == 0) ? "(" : ", ")
                    + terms[inputs.size() + j++] + "=["
                    + child.toString() + "]");
            }
            for (int i = 0; i < values.length; i++) {
                Object value = values[i];
                print(
                    ((j == 0) ? "(" : ", ")
                    + terms[inputs.size() + j++] + "=["
                    + value + "]");
            }
            if (j > 0) {
                print(")");
            }
        }
        if (detailLevel == SqlExplainLevel.ALL_ATTRIBUTES) {
            print(": rowcount = " + RelMetadataQuery.getRowCount(rel));
            print(", cumulative cost = ");
            print(RelMetadataQuery.getCumulativeCost(rel));
        }
        println("");
        level++;
        explainInputs(inputs);
        level--;
    }

    private void explainInputs(List<RelNode> inputs)
    {
        for (int i = 0; i < inputs.size(); i++) {
            RelNode child = inputs.get(i);
            child.explain(this);
        }
    }

    public void explain(
        RelNode rel,
        String [] terms)
    {
        explain(rel, terms, Util.emptyStringArray);
    }

    /**
     * Shorthand for {@link #explain(RelNode, String[], Object[])}.
     *
     * @param rel Relational expression
     * @param termList List of names of the attributes of the plan
     * @param valueList List of values of the attributes of the plan
     */
    public final void explain(
        RelNode rel,
        List<String> termList,
        List<Object> valueList)
    {
        String [] terms = termList.toArray(new String[termList.size()]);
        Object [] values = valueList.toArray(new Object[valueList.size()]);
        explain(rel, terms, values);
    }

    /**
     * Special form used by {@link
     * org.eigenbase.relopt.volcano.RelSubset}.
     */
    public void explainSubset(
        String s,
        RelNode child)
    {
        print(s);
        level++;
        child.explain(this);
        level--;
    }

    public void explainTree(RelNode exp)
    {
        this.level = 0;
        exp.explain(this);
    }

    /**
     * @return detail level at which plan should be generated
     */
    public SqlExplainLevel getDetailLevel()
    {
        return detailLevel;
    }
}

// End RelOptPlanWriter.java
