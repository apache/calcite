/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.relopt;

import java.io.*;
import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.xom.*;


/**
 * Callback for a relational expression to dump in XML format.
 */
public class RelOptXmlPlanWriter
    extends RelOptPlanWriter
{
    //~ Instance fields --------------------------------------------------------

    private final XMLOutput xmlOutput;
    boolean generic = true;

    //~ Constructors -----------------------------------------------------------

    // TODO jvs 23-Dec-2005:  honor detail level.  The current inheritance
    // structure makes this difficult without duplication; need to factor
    // out the filtering of attributes before rendering.

    public RelOptXmlPlanWriter(PrintWriter pw, SqlExplainLevel detailLevel)
    {
        super(pw, detailLevel);
        xmlOutput = new XMLOutput(this);
        xmlOutput.setGlob(true);
        xmlOutput.setCompact(false);
    }

    //~ Methods ----------------------------------------------------------------

    public void explain(RelNode rel, String [] terms, Object [] values)
    {
        if (generic) {
            explainGeneric(rel, terms, values);
        } else {
            explainSpecific(rel, terms, values);
        }
    }

    /**
     * Generates generic XML (sometimes called 'element-oriented XML'). Like
     * this:
     *
     * <blockquote>
     * <code>
     * &lt;RelNode id="1" type="Join"&gt;<br/>
     * &nbsp;&nbsp;&lt;Property name="condition"&gt;EMP.DEPTNO =
     * DEPT.DEPTNO&lt;/Property&gt;<br/>
     * &nbsp;&nbsp;&lt;Inputs&gt;<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&lt;RelNode id="2" type="Project"&gt;<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;Property name="expr1"&gt;x +
     * y&lt;/Property&gt;<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;Property
     * name="expr2"&gt;45&lt;/Property&gt;<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&lt;/RelNode&gt;<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&lt;RelNode id="3" type="TableAccess"&gt;<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;Property
     * name="table"&gt;SALES.EMP&lt;/Property&gt;<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;&lt;/RelNode&gt;<br/>
     * &nbsp;&nbsp;&lt;/Inputs&gt;<br/>
     * &lt;/RelNode&gt;<br/>
     * </code>
     * </blockquote>
     *
     * @param rel Relational expression
     * @param terms Names of the attributes of the plan
     * @param values Values of the attributes of the plan
     */
    private void explainGeneric(
        RelNode rel,
        String [] terms,
        Object [] values)
    {
        List<RelNode> inputs = rel.getInputs();
        RexNode [] children = rel.getChildExps();
        assert terms.length
            == (inputs.size() + children.length
                + values.length) : "terms.length=" + terms.length
            + " inputs.length=" + inputs.size() + " children.length="
            + children.length + " values.length=" + values.length;
        String relType = rel.getRelTypeName();
        xmlOutput.beginBeginTag("RelNode");
        xmlOutput.attribute("type", relType);

        //xmlOutput.attribute("id", rel.getId() + "");
        xmlOutput.endBeginTag("RelNode");

        int j = 0;
        for (RexNode child : children) {
            xmlOutput.beginBeginTag("Property");
            xmlOutput.attribute("name", terms[inputs.size() + j++]);
            xmlOutput.endBeginTag("Property");
            xmlOutput.cdata(child.toString());
            xmlOutput.endTag("Property");
        }
        for (Object value : values) {
            if (value != null) {
                xmlOutput.beginBeginTag("Property");
                xmlOutput.attribute("name", terms[inputs.size() + j++]);
                xmlOutput.endBeginTag("Property");
                xmlOutput.cdata(value.toString());
                xmlOutput.endTag("Property");
            }
        }
        xmlOutput.beginTag("Inputs", null);
        level++;
        for (RelNode input : inputs) {
            input.explain(this);
        }
        level--;
        xmlOutput.endTag("Inputs");
        xmlOutput.endTag("RelNode");
    }

    /**
     * Generates specific XML (sometimes called 'attribute-oriented XML'). Like
     * this:
     *
     * <pre>
     * &lt;Join condition="EMP.DEPTNO = DEPT.DEPTNO"&gt;
     *   &lt;Project expr1="x + y" expr2="42"&gt;
     *   &lt;TableAccess table="SALES.EMPS"&gt;
     * &lt;/Join&gt;
     * </pre>
     *
     * @param rel Relational expression
     * @param terms Names of the attributes of the plan
     * @param values Values of the attributes of the plan
     */
    private void explainSpecific(
        RelNode rel,
        String [] terms,
        Object [] values)
    {
        List<RelNode> inputs = rel.getInputs();
        RexNode [] children = rel.getChildExps();
        assert terms.length
            == (inputs.size() + children.length
                + values.length) : "terms.length=" + terms.length
            + " inputs.length=" + inputs.size() + " children.length="
            + children.length + " values.length=" + values.length;
        String tagName = rel.getRelTypeName();
        xmlOutput.beginBeginTag(tagName);
        xmlOutput.attribute("id", rel.getId() + "");

        int j = 0;
        for (RexNode child : children) {
            xmlOutput.attribute(
                terms[inputs.size() + j++],
                child.toString());
        }
        for (Object value : values) {
            if (value != null) {
                xmlOutput.attribute(
                    terms[inputs.size() + j++],
                    value.toString());
            }
        }
        xmlOutput.endBeginTag(tagName);
        level++;
        for (RelNode input : inputs) {
            input.explain(this);
        }
        level--;
    }
}

// End RelOptXmlPlanWriter.java
