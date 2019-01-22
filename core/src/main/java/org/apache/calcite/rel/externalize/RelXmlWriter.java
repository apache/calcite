/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.externalize;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.XmlOutput;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Callback for a relational expression to dump in XML format.
 */
public class RelXmlWriter extends RelWriterImpl {
  //~ Instance fields --------------------------------------------------------

  private final XmlOutput xmlOutput;
  boolean generic = true;

  //~ Constructors -----------------------------------------------------------

  // TODO jvs 23-Dec-2005:  honor detail level.  The current inheritance
  // structure makes this difficult without duplication; need to factor
  // out the filtering of attributes before rendering.

  public RelXmlWriter(PrintWriter pw, SqlExplainLevel detailLevel) {
    super(pw, detailLevel, true);
    xmlOutput = new XmlOutput(pw);
    xmlOutput.setGlob(true);
    xmlOutput.setCompact(false);
  }

  //~ Methods ----------------------------------------------------------------

  protected void explain_(
      RelNode rel,
      List<Pair<String, Object>> values) {
    if (generic) {
      explainGeneric(rel, values);
    } else {
      explainSpecific(rel, values);
    }
  }

  /**
   * Generates generic XML (sometimes called 'element-oriented XML'). Like
   * this:
   *
   * <blockquote>
   * <code>
   * &lt;RelNode id="1" type="Join"&gt;<br>
   * &nbsp;&nbsp;&lt;Property name="condition"&gt;EMP.DEPTNO =
   * DEPT.DEPTNO&lt;/Property&gt;<br>
   * &nbsp;&nbsp;&lt;Inputs&gt;<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&lt;RelNode id="2" type="Project"&gt;<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;Property name="expr1"&gt;x +
   * y&lt;/Property&gt;<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;Property
   * name="expr2"&gt;45&lt;/Property&gt;<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&lt;/RelNode&gt;<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&lt;RelNode id="3" type="TableAccess"&gt;<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;Property
   * name="table"&gt;SALES.EMP&lt;/Property&gt;<br>
   * &nbsp;&nbsp;&nbsp;&nbsp;&lt;/RelNode&gt;<br>
   * &nbsp;&nbsp;&lt;/Inputs&gt;<br>
   * &lt;/RelNode&gt;</code>
   * </blockquote>
   *
   * @param rel    Relational expression
   * @param values List of term-value pairs
   */
  private void explainGeneric(
      RelNode rel,
      List<Pair<String, Object>> values) {
    String relType = rel.getRelTypeName();
    xmlOutput.beginBeginTag("RelNode");
    xmlOutput.attribute("type", relType);

    xmlOutput.endBeginTag("RelNode");

    final List<RelNode> inputs = new ArrayList<>();
    for (Pair<String, Object> pair : values) {
      if (pair.right instanceof RelNode) {
        inputs.add((RelNode) pair.right);
        continue;
      }
      if (pair.right == null) {
        continue;
      }
      xmlOutput.beginBeginTag("Property");
      xmlOutput.attribute("name", pair.left);
      xmlOutput.endBeginTag("Property");
      xmlOutput.cdata(pair.right.toString());
      xmlOutput.endTag("Property");
    }
    xmlOutput.beginTag("Inputs", null);
    spacer.add(2);
    for (RelNode input : inputs) {
      input.explain(this);
    }
    spacer.subtract(2);
    xmlOutput.endTag("Inputs");
    xmlOutput.endTag("RelNode");
  }

  /**
   * Generates specific XML (sometimes called 'attribute-oriented XML'). Like
   * this:
   *
   * <blockquote><pre>
   * &lt;Join condition="EMP.DEPTNO = DEPT.DEPTNO"&gt;
   *   &lt;Project expr1="x + y" expr2="42"&gt;
   *   &lt;TableAccess table="SALES.EMPS"&gt;
   * &lt;/Join&gt;
   * </pre></blockquote>
   *
   * @param rel    Relational expression
   * @param values List of term-value pairs
   */
  private void explainSpecific(
      RelNode rel,
      List<Pair<String, Object>> values) {
    String tagName = rel.getRelTypeName();
    xmlOutput.beginBeginTag(tagName);
    xmlOutput.attribute("id", rel.getId() + "");

    for (Pair<String, Object> value : values) {
      if (value.right instanceof RelNode) {
        continue;
      }
      xmlOutput.attribute(
          value.left,
          value.right.toString());
    }
    xmlOutput.endBeginTag(tagName);
    spacer.add(2);
    for (RelNode input : rel.getInputs()) {
      input.explain(this);
    }
    spacer.subtract(2);
  }
}

// End RelXmlWriter.java
