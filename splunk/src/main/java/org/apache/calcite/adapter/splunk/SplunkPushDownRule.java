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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.adapter.splunk.util.StringUtils;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Planner rule to push filters and projections to Splunk.
 */
public class SplunkPushDownRule
    extends RelOptRule {
  private static final Logger LOGGER =
      StringUtils.getClassTracer(SplunkPushDownRule.class);

  private static final Set<SqlKind> SUPPORTED_OPS =
      ImmutableSet.of(
          SqlKind.CAST,
          SqlKind.EQUALS,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL,
          SqlKind.NOT_EQUALS,
          SqlKind.LIKE,
          SqlKind.AND,
          SqlKind.OR,
          SqlKind.NOT);

  public static final SplunkPushDownRule PROJECT_ON_FILTER =
      new SplunkPushDownRule(
          operand(
              LogicalProject.class,
              operand(
                  LogicalFilter.class,
                  operand(
                      LogicalProject.class,
                      operand(SplunkTableScan.class, none())))),
          RelFactories.LOGICAL_BUILDER,
          "proj on filter on proj");

  public static final SplunkPushDownRule FILTER_ON_PROJECT =
      new SplunkPushDownRule(
          operand(
              LogicalFilter.class,
              operand(
                  LogicalProject.class,
                  operand(SplunkTableScan.class, none()))),
          RelFactories.LOGICAL_BUILDER,
          "filter on proj");

  public static final SplunkPushDownRule FILTER =
      new SplunkPushDownRule(
          operand(
              LogicalFilter.class, operand(SplunkTableScan.class, none())),
          RelFactories.LOGICAL_BUILDER,
          "filter");

  public static final SplunkPushDownRule PROJECT =
      new SplunkPushDownRule(
          operand(
              LogicalProject.class,
              operand(SplunkTableScan.class, none())),
          RelFactories.LOGICAL_BUILDER,
          "proj");

  /** Creates a SplunkPushDownRule. */
  @Deprecated // to be removed before 2.0
  protected SplunkPushDownRule(RelOptRuleOperand rule, String id) {
    this(rule, RelFactories.LOGICAL_BUILDER, id);
  }

  /** Creates a SplunkPushDownRule. */
  public SplunkPushDownRule(RelOptRuleOperand rule,
      RelBuilderFactory relBuilderFactory, String id) {
    super(rule, relBuilderFactory, "SplunkPushDownRule: " + id);
  }

  // ~ Methods --------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    LOGGER.debug(description);

    int relLength = call.rels.length;
    SplunkTableScan splunkRel =
        (SplunkTableScan) call.rels[relLength - 1];

    LogicalFilter filter;
    LogicalProject topProj    = null;
    LogicalProject bottomProj = null;


    RelDataType topRow = splunkRel.getRowType();

    int filterIdx = 2;
    if (call.rels[relLength - 2] instanceof LogicalProject) {
      bottomProj = (LogicalProject) call.rels[relLength - 2];
      filterIdx  = 3;

      // bottom projection will change the field count/order
      topRow =  bottomProj.getRowType();
    }

    String filterString;

    if (filterIdx <= relLength
        && call.rels[relLength - filterIdx] instanceof LogicalFilter) {
      filter = (LogicalFilter) call.rels[relLength - filterIdx];

      int topProjIdx = filterIdx + 1;
      if (topProjIdx <= relLength
          && call.rels[relLength - topProjIdx] instanceof LogicalProject) {
        topProj = (LogicalProject) call.rels[relLength - topProjIdx];
      }

      RexCall filterCall = (RexCall) filter.getCondition();
      SqlOperator op = filterCall.getOperator();
      List<RexNode> operands = filterCall.getOperands();

      LOGGER.debug("fieldNames: {}", getFieldsString(topRow));

      final StringBuilder buf = new StringBuilder();
      if (getFilter(op, operands, buf, topRow.getFieldNames())) {
        filterString = buf.toString();
      } else {
        return; // can't handle
      }
    } else {
      filterString = "";
    }

    // top projection will change the field count/order
    if (topProj != null) {
      topRow =  topProj.getRowType();
    }
    LOGGER.debug("pre transformTo fieldNames: {}", getFieldsString(topRow));

    call.transformTo(
        appendSearchString(
            filterString, splunkRel, topProj, bottomProj,
            topRow, null));
  }

  /**
   * Appends a search string.
   *
   * @param toAppend Search string to append
   * @param splunkRel Relational expression
   * @param topProj Top projection
   * @param bottomProj Bottom projection
   */
  protected RelNode appendSearchString(
      String toAppend,
      SplunkTableScan splunkRel,
      LogicalProject topProj,
      LogicalProject bottomProj,
      RelDataType topRow,
      RelDataType bottomRow) {
    final RelOptCluster cluster = splunkRel.getCluster();
    StringBuilder updateSearchStr = new StringBuilder(splunkRel.search);

    if (!toAppend.isEmpty()) {
      updateSearchStr.append(" ").append(toAppend);
    }
    List<RelDataTypeField> bottomFields =
        bottomRow == null ? null : bottomRow.getFieldList();
    List<RelDataTypeField> topFields    =
        topRow    == null ? null : topRow.getFieldList();

    if (bottomFields == null) {
      bottomFields = splunkRel.getRowType().getFieldList();
    }

    // handle bottom projection (ie choose a subset of the table fields)
    if (bottomProj != null) {
      List<RelDataTypeField> tmp  =
          new ArrayList<RelDataTypeField>();
      List<RelDataTypeField> dRow = bottomProj.getRowType().getFieldList();
      for (RexNode rn : bottomProj.getProjects()) {
        RelDataTypeField rdtf;
        if (rn instanceof RexSlot) {
          RexSlot rs = (RexSlot) rn;
          rdtf = bottomFields.get(rs.getIndex());
        } else {
          rdtf = dRow.get(tmp.size());
        }
        tmp.add(rdtf);
      }
      bottomFields = tmp;
    }

    // field renaming: to -> from
    List<Pair<String, String>> renames =
        new LinkedList<Pair<String, String>>();

    // handle top projection (ie reordering and renaming)
    List<RelDataTypeField> newFields = bottomFields;
    if (topProj != null) {
      LOGGER.debug("topProj: {}", String.valueOf(topProj.getPermutation()));
      newFields = new ArrayList<RelDataTypeField>();
      int i = 0;
      for (RexNode rn : topProj.getProjects()) {
        RexInputRef rif = (RexInputRef) rn;
        RelDataTypeField field = bottomFields.get(rif.getIndex());
        if (!bottomFields.get(rif.getIndex()).getName()
            .equals(topFields.get(i).getName())) {
          renames.add(
              Pair.of(
                  bottomFields.get(rif.getIndex()).getName(),
                  topFields.get(i).getName()));
          field = topFields.get(i);
        }
        newFields.add(field);
      }
    }

    if (!renames.isEmpty()) {
      updateSearchStr.append("| rename ");
      for (Pair<String, String> p : renames) {
        updateSearchStr.append(p.left).append(" AS ")
            .append(p.right).append(" ");
      }
    }

    RelDataType resultType =
        cluster.getTypeFactory().createStructType(newFields);
    String searchWithFilter = updateSearchStr.toString();

    RelNode rel =
        new SplunkTableScan(
            cluster,
            splunkRel.getTable(),
            splunkRel.splunkTable,
            searchWithFilter,
            splunkRel.earliest,
            splunkRel.latest,
            resultType.getFieldNames());

    LOGGER.debug("end of appendSearchString fieldNames: {}",
        rel.getRowType().getFieldNames());
    return rel;
  }

  // ~ Private Methods ------------------------------------------------------

  private static RelNode addProjectionRule(LogicalProject proj, RelNode rel) {
    if (proj == null) {
      return rel;
    }
    return LogicalProject.create(rel, proj.getProjects(), proj.getRowType());
  }

  // TODO: use StringBuilder instead of String
  // TODO: refactor this to use more tree like parsing, need to also
  //      make sure we use parens properly - currently precedence
  //      rules are simply left to right
  private boolean getFilter(SqlOperator op, List<RexNode> operands,
      StringBuilder s, List<String> fieldNames) {
    if (!valid(op.getKind())) {
      return false;
    }

    boolean like = false;
    switch (op.getKind()) {
    case NOT:
      // NOT op pre-pended
      s = s.append(" NOT ");
      break;
    case CAST:
      return asd(false, operands, s, fieldNames, 0);
    case LIKE:
      like = true;
      break;
    }

    for (int i = 0; i < operands.size(); i++) {
      if (!asd(like, operands, s, fieldNames, i)) {
        return false;
      }
      if (op instanceof SqlBinaryOperator && i == 0) {
        s.append(" ").append(op).append(" ");
      }
    }
    return true;
  }

  private boolean asd(boolean like, List<RexNode> operands, StringBuilder s,
      List<String> fieldNames, int i) {
    RexNode operand = operands.get(i);
    if (operand instanceof RexCall) {
      s.append("(");
      final RexCall call = (RexCall) operand;
      boolean b =
          getFilter(
              call.getOperator(),
              call.getOperands(),
              s,
              fieldNames);
      if (!b) {
        return false;
      }
      s.append(")");
    } else {
      if (operand instanceof RexInputRef) {
        if (i != 0) {
          return false;
        }
        int fieldIndex = ((RexInputRef) operand).getIndex();
        String name = fieldNames.get(fieldIndex);
        s.append(name);
      } else { // RexLiteral
        String tmp = toString(like, (RexLiteral) operand);
        if (tmp == null) {
          return false;
        }
        s.append(tmp);
      }
    }
    return true;
  }

  private boolean valid(SqlKind kind) {
    return SUPPORTED_OPS.contains(kind);
  }

  private String toString(SqlOperator op) {
    if (op.equals(SqlStdOperatorTable.LIKE)) {
      return SqlStdOperatorTable.EQUALS.toString();
    } else if (op.equals(SqlStdOperatorTable.NOT_EQUALS)) {
      return "!=";
    }
    return op.toString();
  }

  public static String searchEscape(String str) {
    if (str.isEmpty()) {
      return "\"\"";
    }
    StringBuilder sb = new StringBuilder(str.length());
    boolean quote = false;

    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (c == '"' || c == '\\') {
        sb.append('\\');
      }
      sb.append(c);

      quote |= !(Character.isLetterOrDigit(c) || c == '_');
    }

    if (quote || sb.length() != str.length()) {
      sb.insert(0, '"');
      sb.append('"');
      return sb.toString();
    }
    return str;
  }

  private String toString(boolean like, RexLiteral literal) {
    String value = null;
    SqlTypeName litSqlType = literal.getTypeName();
    if (SqlTypeName.NUMERIC_TYPES.contains(litSqlType)) {
      value = literal.getValue().toString();
    } else if (litSqlType == SqlTypeName.CHAR) {
      value = ((NlsString) literal.getValue()).getValue();
      if (like) {
        value = value.replaceAll("%", "*");
      }
      value = searchEscape(value);
    }
    return value;
  }

  // transform the call from SplunkUdxRel to FarragoJavaUdxRel
  // usually used to stop the optimizer from calling us
  protected void transformToFarragoUdxRel(
      RelOptRuleCall call,
      SplunkTableScan splunkRel,
      LogicalFilter filter,
      LogicalProject topProj,
      LogicalProject bottomProj) {
    assert false;
/*
    RelNode rel =
        new EnumerableTableScan(
            udxRel.getCluster(),
            udxRel.getTable(),
            udxRel.getRowType(),
            udxRel.getServerMofId());

    rel = RelOptUtil.createCastRel(rel, udxRel.getRowType(), true);

    rel = addProjectionRule(bottomProj, rel);

    if (filter != null) {
      rel =
          new LogicalFilter(filter.getCluster(), rel, filter.getCondition());
    }

    rel = addProjectionRule(topProj, rel);

    call.transformTo(rel);
*/
  }

  public static String getFieldsString(RelDataType row) {
    return row.getFieldNames().toString();
  }
}

// End SplunkPushDownRule.java
