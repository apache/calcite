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
package net.hydromatic.optiq.impl.splunk;

import net.hydromatic.optiq.impl.splunk.util.StringUtils;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Pair;

import java.util.*;
import java.util.logging.Logger;

/**
 * Planner rule to push filters and projections to Splunk.
 */
public class SplunkPushDownRule
    extends RelOptRule {
  private static final Logger LOGGER =
      StringUtils.getClassTracer(SplunkPushDownRule.class);

  private static final Set<SqlKind> SUPPORTED_OPS =
      new HashSet<SqlKind>(
          Arrays.asList(
              SqlKind.EQUALS,
              SqlKind.LESS_THAN,
              SqlKind.LESS_THAN_OR_EQUAL,
              SqlKind.GREATER_THAN,
              SqlKind.GREATER_THAN_OR_EQUAL,
              SqlKind.NOT_EQUALS,
              SqlKind.LIKE,
              SqlKind.AND,
              SqlKind.OR,
              SqlKind.NOT));

  public static final SplunkPushDownRule PROJECT_ON_FILTER =
      new SplunkPushDownRule(
          new RelOptRuleOperand(
              ProjectRel.class,
              new RelOptRuleOperand(
                  FilterRel.class,
                  new RelOptRuleOperand(
                      ProjectRel.class,
                      new RelOptRuleOperand(SplunkTableAccessRel.class)))),
          "proj on filter on proj");

  public static final SplunkPushDownRule FILTER_ON_PROJECT =
      new SplunkPushDownRule(
          new RelOptRuleOperand(
              FilterRel.class,
              new RelOptRuleOperand(
                  ProjectRel.class,
                  new RelOptRuleOperand(SplunkTableAccessRel.class))),
          "filter on proj");

  public static final SplunkPushDownRule FILTER =
      new SplunkPushDownRule(
          new RelOptRuleOperand(
              FilterRel.class,
              new RelOptRuleOperand(SplunkTableAccessRel.class)),
          "filter");

  public static final SplunkPushDownRule PROJECT =
      new SplunkPushDownRule(
          new RelOptRuleOperand(
              ProjectRel.class,
              new RelOptRuleOperand(SplunkTableAccessRel.class)),
          "proj");

  /** Creates a SplunkPushDownRule. */
  protected SplunkPushDownRule(RelOptRuleOperand rule, String id) {
    super(rule, "SplunkPushDownRule: " + id);
  }

  // ~ Methods --------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    LOGGER.fine(description);

    int relLength = call.rels.length;
    SplunkTableAccessRel splunkRel =
        (SplunkTableAccessRel) call.rels[relLength - 1];

    FilterRel  filter     = null;
    ProjectRel topProj    = null;
    ProjectRel bottomProj = null;


    RelDataType topRow = splunkRel.getRowType();

    int filterIdx = 2;
    if (call.rels[relLength - 2] instanceof ProjectRel) {
      bottomProj = (ProjectRel)call.rels[relLength - 2];
      filterIdx  = 3;

      // bottom projection will change the field count/order
      topRow =  bottomProj.getRowType();
    }

    String filterString;

    if (filterIdx <= relLength
        && call.rels[relLength - filterIdx] instanceof FilterRel) {
      filter = (FilterRel) call.rels[relLength - filterIdx];

      int topProjIdx = filterIdx + 1;
      if (topProjIdx <= relLength
          && call.rels[relLength - topProjIdx] instanceof ProjectRel) {
        topProj = (ProjectRel)call.rels[relLength - topProjIdx];
      }

      RexCall filterCall = (RexCall) filter.getCondition();
      SqlOperator op = filterCall.getOperator();
      List<RexNode> operands = filterCall.getOperands();

      LOGGER.fine("fieldNames: " + getFieldsString(topRow));

      filterString = getFilter(op, operands, "", topRow.getFieldNames());

      if (filterString == null) {
        // can't handle - exit and stop optimizer from calling
        // any SplunkUdxRel related optimizations
        transformToFarragoUdxRel(
            call, splunkRel,
            filter,
            topProj,
            bottomProj);
        return;
      }
    } else {
      filterString = "";
    }

    // top projection will change the field count/order
    if (topProj != null) {
      topRow =  topProj.getRowType();
    }
    LOGGER.fine("pre transformTo fieldNames: " + getFieldsString(topRow));

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
      SplunkTableAccessRel splunkRel,
      ProjectRel topProj,
      ProjectRel bottomProj,
      RelDataType topRow,
      RelDataType bottomRow) {
    final RexBuilder rexBuilder = splunkRel.getCluster().getRexBuilder();
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
      LOGGER.fine("topProj: " + String.valueOf(topProj.getPermutation()));
      newFields = new ArrayList<RelDataTypeField>();
      int i = 0;
      for (RexNode rn : topProj.getProjects()) {
        RexInputRef rif = (RexInputRef)rn;
        RelDataTypeField field = bottomFields.get(rif.getIndex());
        if (!bottomFields.get(rif.getIndex()).getName()
            .equals(topFields.get(i).getName())) {
          renames.add(
              new Pair<String, String>(
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

    RelDataType resultType = new RelRecordType(newFields);
    String searchWithFilter = updateSearchStr.toString();

    RelNode rel =
        new SplunkTableAccessRel(
            splunkRel.getCluster(),
            splunkRel.getTable(),
            splunkRel.splunkTable,
            searchWithFilter,
            splunkRel.earliest,
            splunkRel.latest,
            resultType.getFieldNames());

    LOGGER.fine(
        "end of appendSearchString fieldNames: "
        + rel.getRowType().getFieldNames());
    return rel;
  }

  // ~ Private Methods ------------------------------------------------------

  private static RelNode addProjectionRule(ProjectRel proj, RelNode rel) {
    if (proj == null) {
      return rel;
    }
    return new ProjectRel(
        proj.getCluster(),
        proj.getCluster().traitSetOf(
            proj.getCollationList().isEmpty()
                ? RelCollationImpl.EMPTY
                : proj.getCollationList().get(0)),
        rel, proj.getProjects(), proj.getRowType(), proj.getFlags());
  }

  // TODO: use StringBuilder instead of String
  // TODO: refactor this to use more tree like parsing, need to also
  //      make sure we use parens properly - currently precedence
  //      rules are simply left to right
  private String getFilter(
      SqlOperator op,
      List<RexNode> operands,
      String s,
      List<String> fieldNames) {
    if (!valid(op.getKind())) {
      return null;
    }

    // NOT op pre-pended
    if (op.equals(SqlStdOperatorTable.notOperator)) {
      s = s.concat(" NOT ");
    }

    for (int i = 0; i < operands.size(); i++) {
      final RexNode operand = operands.get(i);
      if (operand instanceof RexCall) {
        s = s.concat("(");
        final RexCall call = (RexCall) operand;
        s = getFilter(
            call.getOperator(),
            call.getOperands(),
            s,
            fieldNames);
        if (s == null) {
          return null;
        }
        s = s.concat(")");
        if (i != (operands.size() - 1)) {
          s = s.concat(" " + op.toString() + " ");
        }
      } else {
        if (operands.size() != 2) {
          return null;
        }
        if (operand instanceof RexInputRef) {
          if (i != 0) {
            return null; // must be of form field=value
          }

          int fieldIndex = ((RexInputRef) operand).getIndex();
          String name = fieldNames.get(fieldIndex);
          s = s.concat(name);
        } else { // RexLiteral
          RexLiteral lit = (RexLiteral) operand;

          String tmp = toString(op, lit);
          if (tmp == null) {
            return null;
          }
          s = s.concat(tmp);
        }
        if (i == 0) {
          s = s.concat(toString(op));
        }
      }
    }
    return s;
  }

  private boolean valid(SqlKind kind) {
    return SUPPORTED_OPS.contains(kind);
  }

  private String toString(SqlOperator op) {
    if (op.equals(SqlStdOperatorTable.likeOperator)) {
      return SqlStdOperatorTable.equalsOperator.toString();
    } else if (op.equals(SqlStdOperatorTable.notEqualsOperator)) {
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

  private String toString(SqlOperator op, RexLiteral literal) {
    String value = null;
    SqlTypeName litSqlType = literal.getTypeName();
    if (SqlTypeName.numericTypes.contains(litSqlType)) {
      value = literal.getValue().toString();
    } else if (litSqlType.equals(SqlTypeName.CHAR)) {
      value = ((NlsString) literal.getValue()).getValue();
      if (op.equals(SqlStdOperatorTable.likeOperator)) {
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
      SplunkTableAccessRel splunkRel,
      FilterRel filter,
      ProjectRel topProj,
      ProjectRel bottomProj) {
    assert false;
/*
        RelNode rel =
            new JavaRules.EnumerableTableAccessRel(
                udxRel.getCluster(),
                udxRel.getTable(),
                udxRel.getRowType(),
                udxRel.getServerMofId());

        rel = RelOptUtil.createCastRel(rel, udxRel.getRowType(), true);

        rel = addProjectionRule(bottomProj, rel);

        if (filter != null) {
            rel =
                new FilterRel(filter.getCluster(), rel, filter.getCondition());
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
