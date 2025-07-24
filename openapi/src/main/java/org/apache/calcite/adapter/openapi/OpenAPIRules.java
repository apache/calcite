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
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.AbstractList;
import java.util.List;

/**
 * Rules and relational operators for {@link OpenAPIRel#CONVENTION OPENAPI}
 * calling convention.
 */
class OpenAPIRules {

  static final RelOptRule[] RULES = {
      OpenAPIFilterRule.INSTANCE,
      OpenAPIProjectRule.INSTANCE,
      OpenAPISortRule.INSTANCE
  };

  private OpenAPIRules() {}

  /**
   * Returns 'string' if it is a call to item['string'], null otherwise.
   * This handles field access in the map-based row type.
   */
  private static String isItemCall(RexCall call) {
    if (call.getOperator() != SqlStdOperatorTable.ITEM) {
      return null;
    }
    final RexNode op0 = call.getOperands().get(0);
    final RexNode op1 = call.getOperands().get(1);

    if (op0 instanceof RexInputRef
        && ((RexInputRef) op0).getIndex() == 0
        && op1 instanceof RexLiteral
        && ((RexLiteral) op1).getValue2() instanceof String) {
      return (String) ((RexLiteral) op1).getValue2();
    }
    return null;
  }

  /**
   * Checks if current node represents item access as in {@code _MAP['foo']} or
   * {@code cast(_MAP['foo'] as integer)}.
   */
  static boolean isItem(RexNode node) {
    final Boolean result = node.accept(new RexVisitorImpl<Boolean>(false) {
      @Override public Boolean visitCall(final RexCall call) {
        return isItemCall(uncast(call)) != null;
      }
    });
    return Boolean.TRUE.equals(result);
  }

  /**
   * Unwraps cast expressions from current call.
   */
  private static RexCall uncast(RexCall maybeCast) {
    if (maybeCast.getKind() == SqlKind.CAST && maybeCast.getOperands().get(0) instanceof RexCall) {
      return uncast((RexCall) maybeCast.getOperands().get(0));
    }
    return maybeCast;
  }

  /**
   * Generates field names for OpenAPI results, similar to Elasticsearch.
   */
  static List<String> openAPIFieldNames(final RelDataType rowType) {
    return SqlValidatorUtil.uniquify(
        new AbstractList<String>() {
          @Override public String get(int index) {
            final String name = rowType.getFieldList().get(index).getName();
            return name.startsWith("$") ? "_" + name.substring(2) : name;
          }

          @Override public int size() {
            return rowType.getFieldCount();
          }
        },
        SqlValidatorUtil.EXPR_SUGGESTER, true);
  }

  /**
   * Base class for planner rules that convert a relational expression to
   * OpenAPI calling convention.
   */
  abstract static class OpenAPIConverterRule extends ConverterRule {
    protected OpenAPIConverterRule(Config config) {
      super(config);
    }
  }

  /**
   * Rule to convert a {@link LogicalFilter} to an {@link OpenAPIFilter}.
   */
  private static class OpenAPIFilterRule extends OpenAPIConverterRule {
    private static final OpenAPIFilterRule INSTANCE = Config.INSTANCE
        .withConversion(LogicalFilter.class, Convention.NONE,
            OpenAPIRel.CONVENTION, "OpenAPIFilterRule")
        .withRuleFactory(OpenAPIFilterRule::new)
        .toRule(OpenAPIFilterRule.class);

    protected OpenAPIFilterRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode relNode) {
      final LogicalFilter filter = (LogicalFilter) relNode;
      final RelTraitSet traitSet = filter.getTraitSet().replace(out);
      return new OpenAPIFilter(relNode.getCluster(), traitSet,
          convert(filter.getInput(), out), filter.getCondition());
    }
  }

  /**
   * Rule to convert a {@link LogicalProject} to an {@link OpenAPIProject}.
   */
  private static class OpenAPIProjectRule extends OpenAPIConverterRule {
    private static final OpenAPIProjectRule INSTANCE = Config.INSTANCE
        .withConversion(LogicalProject.class, Convention.NONE,
            OpenAPIRel.CONVENTION, "OpenAPIProjectRule")
        .withRuleFactory(OpenAPIProjectRule::new)
        .toRule(OpenAPIProjectRule.class);

    protected OpenAPIProjectRule(Config config) {
      super(config);
    }

    @Override public boolean matches(RelOptRuleCall call) {
      final LogicalProject project = call.rel(0);
      return project.getVariablesSet().isEmpty();
    }

    @Override public RelNode convert(RelNode relNode) {
      final LogicalProject project = (LogicalProject) relNode;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new OpenAPIProject(project.getCluster(), traitSet,
          convert(project.getInput(), out), project.getProjects(), project.getRowType());
    }
  }

  /**
   * Rule to convert a {@link Sort} to an {@link OpenAPISort}.
   */
  private static class OpenAPISortRule extends OpenAPIConverterRule {
    private static final OpenAPISortRule INSTANCE = Config.INSTANCE
        .withConversion(Sort.class, Convention.NONE,
            OpenAPIRel.CONVENTION, "OpenAPISortRule")
        .withRuleFactory(OpenAPISortRule::new)
        .toRule(OpenAPISortRule.class);

    protected OpenAPISortRule(Config config) {
      super(config);
    }

    @Override public RelNode convert(RelNode relNode) {
      final Sort sort = (Sort) relNode;
      final RelTraitSet traitSet = sort.getTraitSet().replace(out).replace(sort.getCollation());
      return new OpenAPISort(relNode.getCluster(), traitSet,
          convert(sort.getInput(), traitSet.replace(org.apache.calcite.rel.RelCollations.EMPTY)),
          sort.getCollation(), sort.offset, sort.fetch);
    }
  }

  /**
   * Utility methods for parsing SQL expressions into OpenAPI calls.
   */
  static class ExpressionTranslator {

    /**
     * Extracts simple equality filters from a condition.
     * Only supports conditions like "field = value".
     */
    static void translateFilter(RexNode condition, OpenAPIRel.Implementor implementor) {
      if (condition.getKind() == SqlKind.EQUALS) {
        RexCall call = (RexCall) condition;
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        if (left instanceof RexCall && right instanceof RexLiteral) {
          // Handle _MAP['fieldName'] = value
          String fieldName = isItemCall((RexCall) left);
          if (fieldName != null) {
            RexLiteral literal = (RexLiteral) right;
            Object value = literal.getValue();
            implementor.filters.put(fieldName, value);
          }
        } else if (left instanceof RexInputRef && right instanceof RexLiteral) {
          // Handle direct field reference (less common in map-based schemas)
          RexInputRef inputRef = (RexInputRef) left;
          RexLiteral literal = (RexLiteral) right;
          String fieldName = "field_" + inputRef.getIndex(); // Simplified
          Object value = literal.getValue();
          implementor.filters.put(fieldName, value);
        }
      } else if (condition.getKind() == SqlKind.AND) {
        // Handle AND conditions recursively
        RexCall call = (RexCall) condition;
        for (RexNode operand : call.getOperands()) {
          translateFilter(operand, implementor);
        }
      }
      // Other condition types would be handled by local filtering
    }

    /**
     * Extracts projection fields from a list of RexNodes.
     */
    static void translateProjection(List<RexNode> projects, OpenAPIRel.Implementor implementor,
        RelDataType rowType) {
      for (int i = 0; i < projects.size(); i++) {
        RexNode project = projects.get(i);
        String fieldName = rowType.getFieldList().get(i).getName();

        if (project instanceof RexCall) {
          // Handle _MAP['fieldName'] projections
          String extractedField = isItemCall((RexCall) project);
          if (extractedField != null) {
            implementor.projections.put(extractedField, Object.class);
          } else {
            implementor.projections.put(fieldName, Object.class);
          }
        } else {
          implementor.projections.put(fieldName, Object.class);
        }
      }
    }
  }
}

/**
 * OpenAPI Filter implementation.
 */
class OpenAPIFilter extends org.apache.calcite.rel.SingleRel implements OpenAPIRel {
  private final RexNode condition;

  protected OpenAPIFilter(org.apache.calcite.plan.RelOptCluster cluster,
      RelTraitSet traitSet, RelNode input, RexNode condition) {
    super(cluster, traitSet, input);
    this.condition = condition;
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    OpenAPIRules.ExpressionTranslator.translateFilter(condition, implementor);
  }

  @Override public RelNode copy(RelTraitSet traitSet, java.util.List<RelNode> inputs) {
    return new OpenAPIFilter(getCluster(), traitSet, sole(inputs), condition);
  }
}

/**
 * OpenAPI Project implementation.
 */
class OpenAPIProject extends org.apache.calcite.rel.SingleRel implements OpenAPIRel {
  private final java.util.List<RexNode> projects;
  private final org.apache.calcite.rel.type.RelDataType rowType;

  protected OpenAPIProject(org.apache.calcite.plan.RelOptCluster cluster,
      RelTraitSet traitSet, RelNode input, java.util.List<RexNode> projects,
      org.apache.calcite.rel.type.RelDataType rowType) {
    super(cluster, traitSet, input);
    this.projects = projects;
    this.rowType = rowType;
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    OpenAPIRules.ExpressionTranslator.translateProjection(projects, implementor, rowType);
  }

  @Override public RelNode copy(RelTraitSet traitSet, java.util.List<RelNode> inputs) {
    return new OpenAPIProject(getCluster(), traitSet, sole(inputs), projects, rowType);
  }

  @Override public org.apache.calcite.rel.type.RelDataType deriveRowType() {
    return rowType;
  }
}

/**
 * OpenAPI Sort implementation.
 */
class OpenAPISort extends org.apache.calcite.rel.SingleRel implements OpenAPIRel {
  private final org.apache.calcite.rel.RelCollation collation;
  private final RexNode offset;
  private final RexNode fetch;

  protected OpenAPISort(org.apache.calcite.plan.RelOptCluster cluster,
      RelTraitSet traitSet, RelNode input, org.apache.calcite.rel.RelCollation collation,
      RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input);
    this.collation = collation;
    this.offset = offset;
    this.fetch = fetch;
  }

  @Override public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());

    // Add sort fields - extract field names from collation
    List<String> fieldNames = OpenAPIRules.openAPIFieldNames(getInput().getRowType());
    for (org.apache.calcite.rel.RelFieldCollation fieldCollation : collation.getFieldCollations()) {
      String fieldName = fieldNames.get(fieldCollation.getFieldIndex());
      implementor.sorts.put(fieldName, fieldCollation.getDirection());
    }

    // Add pagination
    if (offset instanceof RexLiteral) {
      implementor.offset = ((RexLiteral) offset).getValueAs(Long.class);
    }
    if (fetch instanceof RexLiteral) {
      implementor.fetch = ((RexLiteral) fetch).getValueAs(Long.class);
    }
  }

  @Override public RelNode copy(RelTraitSet traitSet, java.util.List<RelNode> inputs) {
    return new OpenAPISort(getCluster(), traitSet, sole(inputs), collation, offset, fetch);
  }
}
