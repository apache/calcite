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
package org.apache.calcite.adapter.elasticsearch;

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Rules and relational operators for
 * {@link ElasticsearchRel#CONVENTION ELASTICSEARCH}
 * calling convention.
 */
class ElasticsearchRules {
  static final RelOptRule[] RULES = {
      ElasticsearchSortRule.INSTANCE,
      ElasticsearchFilterRule.INSTANCE,
      ElasticsearchProjectRule.INSTANCE,
      ElasticsearchAggregateRule.INSTANCE
  };

  private ElasticsearchRules() {}

  /**
   * Returns 'string' if it is a call to item['string'], null otherwise.
   * @param call current relational expression
   * @return literal value
   */
  static String isItem(RexCall call) {
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

  static boolean isItem(RexNode node) {
    final Boolean result = node.accept(new RexVisitorImpl<Boolean>(false) {
      @Override public Boolean visitCall(final RexCall call) {
        return isItem(call) != null;
      }
    });
    return Boolean.TRUE.equals(result);
  }

  static List<String> elasticsearchFieldNames(final RelDataType rowType) {
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

  static String quote(String s) {
    return "\"" + s + "\"";
  }

  static String stripQuotes(String s) {
    return s.length() > 1 && s.startsWith("\"") && s.endsWith("\"")
        ? s.substring(1, s.length() - 1) : s;
  }

  /**
   * Translator from {@link RexNode} to strings in Elasticsearch's expression
   * language.
   */
  static class RexToElasticsearchTranslator extends RexVisitorImpl<String> {
    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    RexToElasticsearchTranslator(JavaTypeFactory typeFactory, List<String> inFields) {
      super(true);
      this.typeFactory = typeFactory;
      this.inFields = inFields;
    }

    @Override public String visitLiteral(RexLiteral literal) {
      if (literal.getValue() == null) {
        return "null";
      }
      return "\"literal\":\""
        + RexToLixTranslator.translateLiteral(literal, literal.getType(),
          typeFactory, RexImpTable.NullAs.NOT_POSSIBLE)
        + "\"";
    }

    @Override public String visitInputRef(RexInputRef inputRef) {
      return quote(inFields.get(inputRef.getIndex()));
    }

    @Override public String visitCall(RexCall call) {
      final String name = isItem(call);
      if (name != null) {
        return name;
      }

      final List<String> strings = visitList(call.operands);
      if (call.getKind() == SqlKind.CAST) {
        return strings.get(0).startsWith("$") ? strings.get(0).substring(1) : strings.get(0);
      }
      if (call.getOperator() == SqlStdOperatorTable.ITEM) {
        final RexNode op1 = call.getOperands().get(1);
        if (op1 instanceof RexLiteral && op1.getType().getSqlTypeName() == SqlTypeName.INTEGER) {
          return stripQuotes(strings.get(0)) + "[" + ((RexLiteral) op1).getValue2() + "]";
        }
      }
      throw new IllegalArgumentException("Translation of " + call
          + " is not supported by ElasticsearchProject");
    }

    List<String> visitList(List<RexNode> list) {
      final List<String> strings = new ArrayList<>();
      for (RexNode node: list) {
        strings.add(node.accept(this));
      }
      return strings;
    }
  }

  /**
   * Base class for planner rules that convert a relational expression to
   * Elasticsearch calling convention.
   */
  abstract static class ElasticsearchConverterRule extends ConverterRule {
    final Convention out;

    ElasticsearchConverterRule(Class<? extends RelNode> clazz, RelTrait in, Convention out,
        String description) {
      super(clazz, in, out, description);
      this.out = out;
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to an
   * {@link ElasticsearchSort}.
   */
  private static class ElasticsearchSortRule extends ElasticsearchConverterRule {
    private static final ElasticsearchSortRule INSTANCE =
        new ElasticsearchSortRule();

    private ElasticsearchSortRule() {
      super(Sort.class, Convention.NONE, ElasticsearchRel.CONVENTION,
          "ElasticsearchSortRule");
    }

    @Override public RelNode convert(RelNode relNode) {
      final Sort sort = (Sort) relNode;
      final RelTraitSet traitSet = sort.getTraitSet().replace(out).replace(sort.getCollation());
      return new ElasticsearchSort(relNode.getCluster(), traitSet,
        convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)), sort.getCollation(),
        sort.offset, sort.fetch);
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to an
   * {@link ElasticsearchFilter}.
   */
  private static class ElasticsearchFilterRule extends ElasticsearchConverterRule {
    private static final ElasticsearchFilterRule INSTANCE = new ElasticsearchFilterRule();

    private ElasticsearchFilterRule() {
      super(LogicalFilter.class, Convention.NONE, ElasticsearchRel.CONVENTION,
        "ElasticsearchFilterRule");
    }

    @Override public RelNode convert(RelNode relNode) {
      final LogicalFilter filter = (LogicalFilter) relNode;
      final RelTraitSet traitSet = filter.getTraitSet().replace(out);
      return new ElasticsearchFilter(relNode.getCluster(), traitSet,
        convert(filter.getInput(), out),
        filter.getCondition());
    }
  }

  /**
   * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalAggregate}
   * to an {@link ElasticsearchAggregate}.
   */
  private static class ElasticsearchAggregateRule extends ElasticsearchConverterRule {
    static final RelOptRule INSTANCE = new ElasticsearchAggregateRule();

    private ElasticsearchAggregateRule() {
      super(LogicalAggregate.class, Convention.NONE, ElasticsearchRel.CONVENTION,
          "ElasticsearchAggregateRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalAggregate agg = (LogicalAggregate) rel;
      final RelTraitSet traitSet = agg.getTraitSet().replace(out);
      try {
        return new ElasticsearchAggregate(
            rel.getCluster(),
            traitSet,
            convert(agg.getInput(), traitSet.simplify()),
            agg.indicator,
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList());
      } catch (InvalidRelException e) {
        return null;
      }
    }
  }


  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
   * to an {@link ElasticsearchProject}.
   */
  private static class ElasticsearchProjectRule extends ElasticsearchConverterRule {
    private static final ElasticsearchProjectRule INSTANCE = new ElasticsearchProjectRule();

    private ElasticsearchProjectRule() {
      super(LogicalProject.class, Convention.NONE, ElasticsearchRel.CONVENTION,
        "ElasticsearchProjectRule");
    }

    @Override public RelNode convert(RelNode relNode) {
      final LogicalProject project = (LogicalProject) relNode;
      final RelTraitSet traitSet = project.getTraitSet().replace(out);
      return new ElasticsearchProject(project.getCluster(), traitSet,
        convert(project.getInput(), out), project.getProjects(), project.getRowType());
    }
  }
}

// End ElasticsearchRules.java
