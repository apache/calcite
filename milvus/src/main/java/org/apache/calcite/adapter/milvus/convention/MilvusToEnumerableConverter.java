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
package org.apache.calcite.adapter.milvus.convention;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.milvus.factory.MilvusTranslatableTable;
import org.apache.calcite.adapter.milvus.operation.MilvusProjectExpression;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * MilvusToEnumerableConverter converts a relational expression
 * from Milvus calling convention to Enumerable calling convention.
 */
public class MilvusToEnumerableConverter
    extends ConverterImpl
    implements EnumerableRel {
  protected MilvusToEnumerableConverter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new MilvusToEnumerableConverter(
        getCluster(), traitSet, sole(inputs));
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder list = new BlockBuilder();
    final MilvusRel.Implementor milvusImplementor =
        new MilvusRel.Implementor(getCluster().getRexBuilder());
    milvusImplementor.visitChild(0, getInput());

    final Expression root = implementor.getRootExpression();
    final Expression schema =
        Expressions.call(root, BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method);

    // scan
    final List<String> qualifiedTableName = milvusImplementor.table.getQualifiedName();
    final Expression table = getScanInfo(qualifiedTableName, schema);
    final Expression tableExpr =
        Expressions.convert_(table, MilvusTranslatableTable.class);
    // project
    final RelDataType rowType = milvusImplementor.projectRowType != null
        ? milvusImplementor.projectRowType
        : getRowType();

    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), rowType,
            pref.prefer(JavaRowFormat.ARRAY));

    final List<RexNode> projects = milvusImplementor.projects;
    List<Pair<Integer, MilvusProjectExpression>>
        projectInfo = getProjectInfo(projects, rowType, milvusImplementor.rowType, physType);
    final Expression projectInfoExpr =
        list.append("projectRowTypeMapForEnumerator", expressionForProjectPairs(projectInfo));

    Expression enumerable =
        list.append(
            "enumerable", Expressions.call(tableExpr,
                "scan",
                Expressions.constant(""),
                projectInfoExpr));

    list.add(Expressions.return_(null, enumerable));
    return implementor.result(physType, list.toBlock());
  }

  private static Expression getScanInfo(List<String> qualifiedName,
      Expression schema) {
    final String schemaName = qualifiedName.size() > 1 ? qualifiedName.get(0) : null;
    final String tableName = qualifiedName.get(qualifiedName.size() - 1);

    Expression current = schema;

    if (schemaName != null) {
      current =
          Expressions.call(current, BuiltInMethod.SCHEMA_GET_SUB_SCHEMA.method,
              Expressions.constant(schemaName));
      current = Expressions.convert_(current, Schema.class);
    }

    return Expressions.call(current,
        BuiltInMethod.SCHEMA_GET_TABLE.method,
        Expressions.constant(tableName));
  }

  private static List<Pair<Integer, MilvusProjectExpression>> getProjectInfo(List<RexNode> projects,
      RelDataType rowType, RelDataType inputRowType, PhysType physType) {
    List<Pair<Integer, MilvusProjectExpression>> projectInfo = new ArrayList<>();
    if (projects != null) {
      for (int i = 0; i < projects.size(); i++) {
        RexNode project = projects.get(i);
        Class<?> fieldClass = physType.fieldClass(i);
        MilvusProjectExpression expr;

        if (project instanceof RexInputRef) {
          int inputIndex = ((RexInputRef) project).getIndex();
          String originalFieldName = inputRowType.getFieldNames().get(inputIndex);
          expr = new MilvusProjectExpression.InputField(originalFieldName, fieldClass);
        } else if (project instanceof RexLiteral) {
          RexLiteral literal = (RexLiteral) project;
          expr = new MilvusProjectExpression.Constant(fieldClass, literal.getValue3());
        } else {
          throw new UnsupportedOperationException("Unsupported project type");
        }
        projectInfo.add(Pair.of(i, expr));
      }
    } else {
      List<String> inputFields = rowType.getFieldNames();
      for (int i = 0; i < inputFields.size(); i++) {
        String fieldName = inputFields.get(i);
        Class<?> fieldClass = physType.fieldClass(i);
        projectInfo.add(
            Pair.of(i,
                new MilvusProjectExpression.InputField(fieldName, fieldClass)));
      }
    }
    return projectInfo;
  }

  private Expression expressionForProjectExpression(MilvusProjectExpression expr) {
    if (expr instanceof MilvusProjectExpression.InputField) {
      String fieldName = ((MilvusProjectExpression.InputField) expr).getFieldName();
      return Expressions.new_(MilvusProjectExpression.InputField.class,
          Expressions.constant(fieldName),
          Expressions.constant(expr.getClazz(), Class.class));
    } else if (expr instanceof MilvusProjectExpression.Constant) {
      Object value = ((MilvusProjectExpression.Constant) expr).getValue();
      return Expressions.new_(MilvusProjectExpression.Constant.class,
          Expressions.constant(expr.getClazz(), Class.class),
          Expressions.constant(value));
    } else if (expr instanceof MilvusProjectExpression.VectorScore) {
      return Expressions.new_(MilvusProjectExpression.VectorScore.class,
          Expressions.constant(expr.getClazz(), Class.class));
    } else {
      throw new AssertionError("Unknown expression type: " + expr);
    }
  }

  private Expression expressionForProjectPairs(List<Pair<Integer, MilvusProjectExpression>> pairs) {
    List<Expression> pairExpressions = new ArrayList<>();

    for (Pair<Integer, MilvusProjectExpression> pair : pairs) {
      Expression first = Expressions.constant(pair.left, Integer.class);
      Expression second = expressionForProjectExpression(pair.right);
      Type pairType = Types.of(Pair.class, Integer.class, MilvusProjectExpression.class);
      Expression pairExpr = Expressions.new_(pairType, first, second);
      pairExpressions.add(pairExpr);
    }
    return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(Pair.class, pairExpressions));
  }

}
