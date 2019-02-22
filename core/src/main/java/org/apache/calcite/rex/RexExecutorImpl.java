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
package org.apache.calcite.rex;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.IndexExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;

/**
* Evaluates a {@link RexNode} expression.
*/
public class RexExecutorImpl implements RexExecutor {

  private final DataContext dataContext;

  public RexExecutorImpl(DataContext dataContext) {
    this.dataContext = dataContext;
  }

  private String compile(RexBuilder rexBuilder, List<RexNode> constExps,
      RexToLixTranslator.InputGetter getter) {
    final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    final RelDataType emptyRowType = typeFactory.builder().build();
    return compile(rexBuilder, constExps, getter, emptyRowType);
  }

  private String compile(RexBuilder rexBuilder, List<RexNode> constExps,
      RexToLixTranslator.InputGetter getter, RelDataType rowType) {
    final RexProgramBuilder programBuilder =
        new RexProgramBuilder(rowType, rexBuilder);
    for (RexNode node : constExps) {
      programBuilder.addProject(
          node, "c" + programBuilder.getProjectList().size());
    }
    final JavaTypeFactoryImpl javaTypeFactory =
        new JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem());
    final BlockBuilder blockBuilder = new BlockBuilder();
    final ParameterExpression root0_ =
        Expressions.parameter(Object.class, "root0");
    final ParameterExpression root_ = DataContext.ROOT;
    blockBuilder.add(
        Expressions.declare(
            Modifier.FINAL, root_,
            Expressions.convert_(root0_, DataContext.class)));
    final SqlConformance conformance = SqlConformanceEnum.DEFAULT;
    final RexProgram program = programBuilder.getProgram();
    final List<Expression> expressions =
        RexToLixTranslator.translateProjects(program, javaTypeFactory,
            conformance, blockBuilder, null, root_, getter, null);
    blockBuilder.add(
        Expressions.return_(null,
            Expressions.newArrayInit(Object[].class, expressions)));
    final MethodDeclaration methodDecl =
        Expressions.methodDecl(Modifier.PUBLIC, Object[].class,
            BuiltInMethod.FUNCTION1_APPLY.method.getName(),
            ImmutableList.of(root0_), blockBuilder.toBlock());
    String code = Expressions.toString(methodDecl);
    if (CalciteSystemProperty.DEBUG.value()) {
      Util.debugCode(System.out, code);
    }
    return code;
  }

  /**
   * Creates an {@link RexExecutable} that allows to apply the
   * generated code during query processing (filter, projection).
   *
   * @param rexBuilder Rex builder
   * @param exps Expressions
   * @param rowType describes the structure of the input row.
   */
  public RexExecutable getExecutable(RexBuilder rexBuilder, List<RexNode> exps,
      RelDataType rowType) {
    final JavaTypeFactoryImpl typeFactory =
        new JavaTypeFactoryImpl(rexBuilder.getTypeFactory().getTypeSystem());
    final InputGetter getter = new DataContextInputGetter(rowType, typeFactory);
    final String code = compile(rexBuilder, exps, getter, rowType);
    return new RexExecutable(code, "generated Rex code");
  }

  /**
   * Do constant reduction using generated code.
   */
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps,
      List<RexNode> reducedValues) {
    final String code = compile(rexBuilder, constExps,
        (list, index, storageType) -> {
          throw new UnsupportedOperationException();
        });

    final RexExecutable executable = new RexExecutable(code, constExps);
    executable.setDataContext(dataContext);
    executable.reduce(rexBuilder, constExps, reducedValues);
  }

  /**
   * Implementation of
   * {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter}
   * that reads the values of input fields by calling
   * <code>{@link org.apache.calcite.DataContext#get}("inputRecord")</code>.
   */
  private static class DataContextInputGetter implements InputGetter {
    private final RelDataTypeFactory typeFactory;
    private final RelDataType rowType;

    DataContextInputGetter(RelDataType rowType,
        RelDataTypeFactory typeFactory) {
      this.rowType = rowType;
      this.typeFactory = typeFactory;
    }

    public Expression field(BlockBuilder list, int index, Type storageType) {
      MethodCallExpression recFromCtx = Expressions.call(
          DataContext.ROOT,
          BuiltInMethod.DATA_CONTEXT_GET.method,
          Expressions.constant("inputRecord"));
      Expression recFromCtxCasted =
          RexToLixTranslator.convert(recFromCtx, Object[].class);
      IndexExpression recordAccess = Expressions.arrayIndex(recFromCtxCasted,
          Expressions.constant(index));
      if (storageType == null) {
        final RelDataType fieldType =
            rowType.getFieldList().get(index).getType();
        storageType = ((JavaTypeFactory) typeFactory).getJavaClass(fieldType);
      }
      return RexToLixTranslator.convert(recordAccess, storageType);
    }
  }
}

// End RexExecutorImpl.java
