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
package org.eigenbase.rex;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;

import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import net.hydromatic.linq4j.expressions.*;

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.rules.java.RexToLixTranslator;
import net.hydromatic.optiq.rules.java.RexToLixTranslator.InputGetter;

import com.google.common.collect.ImmutableList;

/**
* Evaluates a {@link RexNode} expression.
*/
public class RexExecutorImpl implements RelOptPlanner.Executor {

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
    final List<Expression> expressions =
        RexToLixTranslator.translateProjects(programBuilder.getProgram(),
        javaTypeFactory, blockBuilder, null, getter);
    blockBuilder.add(
        Expressions.return_(null,
            Expressions.newArrayInit(Object[].class, expressions)));
    final MethodDeclaration methodDecl =
        Expressions.methodDecl(Modifier.PUBLIC, Object[].class,
            BuiltinMethod.FUNCTION1_APPLY.method.getName(),
            ImmutableList.of(root0_), blockBuilder.toBlock());
    String code = Expressions.toString(methodDecl);
    if (OptiqPrepareImpl.DEBUG) {
      System.out.println(code);
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
    final InputGetter getter =
        new DataContextInputGetter(rowType, rexBuilder.getTypeFactory());
    final String code = compile(rexBuilder, exps, getter, rowType);
    return new RexExecutable(code, "generated Rex code");
  }

  /**
   * Do constant reduction using generated code.
   */
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps,
      List<RexNode> reducedValues) {
    final String code = compile(rexBuilder, constExps,
        new RexToLixTranslator.InputGetter() {
          public Expression field(BlockBuilder list, int index,
              Type storageType) {
            throw new UnsupportedOperationException();
          }
        });

    final RexExecutable executable = new RexExecutable(code, constExps);
    executable.setDataContext(dataContext);
    executable.reduce(rexBuilder, constExps, reducedValues);
  }

  /**
   * Implementation of
   * {@link net.hydromatic.optiq.rules.java.RexToLixTranslator.InputGetter}
   * that reads the values of input fields by calling
   * <code>{@link net.hydromatic.optiq.DataContext#get}("inputRecord")</code>.
   */
  private static class DataContextInputGetter implements InputGetter {
    private final RelDataTypeFactory typeFactory;
    private final RelDataType rowType;

    public DataContextInputGetter(RelDataType rowType,
        RelDataTypeFactory typeFactory) {
      this.rowType = rowType;
      this.typeFactory = typeFactory;
    }

    public Expression field(BlockBuilder list, int index, Type storageType) {
      MethodCallExpression recFromCtx = Expressions.call(
          DataContext.ROOT,
          BuiltinMethod.DATA_CONTEXT_GET.method,
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
