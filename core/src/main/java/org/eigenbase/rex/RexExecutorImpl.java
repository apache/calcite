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
package org.eigenbase.rex;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Modifier;
import java.util.*;

import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.util.Pair;

import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.jdbc.JavaTypeFactoryImpl;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.rules.java.RexToLixTranslator;
import net.hydromatic.optiq.rules.java.RexToLixTranslator.InputGetter;
import net.hydromatic.optiq.runtime.*;

import com.google.common.collect.ImmutableList;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

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
    final JavaTypeFactoryImpl javaTypeFactory = new JavaTypeFactoryImpl();
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
        javaTypeFactory, blockBuilder, getter);
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
   * creates an {@link RexExecutable} that allows to apply the
   * generated code during query processing (filter, projection).
   *
   * @param rowType describes the structure of the input row.
   */
  public RexExecutable getExecutable(final RexBuilder rexBuilder,
      List<RexNode> constExps, final RelDataType rowType) {
    InputGetter getter =  new RexToLixTranslator.InputGetter() {
      final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
      public Expression field(BlockBuilder list, int index) {
        MethodCallExpression recFromCtx = Expressions.call(
            DataContext.ROOT,
            BuiltinMethod.DATA_CONTEXT_GET.method,
            Expressions.constant("inputRecord"));
        Expression recFromCtxCasted =
            RexToLixTranslator.convert(recFromCtx, Object[].class);
        IndexExpression recordAccess = Expressions.arrayIndex(recFromCtxCasted,
            Expressions.constant(index));
        return RexToLixTranslator.convert(recordAccess,
            typeFactory.getJavaClass(rowType.getFieldList().get(index)
                .getType()));
      }
    };

    final String code = compile(rexBuilder, constExps, getter, rowType);
    return new RexExecutable(code, rexBuilder);
  }

  /**
   * Do constant reduction using generated code
   */
  public void reduce(RexBuilder rexBuilder, List<RexNode> constExps,
      List<RexNode> reducedValues) {
    final String code = compile(rexBuilder, constExps,
        new RexToLixTranslator.InputGetter() {
          public Expression field(BlockBuilder list, int index) {
            throw new UnsupportedOperationException();
          }
        });

    try {
      //noinspection unchecked
      Function1<DataContext, Object[]> function =
          (Function1) ClassBodyEvaluator.createFastClassBodyEvaluator(
              new Scanner(null, new StringReader(code)),
              "Reducer",
              Utilities.class,
              new Class[]{Function1.class},
              getClass().getClassLoader());
      Object[] values = function.apply(dataContext);
      assert values.length == constExps.size();
      final List<Object> valueList = Arrays.asList(values);
      for (Pair<RexNode, Object> value : Pair.zip(constExps, valueList)) {
        reducedValues.add(
            rexBuilder.makeLiteral(value.right, value.left.getType(), true));
      }
      Hook.EXPRESSION_REDUCER.run(Pair.of(code, values));
    } catch (CompileException e) {
      throw new RuntimeException("While evaluating " + constExps, e);
    } catch (IOException e) {
      throw new RuntimeException("While evaluating " + constExps, e);
    }
  }
}

// End RexExecutorImpl.java
