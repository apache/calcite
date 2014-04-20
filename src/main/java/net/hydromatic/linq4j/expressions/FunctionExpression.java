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
package net.hydromatic.linq4j.expressions;

import net.hydromatic.linq4j.function.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * Represents a strongly typed lambda expression as a data structure in the form
 * of an expression tree. This class cannot be inherited.
 *
 * @param <F> Function type
 */
public final class FunctionExpression<F extends Function<?>>
    extends LambdaExpression {
  public final F function;
  public final BlockStatement body;
  public final List<ParameterExpression> parameterList;
  private F dynamicFunction;
  /**
   * Cache the hash code for the expression
   */
  private int hash;

  private FunctionExpression(Class<F> type, F function, BlockStatement body,
      List<ParameterExpression> parameterList) {
    super(ExpressionType.Lambda, type);
    assert type != null : "type should not be null";
    assert function != null || body != null : "both function and body should "
        + "not be null";
    assert parameterList != null : "parameterList should not be null";
    this.function = function;
    this.body = body;
    this.parameterList = parameterList;
  }

  public FunctionExpression(F function) {
    this((Class) function.getClass(), function, null,
        Collections.<ParameterExpression>emptyList());
  }

  public FunctionExpression(Class<F> type, BlockStatement body,
      List<ParameterExpression> parameters) {
    this(type, null, body, parameters);
  }

  @Override
  public Expression accept(Visitor visitor) {
    visitor = visitor.preVisit(this);
    BlockStatement body = this.body.accept(visitor);
    return visitor.visit(this, body);
  }

  public Invokable compile() {
    return new Invokable() {
      public Object dynamicInvoke(Object... args) {
        final Evaluator evaluator = new Evaluator();
        for (int i = 0; i < args.length; i++) {
          evaluator.push(parameterList.get(i), args[i]);
        }
        return evaluator.evaluate(body);
      }
    };
  }

  public F getFunction() {
    if (function != null) {
      return function;
    }
    if (dynamicFunction == null) {
      final Invokable x = compile();

      //noinspection unchecked
      dynamicFunction = (F) Proxy.newProxyInstance(getClass().getClassLoader(),
          new Class[]{Types.toClass(type)},
          new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args)
              throws Throwable {
              return x.dynamicInvoke(args);
            }
          });
    }
    return dynamicFunction;
  }

  @Override
  void accept(ExpressionWriter writer, int lprec, int rprec) {
    // "new Function1() {
    //    public Result apply(T1 p1, ...) {
    //        <body>
    //    }
    //    // bridge method
    //    public Object apply(Object p1, ...) {
    //        return apply((T1) p1, ...);
    //    }
    // }
    //
    // if any arguments are primitive there is an extra bridge method:
    //
    //  new Function1() {
    //    public double apply(double p1, int p2) {
    //      <body>
    //    }
    //    // box bridge method
    //    public Double apply(Double p1, Integer p2) {
    //      return apply(p1.doubleValue(), p2.intValue());
    //    }
    //    // bridge method
    //    public Object apply(Object p1, Object p2) {
    //      return apply((Double) p1, (Integer) p2);
    //    }
    List<String> params = new ArrayList<String>();
    List<String> bridgeParams = new ArrayList<String>();
    List<String> bridgeArgs = new ArrayList<String>();
    List<String> boxBridgeParams = new ArrayList<String>();
    List<String> boxBridgeArgs = new ArrayList<String>();
    for (ParameterExpression parameterExpression : parameterList) {
      final Type parameterType = parameterExpression.getType();
      final Type parameterBoxType = Types.box(parameterType);
      final String parameterBoxTypeName = Types.className(parameterBoxType);
      params.add(parameterExpression.declString());
      bridgeParams.add(parameterExpression.declString(Object.class));
      bridgeArgs.add("(" + parameterBoxTypeName + ") "
          + parameterExpression.name);

      boxBridgeParams.add(parameterExpression.declString(parameterBoxType));
      boxBridgeArgs.add(parameterExpression.name
          + (Primitive.is(parameterType)
          ? "." + Primitive.of(parameterType).primitiveName + "Value()"
          : ""));
    }
    Type bridgeResultType = Functions.FUNCTION_RESULT_TYPES.get(this.type);
    if (bridgeResultType == null) {
      bridgeResultType = body.getType();
    }
    Type resultType2 = bridgeResultType;
    if (bridgeResultType == Object.class
        && !params.equals(bridgeParams)
        && !(body.getType() instanceof TypeVariable)) {
      resultType2 = body.getType();
    }
    String methodName = getAbstractMethodName();
    writer.append("new ")
        .append(type)
        .append("()")
        .begin(" {\n")
        .append("public ")
        .append(Types.className(resultType2))
        .list(" " + methodName + "(", ", ", ") ", params)
        .append(Blocks.toFunctionBlock(body));

    // Generate an intermediate bridge method if at least one parameter is
    // primitive.
    if (!boxBridgeParams.equals(params)) {
      writer
          .append("public ")
          .append(Types.boxClassName(bridgeResultType))
          .list(" " + methodName + "(", ", ", ") ", boxBridgeParams)
          .begin("{\n")
          .list("return " + methodName + "(\n", ",\n", ");\n", boxBridgeArgs)
          .end("}\n");
    }

    // Generate a bridge method. Argument types are looser (as if every
    // type parameter is set to 'Object').
    //
    // Skip the bridge method if there are no arguments. It would have the
    // same overload as the regular method.
    if (!bridgeParams.equals(params)) {
      writer
        .append("public ")
        .append(Types.boxClassName(bridgeResultType))
        .list(" " + methodName + "(", ", ", ") ", bridgeParams)
        .begin("{\n")
        .list("return " + methodName + "(\n", ",\n", ");\n", bridgeArgs)
        .end("}\n");
    }

    writer.end("}\n");
  }

  private String getAbstractMethodName() {
    if (type.toString().contains("OptiqFlatMapFunction")) {
      return "call"; // FIXME
    }
    return "apply";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    FunctionExpression that = (FunctionExpression) o;

    if (body != null ? !body.equals(that.body) : that.body != null) {
      return false;
    }
    if (function != null ? !function.equals(that.function) : that.function
        != null) {
      return false;
    }
    if (!parameterList.equals(that.parameterList)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = hash;
    if (result == 0) {
      result = super.hashCode();
      result = 31 * result + (function != null ? function.hashCode() : 0);
      result = 31 * result + (body != null ? body.hashCode() : 0);
      result = 31 * result + parameterList.hashCode();
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return result;
  }

  /** Function that can be invoked with a variable number of arguments. */
  public interface Invokable {
    Object dynamicInvoke(Object... args);
  }
}

// End FunctionExpression.java
