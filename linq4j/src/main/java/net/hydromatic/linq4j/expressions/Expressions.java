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

import net.hydromatic.linq4j.Extensions;
import net.hydromatic.linq4j.function.*;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Utility methods for expressions, including a lot of factory methods.
 */
public abstract class Expressions {
  private Expressions() {}

  /**
   * Converts a list of expressions to Java source code, optionally emitting
   * extra type information in generics.
   */
  public static String toString(List<? extends Node> expressions, String sep,
      boolean generics) {
    final ExpressionWriter writer = new ExpressionWriter(generics);
    for (Node expression : expressions) {
      writer.write(expression);
      writer.append(sep);
    }
    return writer.toString();
  }

  /**
   * Converts an expression to Java source code.
   */
  public static String toString(Node expression) {
    return toString(Collections.singletonList(expression), "", true);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * addition operation that does not have overflow checking.
   */
  public static BinaryExpression add(Expression left, Expression right) {
    return makeBinary(ExpressionType.Add, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * addition operation that does not have overflow checking. The
   * implementing method can be specified.
   */
  public static BinaryExpression add(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents an addition
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression addAssign(Expression left, Expression right) {
    return makeBinary(ExpressionType.AddAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an addition
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression addAssign(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents an addition
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression addAssign(Expression left, Expression right,
      Method method, LambdaExpression lambdaLeft,
      LambdaExpression lambdaRight) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents an addition
   * assignment operation that has overflow checking.
   */
  public static BinaryExpression addAssignChecked(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.AddAssignChecked, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an addition
   * assignment operation that has overflow checking.
   */
  public static BinaryExpression addAssignChecked(Expression left,
      Expression right, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents an addition
   * assignment operation that has overflow checking.
   */
  public static BinaryExpression addAssignChecked(Expression left,
      Expression right, Method method, LambdaExpression lambdaExpression) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * addition operation that has overflow checking.
   */
  public static BinaryExpression addChecked(Expression left, Expression right) {
    return makeBinary(ExpressionType.AddChecked, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * addition operation that has overflow checking. The implementing
   * method can be specified.
   */
  public static BinaryExpression addChecked(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a bitwise AND
   * operation.
   */
  public static BinaryExpression and(Expression left, Expression right) {
    return makeBinary(ExpressionType.And, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise AND
   * operation. The implementing method can be specified.
   */
  public static BinaryExpression and(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a conditional AND
   * operation that evaluates the second operand only if the first
   * operand evaluates to true.
   */
  public static BinaryExpression andAlso(Expression left, Expression right) {
    return makeBinary(ExpressionType.AndAlso, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a conditional AND
   * operation that evaluates the second operand only if the first
   * operand is resolved to true. The implementing method can be
   * specified.
   */
  public static BinaryExpression andAlso(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a bitwise AND
   * assignment operation.
   */
  public static BinaryExpression andAssign(Expression left, Expression right) {
    return makeBinary(ExpressionType.AndAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise AND
   * assignment operation.
   */
  public static BinaryExpression andAssign(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a bitwise AND
   * assignment operation.
   */
  public static BinaryExpression andAssign(Expression left, Expression right,
      Method method, LambdaExpression lambdaExpression) {
    throw Extensions.todo();
  }

  /**
   * Creates an expression that represents applying an array
   * index operator to an array of rank one.
   */
  public static IndexExpression arrayIndex(Expression array,
      Expression indexExpression) {
    return new IndexExpression(array,
        Collections.singletonList(indexExpression));
  }

  /**
   * Creates a UnaryExpression that represents an expression for
   * obtaining the length of a one-dimensional array.
   */
  public static UnaryExpression arrayLength(Expression array) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents an assignment
   * operation.
   */
  public static BinaryExpression assign(Expression left, Expression right) {
    return makeBinary(ExpressionType.Assign, left, right);
  }

  /**
   * Creates a MemberAssignment that represents the initialization
   * of a field or property.
   */
  public static MemberAssignment bind(Member member, Expression right) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberAssignment that represents the initialization
   * of a member by using a property accessor method.
   */
  public static MemberAssignment bind(Method method, Expression expression) {
    throw Extensions.todo();
  }

  /**
   * Creates a BlockExpression that contains the given statements.
   */
  public static BlockStatement block(
      Iterable<? extends Statement> statements) {
    return block((Type) null, statements);
  }

  /**
   * Creates a BlockExpression that contains the given statements,
   * using varargs.
   */
  public static BlockStatement block(Statement... statements) {
    return block(toList(statements));
  }

  /**
   * Creates a BlockExpression that contains the given expressions,
   * has no variables and has specific result type.
   */
  public static BlockStatement block(Type type,
      Iterable<? extends Statement> expressions) {
    List<Statement> list = toList(expressions);
    if (type == null) {
      if (list.size() > 0) {
        type = list.get(list.size() - 1).getType();
      } else {
        type = Void.TYPE;
      }
    }
    return new BlockStatement(list, type);
  }

  /**
   * Creates a BlockExpression that contains the given statements
   * and has a specific result type, using varargs.
   */
  public static BlockStatement block(Type type, Statement... statements) {
    return block(type, toList(statements));
  }

  /**
   * Creates a GotoExpression representing a break statement.
   */
  public static GotoStatement break_(LabelTarget labelTarget) {
    return new GotoStatement(GotoExpressionKind.Break, null, null);
  }

  /**
   * Creates a GotoExpression representing a break statement. The
   * value passed to the label upon jumping can be specified.
   */
  public static GotoStatement break_(LabelTarget labelTarget,
      Expression expression) {
    return new GotoStatement(GotoExpressionKind.Break, null, expression);
  }

  /**
   * Creates a GotoExpression representing a break statement with
   * the specified type.
   */
  public static GotoStatement break_(LabelTarget labelTarget, Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a GotoExpression representing a break statement with
   * the specified type. The value passed to the label upon jumping
   * can be specified.
   */
  public static GotoStatement break_(LabelTarget labelTarget,
      Expression expression, Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a MethodCallExpression that represents a call to a
   * static method that has arguments.
   */
  public static MethodCallExpression call(Method method,
      Iterable<? extends Expression> arguments) {
    return new MethodCallExpression(method, null, toList(arguments));
  }

  /**
   * Creates a MethodCallExpression that represents a call to a
   * static method that has arguments, using varargs.
   */
  public static MethodCallExpression call(Method method,
      Expression... arguments) {
    return new MethodCallExpression(method, null, toList(arguments));
  }

  /**
   * Creates a MethodCallExpression that represents a call to a
   * method that takes arguments.
   */
  public static MethodCallExpression call(Expression expression, Method method,
      Iterable<? extends Expression> arguments) {
    return new MethodCallExpression(method, expression, toList(arguments));
  }

  /**
   * Creates a MethodCallExpression that represents a call to a
   * method that takes arguments, using varargs.
   */
  public static MethodCallExpression call(Expression expression, Method method,
      Expression... arguments) {
    return new MethodCallExpression(method, expression, toList(arguments));
  }

  /**
   * Creates a MethodCallExpression that represents a call to a
   * method that takes arguments, with an explicit return type.
   *
   * <p>The return type must be consistent with the return type of the method,
   * but may contain extra information, such as type parameters.</p>
   *
   * <p>The {@code expression} argument may be null if and only if the method
   * is static.</p>
   */
  public static MethodCallExpression call(Type returnType,
      Expression expression, Method method,
      Iterable<? extends Expression> arguments) {
    return new MethodCallExpression(returnType, method, expression,
        toList(arguments));
  }

  /**
   * Creates a MethodCallExpression that represents a call to a
   * method that takes arguments, with an explicit return type, with varargs.
   *
   * <p>The return type must be consistent with the return type of the method,
   * but may contain extra information, such as type parameters.</p>
   *
   * <p>The {@code expression} argument may be null if and only if the method
   * is static.</p>
   */
  public static MethodCallExpression call(Type returnType,
      Expression expression, Method method,
      Expression... arguments) {
    return new MethodCallExpression(returnType, method, expression,
        toList(arguments));
  }

  /**
   * Creates a MethodCallExpression that represents a call to an
   * instance method by calling the appropriate factory method.
   */
  public static MethodCallExpression call(Expression target, String methodName,
      Iterable<? extends Expression> arguments) {
    Method method;
    try {
      //noinspection unchecked
      method = Types.toClass(target.getType())
          .getMethod(methodName, Types.toClassArray(arguments));
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("while resolving method '" + methodName
          + "' in class " + target.getType(), e);
    }
    return call(target, method, arguments);
  }

  /**
   * Creates a MethodCallExpression that represents a call to an
   * instance method by calling the appropriate factory method, using varargs.
   */
  public static MethodCallExpression call(Expression target, String methodName,
      Expression... arguments) {
    return call(target, methodName, toList(arguments));
  }

  /**
   * Creates a MethodCallExpression that represents a call to a
   * static method by calling the
   * appropriate factory method.
   */
  public static MethodCallExpression call(Type type, String methodName,
      Iterable<? extends Expression> arguments) {
    Method method = Types.lookupMethod(Types.toClass(type), methodName,
        Types.toClassArray(arguments));
    return new MethodCallExpression(method, null, toList(arguments));
  }

  /**
   * Creates a MethodCallExpression that represents a call to a
   * static method by calling the
   * appropriate factory method, using varargs.
   */
  public static MethodCallExpression call(Type type, String methodName,
      Expression... arguments) {
    return call(type, methodName, toList(arguments));
  }

  /**
   * Creates a CatchBlock representing a catch statement with a
   * reference to the caught Exception object for use in the handler
   * body.
   */
  public static CatchBlock catch_(ParameterExpression parameter,
      Statement statement) {
    return new CatchBlock(parameter, statement);
  }

  /**
   * Creates a DebugInfoExpression for clearing a sequence
   * point.
   */
  public static void clearDebugInfo() {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a coalescing
   * operation.
   */
  public static BinaryExpression coalesce(Expression left, Expression right) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a coalescing
   * operation, given a conversion function.
   */
  public static BinaryExpression coalesce(Expression left, Expression right,
      LambdaExpression lambdaExpression) {
    throw Extensions.todo();
  }

  /**
   * Creates a ConditionalExpression that represents a conditional
   * statement.
   */
  public static Expression condition(Expression test, Expression ifTrue,
      Expression ifFalse) {
    return makeTernary(ExpressionType.Conditional, test, ifTrue, ifFalse);
  }

  private static Type box(Type type) {
    Primitive primitive = Primitive.of(type);
    if (primitive != null) {
      return primitive.boxClass;
    }
    return type;
  }

  /** Returns whether an expression always evaluates to null. */
  public static boolean isConstantNull(Expression e) {
    return e instanceof ConstantExpression
           && ((ConstantExpression) e).value == null;
  }

  /**
   * Creates a ConditionalExpression that represents a conditional
   * statement.
   *
   * <p>This method allows explicitly unifying the result type of the
   * conditional expression in cases where the types of ifTrue and ifFalse
   * expressions are not equal. Types of both ifTrue and ifFalse must be
   * implicitly reference assignable to the result type. The type is allowed
   * to be {@link Void#TYPE void}.</p>
   */
  public static ConditionalExpression condition(Expression test,
      Expression ifTrue, Expression ifFalse, Type type) {
    return new ConditionalExpression(Arrays.<Node>asList(test, ifFalse, ifTrue),
        type);
  }

  /**
   * Creates a ConstantExpression that has the Value property set
   * to the specified value.
   *
   * <p>Does the right thing for null, String, primitive values (e.g. int 12,
   * short 12, double 3.14 and boolean false), boxed primitives
   * (e.g. Integer.valueOf(12)), enums, classes, BigDecimal, BigInteger,
   * classes that have a constructor with a parameter for each field, and
   * arrays.</p>
   */
  public static ConstantExpression constant(Object value) {
    Class type;
    if (value == null) {
      return ConstantUntypedNull.INSTANCE;
    } else {
      final Class clazz = value.getClass();
      final Primitive primitive = Primitive.ofBox(clazz);
      if (primitive != null) {
        type = primitive.primitiveClass;
      } else {
        type = clazz;
      }
    }
    return new ConstantExpression(type, value);
  }

  /**
   * Creates a ConstantExpression that has the Value and Type
   * properties set to the specified values.
   */
  public static ConstantExpression constant(Object value, Type type) {
    if (value != null && type instanceof Class) {
      // Fix up value so that it matches type.
      Class clazz = (Class) type;
      Primitive primitive = Primitive.ofBoxOr(clazz);
      if (primitive != null) {
        clazz = primitive.boxClass;
      }
      if (!clazz.isInstance(value)) {
        String stringValue = String.valueOf(value);
        if (type == BigDecimal.class) {
          value = new BigDecimal(stringValue);
        }
        if (type == BigInteger.class) {
          value = new BigInteger(stringValue);
        }
        if (primitive != null) {
          value = primitive.parse(stringValue);
        }
      }
    }
    return new ConstantExpression(type, value);
  }

  /**
   * Creates a GotoExpression representing a continue statement.
   */
  public static GotoStatement continue_(LabelTarget labelTarget) {
    throw Extensions.todo();
  }

  /**
   * Creates a GotoExpression representing a continue statement
   * with the specified type.
   */
  public static GotoStatement continue_(LabelTarget labelTarget, Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a UnaryExpression that represents a type conversion
   * operation.
   */
  public static UnaryExpression convert_(Expression expression, Type type) {
    return new UnaryExpression(ExpressionType.Convert, type, expression);
  }

  /**
   * Creates a UnaryExpression that represents a conversion
   * operation for which the implementing method is specified.
   */
  public static UnaryExpression convert_(Expression expression, Type type,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a UnaryExpression that represents a conversion
   * operation that throws an exception if the target type is
   * overflowed.
   */
  public static UnaryExpression convertChecked(Expression expression,
      Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a UnaryExpression that represents a conversion
   * operation that throws an exception if the target type is
   * overflowed and for which the implementing method is
   * specified.
   */
  public static UnaryExpression convertChecked_(Expression expression,
      Type type, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a DebugInfoExpression with the specified span.
   */
  public static void debugInfo() {
    throw Extensions.todo();
  }

  /**
   * Creates a UnaryExpression that represents the decrementing of
   * the expression by 1.
   */
  public static UnaryExpression decrement(Expression expression) {
    throw Extensions.todo();
  }

  /**
   * Creates a UnaryExpression that represents the decrementing of
   * the expression by 1.
   */
  public static UnaryExpression decrement(Expression expression,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a DefaultExpression that has the Type property set to
   * the specified type.
   */
  public static DefaultExpression default_() {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * division operation.
   */
  public static BinaryExpression divide(Expression left, Expression right) {
    return makeBinary(ExpressionType.Divide, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * division operation. The implementing method can be
   * specified.
   */
  public static BinaryExpression divide(Expression left, Expression right,
      Method method) {
    return makeBinary(ExpressionType.Divide, left, right, shouldLift(left,
        right, method), method);
  }

  /**
   * Creates a BinaryExpression that represents a division
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression divideAssign(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.DivideAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a division
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression divideAssign(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a division
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression divideAssign(Expression left, Expression right,
      Method method, LambdaExpression lambdaExpression) {
    throw Extensions.todo();
  }

  /**
   * Creates a DynamicExpression that represents a dynamic
   * operation bound by the provided CallSiteBinder.
   */
  public static DynamicExpression dynamic(CallSiteBinder binder, Type type,
      Iterable<? extends Expression> expressions) {
    throw Extensions.todo();
  }

  /**
   * Creates a {@code DynamicExpression} that represents a dynamic
   * operation bound by the provided {@code CallSiteBinder}, using varargs.
   */
  public static DynamicExpression dynamic(CallSiteBinder binder, Type type,
      Expression... expression) {
    throw Extensions.todo();
  }

  /**
   * Creates an {@code ElementInit}, given an {@code Iterable<T>} as the second
   * argument.
   */
  public static ElementInit elementInit(Method method,
      Iterable<? extends Expression> expressions) {
    throw Extensions.todo();
  }

  /**
   * Creates an ElementInit, given an array of values as the second
   * argument, using varargs.
   */
  public static ElementInit elementInit(Method method,
      Expression... expressions) {
    throw Extensions.todo();
  }

  /**
   * Creates an empty expression that has Void type.
   */
  public static DefaultExpression empty() {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents an equality
   * comparison.
   */
  public static BinaryExpression equal(Expression left, Expression right) {
    return makeBinary(ExpressionType.Equal, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an equality
   * comparison. The implementing method can be specified.
   */
  public static BinaryExpression equal(Expression expression0,
      Expression expression1, boolean liftToNull, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a bitwise XOR
   * operation, using op_ExclusiveOr for user-defined types.
   */
  public static BinaryExpression exclusiveOr(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.ExclusiveOr, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise XOR
   * operation, using op_ExclusiveOr for user-defined types. The
   * implementing method can be specified.
   */
  public static BinaryExpression exclusiveOr(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a bitwise XOR
   * assignment operation, using op_ExclusiveOr for user-defined
   * types.
   */
  public static BinaryExpression exclusiveOrAssign(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.ExclusiveOrAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise XOR
   * assignment operation, using op_ExclusiveOr for user-defined
   * types.
   */
  public static BinaryExpression exclusiveOrAssign(Expression left,
      Expression right, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a bitwise XOR
   * assignment operation, using op_ExclusiveOr for user-defined
   * types.
   */
  public static BinaryExpression exclusiveOrAssign(Expression left,
      Expression right, Method method, LambdaExpression lambdaExpression) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberExpression that represents accessing a field.
   */
  public static MemberExpression field(Expression expression, Field field) {
    return makeMemberAccess(expression, Types.field(field));
  }

  /**
   * Creates a MemberExpression that represents accessing a field.
   */
  public static MemberExpression field(Expression expression,
      PseudoField field) {
    return makeMemberAccess(expression, field);
  }

  /**
   * Creates a MemberExpression that represents accessing a field
   * given the name of the field.
   */
  public static MemberExpression field(Expression expression,
      String fieldName) {
    PseudoField field = Types.getField(fieldName, expression.getType());
    return makeMemberAccess(expression, field);
  }

  /**
   * Creates a MemberExpression that represents accessing a field.
   */
  public static MemberExpression field(Expression expression, Type type,
      String fieldName) {
    PseudoField field = Types.getField(fieldName, type);
    return makeMemberAccess(expression, field);
  }

  /**
   * Creates a Type object that represents a generic System.Action
   * delegate type that has specific type arguments.
   */
  public static Class getActionType(Class... typeArgs) {
    throw Extensions.todo();
  }

  /**
   * Gets a Type object that represents a generic System.Func or
   * System.Action delegate type that has specific type
   * arguments.
   */
  public static Class getDelegateType(Class... typeArgs) {
    throw Extensions.todo();
  }

  /**
   * Creates a Type object that represents a generic System.Func
   * delegate type that has specific type arguments. The last type
   * argument specifies the return type of the created delegate.
   */
  public static Class getFuncType(Class... typeArgs) {
    throw Extensions.todo();
  }

  /**
   * Creates a GotoExpression representing a "go to" statement.
   */
  public static GotoStatement goto_(LabelTarget labelTarget) {
    throw Extensions.todo();
  }

  /**
   * Creates a GotoExpression representing a "go to" statement. The
   * value passed to the label upon jumping can be specified.
   */
  public static GotoStatement goto_(LabelTarget labelTarget,
      Expression expression) {
    throw Extensions.todo();
  }

  /**
   * Creates a GotoExpression representing a "go to" statement with
   * the specified type.
   */
  public static GotoStatement goto_(LabelTarget labelTarget, Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a GotoExpression representing a "go to" statement with
   * the specified type. The value passed to the label upon jumping
   * can be specified.
   */
  public static GotoStatement goto_(LabelTarget labelTarget,
      Expression expression, Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a "greater than"
   * numeric comparison.
   */
  public static BinaryExpression greaterThan(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.GreaterThan, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a "greater than"
   * numeric comparison. The implementing method can be
   * specified.
   */
  public static BinaryExpression greaterThan(Expression left, Expression right,
      boolean liftToNull, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a "greater than or
   * equal" numeric comparison.
   */
  public static BinaryExpression greaterThanOrEqual(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.GreaterThanOrEqual, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a "greater than or
   * equal" numeric comparison.
   */
  public static BinaryExpression greaterThanOrEqual(Expression left,
      Expression right, boolean liftToNull, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a ConditionalExpression that represents a conditional
   * block with an if statement.
   */
  public static ConditionalStatement ifThen(Expression test, Node ifTrue) {
    return new ConditionalStatement(Arrays.<Node>asList(test, ifTrue));
  }

  /**
   * Creates a ConditionalExpression that represents a conditional
   * block with if and else statements.
   */
  public static ConditionalStatement ifThenElse(Expression test, Node ifTrue,
      Node ifFalse) {
    return new ConditionalStatement(Arrays.<Node>asList(test, ifTrue, ifFalse));
  }

  /**
   * Creates a ConditionalExpression that represents a conditional
   * block with if and else statements:
   * <code>if (test) stmt1 [ else if (test2) stmt2 ]... [ else stmtN ]</code>.
   */
  public static ConditionalStatement ifThenElse(Expression test,
      Node... nodes) {
    return ifThenElse(new FluentArrayList<Node>().append(test)
        .appendAll(nodes));
  }

  /**
   * Creates a ConditionalExpression that represents a conditional
   * block with if and else statements:
   * <code>if (test) stmt1 [ else if (test2) stmt2 ]... [ else stmtN ]</code>.
   */
  public static ConditionalStatement ifThenElse(Iterable<? extends Node>
                                                    nodes) {
    List<Node> list = toList(nodes);
    assert list.size() >= 2 : "At least one test and one statement is required";
    return new ConditionalStatement(list);
  }

  /**
   * Creates a UnaryExpression that represents the incrementing of
   * the expression value by 1.
   */
  public static UnaryExpression increment(Expression expression) {
    throw Extensions.todo();
  }

  /**
   * Creates a UnaryExpression that represents the incrementing of
   * the expression by 1.
   */
  public static UnaryExpression increment(Expression expression,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates an InvocationExpression that applies a delegate or
   * lambda expression to a list of argument expressions.
   */
  public static InvocationExpression invoke(Expression expression,
      Iterable<? extends Expression> arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates an InvocationExpression that applies a delegate or
   * lambda expression to a list of argument expressions, using varargs.
   */
  public static InvocationExpression invoke(Expression expression,
      Expression... arguments) {
    throw Extensions.todo();
  }

  /**
   * Returns whether the expression evaluates to false.
   */
  public static UnaryExpression isFalse(Expression expression) {
    throw Extensions.todo();
  }

  /**
   * Returns whether the expression evaluates to false.
   */
  public static UnaryExpression isFalse(Expression expression, Method method) {
    throw Extensions.todo();
  }

  /**
   * Returns whether the expression evaluates to true.
   */
  public static UnaryExpression isTrue(Expression expression) {
    throw Extensions.todo();
  }

  /**
   * Returns whether the expression evaluates to true.
   */
  public static UnaryExpression isTrue(Expression expression, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a LabelTarget representing a label with X type and
   * no name.
   */
  public static LabelTarget label() {
    throw Extensions.todo();
  }

  /**
   * Creates a LabelExpression representing a label without a
   * default value.
   */
  public static LabelStatement label(LabelTarget labelTarget) {
    throw Extensions.todo();
  }

  /**
   * Creates a LabelTarget representing a label with X type and
   * the given name.
   */
  public static LabelTarget label(String name) {
    throw Extensions.todo();
  }

  /**
   * Creates a LabelTarget representing a label with the given
   * type.
   */
  public static LabelTarget label(Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a LabelExpression representing a label with the given
   * default value.
   */
  public static LabelStatement label(LabelTarget labelTarget,
      Expression expression) {
    throw Extensions.todo();
  }

  /**
   * Creates a LabelTarget representing a label with the given type
   * and name.
   */
  public static LabelTarget label(Type type, String name) {
    throw Extensions.todo();
  }

  /**
   * Creates a FunctionExpression from an actual function.
   */
  public static <F extends Function<?>> FunctionExpression<F> lambda(
      F function) {
    // REVIEW: Check that that function class is non-inner, has a public
    // default constructor, etc.?

    //noinspection unchecked
    return new FunctionExpression<F>(function);
  }

  /**
   * Creates a LambdaExpression by first constructing a delegate
   * type.
   */
  public static <F extends Function<?>> FunctionExpression<F> lambda(
      BlockStatement body,
      Iterable<? extends ParameterExpression> parameters) {
    final List<ParameterExpression> parameterList = toList(parameters);
    @SuppressWarnings("unchecked")
    Class<F> type = deduceType(parameterList, body.getType());
    return new FunctionExpression<F>(type, body, parameterList);
  }

  /**
   * Creates a LambdaExpression by first constructing a delegate
   * type, using varargs.
   */
  public static <F extends Function<?>> FunctionExpression<F> lambda(
      BlockStatement body, ParameterExpression... parameters) {
    return lambda(body, toList(parameters));
  }

  /**
   * Creates an Expression where the delegate type {@code F} is
   * known at compile time.
   */
  public static <F extends Function<?>> FunctionExpression<F> lambda(
      Expression body, Iterable<? extends ParameterExpression> parameters) {
    return lambda(Blocks.toFunctionBlock(body), parameters);
  }

  /**
   * Creates an Expression where the delegate type {@code F} is
   * known at compile time, using varargs.
   */
  public static <F extends Function<?>> FunctionExpression<F> lambda(
      Expression body, ParameterExpression... parameters) {
    return lambda(Blocks.toFunctionBlock(body), toList(parameters));
  }

  /**
   * Creates a LambdaExpression by first constructing a delegate
   * type.
   *
   * <p>It can be used when the delegate type is not known at compile time.
   */
  public static <T, F extends Function<? extends T>> FunctionExpression<F>
  lambda(Class<F> type, BlockStatement body,
      Iterable<? extends ParameterExpression> parameters) {
    return new FunctionExpression<F>(type, body, toList(parameters));
  }

  /**
   * Creates a LambdaExpression by first constructing a delegate
   * type, using varargs.
   *
   * <p>It can be used when the delegate type is not known at compile time.
   */
  public static <T, F extends Function<? extends T>> FunctionExpression<F>
  lambda(Class<F> type, BlockStatement body,
      ParameterExpression... parameters) {
    return lambda(type, body, toList(parameters));
  }

  /**
   * Creates a LambdaExpression by first constructing a delegate
   * type.
   *
   * <p>It can be used when the delegate type is not known at compile time.
   */
  public static <T, F extends Function<? extends T>> FunctionExpression<F>
  lambda(Class<F> type, Expression body,
      Iterable<? extends ParameterExpression> parameters) {
    return lambda(type, Blocks.toFunctionBlock(body), toList(parameters));
  }

  /**
   * Creates a LambdaExpression by first constructing a delegate
   * type, using varargs.
   *
   * <p>It can be used when the delegate type is not known at compile time.
   */
  public static <T, F extends Function<? extends T>> FunctionExpression<F>
  lambda(Class<F> type, Expression body, ParameterExpression... parameters) {
    return lambda(type, Blocks.toFunctionBlock(body), toList(parameters));
  }

  /**
   * Creates a BinaryExpression that represents a bitwise
   * left-shift operation.
   */
  public static BinaryExpression leftShift(Expression left, Expression right) {
    return makeBinary(ExpressionType.LeftShift, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise
   * left-shift operation.
   */
  public static BinaryExpression leftShift(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a bitwise
   * left-shift assignment operation.
   */
  public static BinaryExpression leftShiftAssign(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.LeftShiftAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise
   * left-shift assignment operation.
   */
  public static BinaryExpression leftShiftAssign(Expression left,
      Expression right, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a bitwise
   * left-shift assignment operation.
   */
  public static BinaryExpression leftShiftAssign(Expression left,
      Expression right, Method method, LambdaExpression lambdaExpression) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a "less than"
   * numeric comparison.
   */
  public static BinaryExpression lessThan(Expression left, Expression right) {
    return makeBinary(ExpressionType.LessThan, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a "less than"
   * numeric comparison.
   */
  public static BinaryExpression lessThan(Expression left, Expression right,
      boolean liftToNull, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a " less than or
   * equal" numeric comparison.
   */
  public static BinaryExpression lessThanOrEqual(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.LessThanOrEqual, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a "less than or
   * equal" numeric comparison.
   */
  public static BinaryExpression lessThanOrEqual(Expression left,
      Expression right, boolean liftToNull, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberListBinding where the member is a field or
   * property.
   */
  public static MemberListBinding listBind(Member member,
      Iterable<? extends ElementInit> elementInits) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberListBinding where the member is a field or
   * property, using varargs.
   */
  public static MemberListBinding listBind(Member member,
      ElementInit... elementInits) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberListBinding based on a specified property
   * accessor method.
   */
  public static MemberListBinding listBind(Method method,
      Iterable<? extends ElementInit> elementInits) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberListBinding object based on a specified
   * property accessor method, using varargs.
   */
  public static MemberListBinding listBind(Method method,
      ElementInit... elementInits) {
    throw Extensions.todo();
  }

  /**
   * Creates a ListInitExpression that uses specified ElementInit
   * objects to initialize a collection.
   */
  public static ListInitExpression listInit(NewExpression newExpression,
      Iterable<? extends ElementInit> elementInits) {
    throw Extensions.todo();
  }

  /**
   * Creates a ListInitExpression that uses specified ElementInit
   * objects to initialize a collection, using varargs.
   */
  public static ListInitExpression listInit(NewExpression newExpression,
      ElementInit... elementInits) {
    throw Extensions.todo();
  }

  /**
   * Creates a ListInitExpression that uses a method named "Add" to
   * add elements to a collection.
   */
  public static ListInitExpression listInitE(NewExpression newExpression,
      Iterable<? extends Expression> arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates a ListInitExpression that uses a method named "Add" to
   * add elements to a collection, using varargs.
   */
  public static ListInitExpression listInit(NewExpression newExpression,
      Expression... arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates a ListInitExpression that uses a specified method to
   * add elements to a collection.
   */
  public static ListInitExpression listInit(NewExpression newExpression,
      Method method, Iterable<? extends Expression> arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates a ListInitExpression that uses a specified method to
   * add elements to a collection, using varargs.
   */
  public static ListInitExpression listInit(NewExpression newExpression,
      Method method, Expression... arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates a LoopExpression with the given body.
   */
  public static ForStatement for_(
      Iterable<? extends DeclarationStatement> declarations,
      Expression condition, Expression post, Statement body) {
    return new ForStatement(toList(declarations), condition, post, body);
  }

  /**
   * Creates a LoopExpression with the given body.
   */
  public static ForStatement for_(
      DeclarationStatement declaration,
      Expression condition, Expression post, Statement body) {
    return new ForStatement(Collections.singletonList(declaration), condition,
        post, body);
  }

  /**
   * Creates a BinaryExpression, given the left and right operands,
   * by calling an appropriate factory method.
   */
  public static BinaryExpression makeBinary(ExpressionType binaryType,
      Expression left, Expression right) {
    final Type type;
    switch (binaryType) {
    case Equal:
    case NotEqual:
    case LessThan:
    case LessThanOrEqual:
    case GreaterThan:
    case GreaterThanOrEqual:
    case AndAlso:
    case OrElse:
      type = Boolean.TYPE;
      break;
    default:
      type = larger(left.type, right.type);
      break;
    }
    return new BinaryExpression(binaryType, type, left, right);
  }

  /** Returns an expression to box the value of a primitive expression.
   * E.g. {@code box(e, Primitive.INT)} returns {@code Integer.valueOf(e)}. */
  public static Expression box(Expression expression, Primitive primitive) {
    return call(primitive.boxClass, "valueOf", expression);
  }

  /** Converts e.g. "anInteger" to "Integer.valueOf(anInteger)". */
  public static Expression box(Expression expression) {
    Primitive primitive = Primitive.of(expression.getType());
    if (primitive == null) {
      return expression;
    }
    return box(expression, primitive);
  }

  /** Returns an expression to unbox the value of a boxed-primitive expression.
   * E.g. {@code unbox(e, Primitive.INT)} returns {@code e.intValue()}.
   * It is assumed that e is of the right box type (or {@link Number})."Value */
  public static Expression unbox(Expression expression, Primitive primitive) {
    return call(expression, primitive.primitiveName + "Value");
  }

  /** Converts e.g. "anInteger" to "anInteger.intValue()". */
  public static Expression unbox(Expression expression) {
    Primitive primitive = Primitive.ofBox(expression.getType());
    if (primitive == null) {
      return expression;
    }
    return unbox(expression, primitive);
  }

  private Type largest(Type... types) {
    Type max = types[0];
    for (int i = 1; i < types.length; i++) {
      max = larger(max, types[i]);
    }
    return max;
  }

  private static Type larger(Type type0, Type type1) {
    // curiously, "short + short" has type "int".
    // similarly, "byte + byte" has type "int".
    // "byte / long" has type "long".
    if (type0 == double.class
        || type0 == Double.class
        || type1 == double.class
        || type1 == Double.class) {
      return double.class;
    }
    if (type0 == float.class
        || type0 == Float.class
        || type1 == float.class
        || type1 == Float.class) {
      return float.class;
    }
    if (type0 == long.class
        || type0 == Long.class
        || type1 == long.class
        || type1 == Long.class) {
      return long.class;
    }
    return int.class;
  }

  /**
   * Creates a BinaryExpression, given the left operand, right
   * operand and implementing method, by calling the appropriate
   * factory method.
   */
  public static BinaryExpression makeBinary(ExpressionType binaryType,
      Expression left, Expression right, boolean liftToNull, Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression, given the left operand, right
   * operand, implementing method and type conversion function, by
   * calling the appropriate factory method.
   */
  public static BinaryExpression makeBinary(ExpressionType binaryType,
      Expression left, Expression right, boolean liftToNull, Method method,
      LambdaExpression lambdaExpression) {
    throw Extensions.todo();
  }

  /**
   * Creates a TernaryExpression, given the left and right operands,
   * by calling an appropriate factory method.
   */
  public static TernaryExpression makeTernary(ExpressionType ternaryType,
      Expression e0, Expression e1, Expression e2) {
    final Type type;
    switch (ternaryType) {
    case Conditional:
      if (e1 instanceof ConstantUntypedNull) {
        type = box(e2.getType());
        if (e1.getType() != type) {
          e1 = constant(null, type);
        }
      } else if (e2 instanceof ConstantUntypedNull) {
        type = box(e1.getType());
        if (e2.getType() != type) {
          e2 = constant(null, type);
        }
      } else {
        type = Types.gcd(e1.getType(), e2.getType());
      }
      break;
    default:
      type = e1.getType();
    }
    return new TernaryExpression(ternaryType, type, e0, e1, e2);
  }

  /**
   * Creates a CatchBlock representing a catch statement with the
   * specified elements.
   */
  public static CatchBlock makeCatchBlock(Type type,
      ParameterExpression variable, Expression body, Expression filter) {
    throw Extensions.todo();
  }

  /**
   * Creates a DynamicExpression that represents a dynamic
   * operation bound by the provided CallSiteBinder.
   */
  public static DynamicExpression makeDynamic(Type type, CallSiteBinder binder,
      Iterable<? extends Expression> arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates a DynamicExpression that represents a dynamic
   * operation bound by the provided CallSiteBinder, using varargs.
   */
  public static DynamicExpression makeDynamic(Type type, CallSiteBinder binder,
      Expression... arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates a GotoExpression representing a jump of the specified
   * GotoExpressionKind. The value passed to the label upon jumping
   * can also be specified.
   */
  public static GotoStatement makeGoto(GotoExpressionKind kind,
      LabelTarget target, Expression value, Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberExpression that represents accessing a field.
   */
  public static MemberExpression makeMemberAccess(Expression expression,
      PseudoField member) {
    return new MemberExpression(expression, member);
  }

  /**
   * Creates a TryExpression representing a try block with the
   * specified elements.
   */
  public static TryStatement makeTry(Type type, Expression body,
      Expression finally_, Expression fault,
      Iterable<? extends CatchBlock> handlers) {
    throw Extensions.todo();
  }

  /**
   * Creates a TryExpression representing a try block with the
   * specified elements, using varargs.
   */
  public static TryStatement makeTry(Type type, Expression body,
      Expression finally_, Expression fault,
      CatchBlock... handlers) {
    throw Extensions.todo();
  }

  /**
   * Creates a UnaryExpression, given an operand, by calling the
   * appropriate factory method.
   */
  public static UnaryExpression makeUnary(ExpressionType expressionType,
      Expression expression) {
    return new UnaryExpression(expressionType, expression.getType(),
        expression);
  }

  /**
   * Creates a UnaryExpression, given an operand and implementing
   * method, by calling the appropriate factory method.
   */
  public static UnaryExpression makeUnary(ExpressionType expressionType,
      Expression expression, Type type, Method method) {
    assert type != null;
    return new UnaryExpression(expressionType, type, expression);
  }

  /**
   * Creates a MemberMemberBinding that represents the recursive
   * initialization of members of a field or property.
   */
  public static MemberMemberBinding memberBind(Member member,
      Iterable<? extends MemberBinding> bindings) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberMemberBinding that represents the recursive
   * initialization of members of a field or property, using varargs.
   */
  public static MemberMemberBinding memberBind(Member member,
      MemberBinding... bindings) {
    return memberBind(member, toList(bindings));
  }

  /**
   * Creates a MemberMemberBinding that represents the recursive
   * initialization of members of a member that is accessed by using
   * a property accessor method.
   */
  public static MemberMemberBinding memberBind(Method method,
      Iterable<? extends MemberBinding> bindings) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberMemberBinding that represents the recursive
   * initialization of members of a member that is accessed by using
   * a property accessor method, using varargs.
   */
  public static MemberMemberBinding memberBind(Method method,
      MemberBinding... bindings) {
    return memberBind(method, toList(bindings));
  }

  /**
   * Represents an expression that creates a new object and
   * initializes a property of the object.
   */
  public static MemberInitExpression memberInit(NewExpression newExpression,
      Iterable<? extends MemberBinding> bindings) {
    throw Extensions.todo();
  }

  /**
   * Represents an expression that creates a new object and
   * initializes a property of the object, using varargs.
   */
  public static MemberInitExpression memberInit(NewExpression newExpression,
      MemberBinding... bindings) {
    return memberInit(newExpression, toList(bindings));
  }

  /**
   * Declares a method.
   */
  public static MethodDeclaration methodDecl(int modifier, Type resultType,
      String name, Iterable<? extends ParameterExpression> parameters,
      BlockStatement body) {
    return new MethodDeclaration(modifier, name, resultType, toList(parameters),
        body);
  }

  /**
   * Declares a constructor.
   */
  public static ConstructorDeclaration constructorDecl(int modifier,
      Type declaredAgainst, Iterable<? extends ParameterExpression> parameters,
      BlockStatement body) {
    return new ConstructorDeclaration(modifier, declaredAgainst, toList(
        parameters), body);
  }

  /**
   * Declares a field with an initializer.
   */
  public static FieldDeclaration fieldDecl(int modifier,
      ParameterExpression parameter, Expression initializer) {
    return new FieldDeclaration(modifier, parameter, initializer);
  }

  /**
   * Declares a field.
   */
  public static FieldDeclaration fieldDecl(int modifier,
      ParameterExpression parameter) {
    return new FieldDeclaration(modifier, parameter, null);
  }

  /**
   * Declares a class.
   */
  public static ClassDeclaration classDecl(int modifier, String name,
      Type extended, List<Type> implemented,
      List<MemberDeclaration> memberDeclarations) {
    return new ClassDeclaration(modifier, name, extended, implemented,
        memberDeclarations);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * remainder operation.
   */
  public static BinaryExpression modulo(Expression left, Expression right) {
    return makeBinary(ExpressionType.Modulo, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * remainder operation.
   */
  public static BinaryExpression modulo(Expression left, Expression right,
      Method method) {
    return makeBinary(ExpressionType.Modulo, left, right, shouldLift(left,
        right, method), method);
  }

  /**
   * Creates a BinaryExpression that represents a remainder
   * assignment operation.
   */
  public static BinaryExpression moduloAssign(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.ModuloAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a remainder
   * assignment operation.
   */
  public static BinaryExpression moduloAssign(Expression left, Expression right,
      Method method) {
    return makeBinary(ExpressionType.ModuloAssign, left, right, false, method);
  }

  /**
   * Creates a BinaryExpression that represents a remainder
   * assignment operation.
   */
  public static BinaryExpression moduloAssign(Expression left, Expression right,
      Method method, LambdaExpression lambdaExpression) {
    return makeBinary(ExpressionType.ModuloAssign, left, right, false, method,
        lambdaExpression);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * multiplication operation that does not have overflow
   * checking.
   */
  public static BinaryExpression multiply(Expression left, Expression right) {
    return makeBinary(ExpressionType.Multiply, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * multiplication operation that does not have overflow
   * checking.
   */
  public static BinaryExpression multiply(Expression left, Expression right,
      Method method) {
    return makeBinary(ExpressionType.Multiply, left, right, shouldLift(left,
        right, method), method);
  }

  /**
   * Creates a BinaryExpression that represents a multiplication
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression multiplyAssign(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.MultiplyAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a multiplication
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression multiplyAssign(Expression left,
      Expression right, Method method) {
    return makeBinary(ExpressionType.MultiplyAssign, left, right, false,
        method);
  }

  /**
   * Creates a BinaryExpression that represents a multiplication
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression multiplyAssign(Expression left,
      Expression right, Method method, LambdaExpression lambdaExpression) {
    return makeBinary(ExpressionType.MultiplyAssign, left, right, false, method,
        lambdaExpression);
  }

  /**
   * Creates a BinaryExpression that represents a multiplication
   * assignment operation that has overflow checking.
   */
  public static BinaryExpression multiplyAssignChecked(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.MultiplyAssignChecked, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a multiplication
   * assignment operation that has overflow checking.
   */
  public static BinaryExpression multiplyAssignChecked(Expression left,
      Expression right, Method method) {
    return makeBinary(ExpressionType.MultiplyAssignChecked, left, right, false,
        method);
  }

  /**
   * Creates a BinaryExpression that represents a multiplication
   * assignment operation that has overflow checking.
   */
  public static BinaryExpression multiplyAssignChecked(Expression left,
      Expression right, Method method, LambdaExpression lambdaExpression) {
    return makeBinary(
        ExpressionType.MultiplyAssignChecked,
        left,
        right,
        false,
        method,
        lambdaExpression);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * multiplication operation that has overflow checking.
   */
  public static BinaryExpression multiplyChecked(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.MultiplyChecked, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * multiplication operation that has overflow checking.
   */
  public static BinaryExpression multiplyChecked(Expression left,
      Expression right, Method method) {
    return makeBinary(ExpressionType.MultiplyChecked, left, right, shouldLift(
        left, right, method), method);
  }

  /**
   * Creates a UnaryExpression that represents an arithmetic
   * negation operation.
   */
  public static UnaryExpression negate(Expression expression) {
    return makeUnary(ExpressionType.Negate, expression);
  }

  /**
   * Creates a UnaryExpression that represents an arithmetic
   * negation operation.
   */
  public static UnaryExpression negate(Expression expression, Method method) {
    return makeUnary(ExpressionType.Negate, expression, null, method);
  }

  /**
   * Creates a UnaryExpression that represents an arithmetic
   * negation operation that has overflow checking.
   */
  public static UnaryExpression negateChecked(Expression expression) {
    return makeUnary(ExpressionType.NegateChecked, expression);
  }

  /**
   * Creates a UnaryExpression that represents an arithmetic
   * negation operation that has overflow checking. The implementing
   * method can be specified.
   */
  public static UnaryExpression negateChecked(Expression expression,
      Method method) {
    return makeUnary(ExpressionType.NegateChecked, expression, null, method);
  }

  /**
   * Creates a NewExpression that represents calling the specified
   * constructor that takes no arguments.
   */
  public static NewExpression new_(Constructor constructor) {
    return new_(
        constructor.getDeclaringClass(), Collections.<Expression>emptyList());
  }

  /**
   * Creates a NewExpression that represents calling the
   * parameterless constructor of the specified type.
   */
  public static NewExpression new_(Type type) {
    return new_(type, Collections.<Expression>emptyList());
  }

  /**
   * Creates a NewExpression that represents calling the constructor of the
   * specified type whose parameters are assignable from the specified
   * arguments.
   */
  public static NewExpression new_(Type type,
      Iterable<? extends Expression> arguments) {
    // Note that the last argument is not an empty list. That would cause
    // an anonymous inner-class with no members to be generated.
    return new NewExpression(type, toList(arguments), null);
  }

  /**
   * Creates a NewExpression that represents calling the constructor of the
   * specified type whose parameters are assignable from the specified
   * arguments, using varargs.
   */
  public static NewExpression new_(Type type, Expression... arguments) {
    // Note that the last argument is not an empty list. That would cause
    // an anonymous inner-class with no members to be generated.
    return new NewExpression(type, toList(arguments), null);
  }

  /**
   * Creates a NewExpression that represents calling the constructor of the
   * specified type whose parameters are assignable from the specified
   * arguments.
   */
  public static NewExpression new_(Type type,
      Iterable<? extends Expression> arguments,
      Iterable<? extends MemberDeclaration> memberDeclarations) {
    return new NewExpression(type, toList(arguments),
        toList(memberDeclarations));
  }

  /**
   * Creates a NewExpression that represents calling the constructor of the
   * specified type whose parameters are assignable from the specified
   * arguments, using varargs.
   */
  public static NewExpression new_(Type type,
      Iterable<? extends Expression> arguments,
      MemberDeclaration... memberDeclarations) {
    return new NewExpression(type, toList(arguments),
        toList(memberDeclarations));
  }

  /**
   * Creates a NewExpression that represents calling the specified
   * constructor with the specified arguments.
   */
  public static NewExpression new_(Constructor constructor,
      Iterable<? extends Expression> expressions) {
    // Note that the last argument is not an empty list. That would cause
    // an anonymous inner-class with no members to be generated.
    return new NewExpression(constructor.getDeclaringClass(),
        toList(expressions), null);
  }

  /**
   * Creates a NewExpression that represents calling the specified
   * constructor with the specified arguments, using varargs.
   */
  public static NewExpression new_(Constructor constructor,
      Expression... expressions) {
    return new NewExpression(constructor.getDeclaringClass(),
        toList(expressions), null);
  }

  /**
   * Creates a NewExpression that represents calling the specified
   * constructor with the specified arguments.
   *
   * <p>The members that access the constructor initialized fields are
   * specified.
   */
  public static NewExpression new_(Constructor constructor,
      Iterable<? extends Expression> expressions,
      Iterable<? extends MemberDeclaration> memberDeclarations) {
    return new_(constructor.getDeclaringClass(), toList(expressions),
        toList(memberDeclarations));
  }

  /**
   * Creates a NewExpression that represents calling the specified
   * constructor with the specified arguments, using varargs.
   *
   * <p>The members that access the constructor initialized fields are
   * specified.
   */
  public static NewExpression new_(Constructor constructor,
      Iterable<? extends Expression> expressions,
      MemberDeclaration... memberDeclarations) {
    return new_(constructor.getDeclaringClass(), toList(expressions),
        toList(memberDeclarations));
  }

  /**
   * Creates a NewArrayExpression that represents creating an array
   * that has a specified rank.
   */
  public static NewArrayExpression newArrayBounds(Type type, int dimension,
      Expression bound) {
    return new NewArrayExpression(type, dimension, bound, null);
  }

  /**
   * Creates a NewArrayExpression that represents creating a
   * one-dimensional array and initializing it from a list of
   * elements.
   *
   * @param type Element type of the array.
   */
  public static NewArrayExpression newArrayInit(Type type,
      Iterable<? extends Expression> expressions) {
    return new NewArrayExpression(type, 1, null, toList(expressions));
  }

  /**
   * Creates a NewArrayExpression that represents creating a
   * one-dimensional array and initializing it from a list of
   * elements, using varargs.
   *
   * @param type Element type of the array.
   */
  public static NewArrayExpression newArrayInit(Type type,
      Expression... expressions) {
    return new NewArrayExpression(type, 1, null, toList(expressions));
  }

  /**
   * Creates a NewArrayExpression that represents creating a
   * n-dimensional array and initializing it from a list of
   * elements.
   *
   * @param type Element type of the array.
   */
  public static NewArrayExpression newArrayInit(Type type, int dimension,
      Iterable<? extends Expression> expressions) {
    return new NewArrayExpression(type, dimension, null, toList(expressions));
  }

  /**
   * Creates a NewArrayExpression that represents creating an
   * n-dimensional array and initializing it from a list of
   * elements, using varargs.
   *
   * @param type Element type of the array.
   */
  public static NewArrayExpression newArrayInit(Type type, int dimension,
      Expression... expressions) {
    return new NewArrayExpression(type, dimension, null, toList(expressions));
  }

  /**
   * Creates a UnaryExpression that represents a bitwise complement
   * operation.
   */
  public static UnaryExpression not(Expression expression) {
    return makeUnary(ExpressionType.Not, expression);
  }

  /**
   * Creates a UnaryExpression that represents a bitwise complement
   * operation. The implementing method can be specified.
   */
  public static UnaryExpression not(Expression expression, Method method) {
    return makeUnary(ExpressionType.Not, expression, null, method);
  }

  /**
   * Creates a BinaryExpression that represents an inequality
   * comparison.
   */
  public static BinaryExpression notEqual(Expression left, Expression right) {
    return makeBinary(ExpressionType.NotEqual, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an inequality
   * comparison.
   */
  public static BinaryExpression notEqual(Expression left, Expression right,
      boolean liftToNull, Method method) {
    return makeBinary(ExpressionType.NotEqual, left, right, liftToNull, method);
  }

  /**
   * Returns the expression representing the ones complement.
   */
  public static UnaryExpression onesComplement(Expression expression) {
    return makeUnary(ExpressionType.OnesComplement, expression);
  }

  /**
   * Returns the expression representing the ones complement.
   */
  public static UnaryExpression onesComplement(Expression expression,
      Method method) {
    return makeUnary(ExpressionType.OnesComplement, expression,
        expression.getType(), method);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise OR
   * operation.
   */
  public static BinaryExpression or(Expression left, Expression right) {
    return makeBinary(ExpressionType.Or, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise OR
   * operation.
   */
  public static BinaryExpression or(Expression left, Expression right,
      Method method) {
    return makeBinary(ExpressionType.Or, left, right, false, method);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise OR
   * assignment operation.
   */
  public static BinaryExpression orAssign(Expression left, Expression right) {
    return makeBinary(ExpressionType.OrAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise OR
   * assignment operation.
   */
  public static BinaryExpression orAssign(Expression left, Expression right,
      Method method) {
    return makeBinary(ExpressionType.OrAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise OR
   * assignment operation.
   */
  public static BinaryExpression orAssign(Expression left, Expression right,
      Method method, LambdaExpression lambdaExpression) {
    return makeBinary(ExpressionType.OrAssign, left, right, false, method,
        lambdaExpression);
  }

  /**
   * Creates a BinaryExpression that represents a conditional OR
   * operation that evaluates the second operand only if the first
   * operand evaluates to false.
   */
  public static BinaryExpression orElse(Expression left, Expression right) {
    return makeBinary(ExpressionType.OrElse, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a conditional OR
   * operation that evaluates the second operand only if the first
   * operand evaluates to false.
   */
  public static BinaryExpression orElse(Expression left, Expression right,
      Method method) {
    return makeBinary(ExpressionType.OrElse, left, right, false, method);
  }

  /**
   * Creates a ParameterExpression node that can be used to
   * identify a parameter or a variable in an expression tree.
   */
  public static ParameterExpression parameter(Type type) {
    return new ParameterExpression(type);
  }

  /**
   * Creates a ParameterExpression node that can be used to
   * identify a parameter or a variable in an expression tree.
   */
  public static ParameterExpression parameter(Type type, String name) {
    return new ParameterExpression(0, type, name);
  }

  /**
   * Creates a ParameterExpression.
   */
  public static ParameterExpression parameter(int modifiers, Type type,
      String name) {
    return new ParameterExpression(modifiers, type, name);
  }

  /**
   * Creates a UnaryExpression that represents the assignment of
   * the expression followed by a subsequent decrement by 1 of the
   * original expression.
   */
  public static UnaryExpression postDecrementAssign(Expression expression) {
    return makeUnary(ExpressionType.PostDecrementAssign, expression);
  }

  /**
   * Creates a UnaryExpression that represents the assignment of
   * the expression followed by a subsequent decrement by 1 of the
   * original expression.
   */
  public static UnaryExpression postDecrementAssign(Expression expression,
      Method method) {
    return makeUnary(ExpressionType.PostDecrementAssign, expression,
        expression.getType(), method);
  }

  /**
   * Creates a UnaryExpression that represents the assignment of
   * the expression followed by a subsequent increment by 1 of the
   * original expression.
   */
  public static UnaryExpression postIncrementAssign(Expression expression) {
    return makeUnary(ExpressionType.PostIncrementAssign, expression);
  }

  /**
   * Creates a UnaryExpression that represents the assignment of
   * the expression followed by a subsequent increment by 1 of the
   * original expression.
   */
  public static UnaryExpression postIncrementAssign(Expression expression,
      Method method) {
    return makeUnary(ExpressionType.PostIncrementAssign, expression,
        expression.getType(), method);
  }

  /**
   * Creates a BinaryExpression that represents raising a number to
   * a power.
   */
  // REVIEW: In Java this is a call to a lib function, Math.pow.
  public static BinaryExpression power(Expression left, Expression right) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents raising a number to
   * a power.
   */
  // REVIEW: In Java this is a call to a lib function, Math.pow.
  public static BinaryExpression power(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents raising an
   * expression to a power and assigning the result back to the
   * expression.
   */
  // REVIEW: In Java this is a call to a lib function, Math.pow.
  public static BinaryExpression powerAssign(Expression left,
      Expression right) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents raising an
   * expression to a power and assigning the result back to the
   * expression.
   */
  // REVIEW: In Java this is a call to a lib function, Math.pow.
  public static BinaryExpression powerAssign(Expression left, Expression right,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents raising an
   * expression to a power and assigning the result back to the
   * expression.
   */
  public static BinaryExpression powerAssign(Expression left, Expression right,
      Method method, LambdaExpression lambdaExpression) {
    throw Extensions.todo();
  }

  /**
   * Creates a UnaryExpression that decrements the expression by 1
   * and assigns the result back to the expression.
   */
  public static UnaryExpression preDecrementAssign(Expression expression) {
    return makeUnary(ExpressionType.PreDecrementAssign, expression);
  }

  /**
   * Creates a UnaryExpression that decrements the expression by 1
   * and assigns the result back to the expression.
   */
  public static UnaryExpression preDecrementAssign(Expression expression,
      Method method) {
    return makeUnary(ExpressionType.PreDecrementAssign, expression,
        expression.getType(), method);
  }

  /**
   * Creates a UnaryExpression that increments the expression by 1
   * and assigns the result back to the expression.
   */
  public static UnaryExpression preIncrementAssign(Expression expression) {
    return makeUnary(ExpressionType.PreIncrementAssign, expression);
  }

  /**
   * Creates a UnaryExpression that increments the expression by 1
   * and assigns the result back to the expression.
   */
  public static UnaryExpression preIncrementAssign(Expression expression,
      Method method) {
    return makeUnary(ExpressionType.PreIncrementAssign, expression,
        expression.getType(), method);
  }

  /**
   * Creates a MemberExpression that represents accessing a
   * property by using a property accessor method.
   */
  // REVIEW: No equivalent to properties in Java.
  public static MemberExpression property(Expression expression,
      Method method) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberExpression that represents accessing a
   * property.
   */
  // REVIEW: No equivalent to properties in Java.
  public static MemberExpression property(Expression expression,
      PropertyInfo property) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberExpression that represents accessing a
   * property.
   */
  // REVIEW: No equivalent to properties in Java.
  public static MemberExpression property(Expression expression, String name) {
    throw Extensions.todo();
  }

  /**
   * Creates an IndexExpression representing the access to an
   * indexed property.
   */
  // REVIEW: No equivalent to properties in Java.
  public static IndexExpression property(Expression expression,
      PropertyInfo property, Iterable<? extends Expression> arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates an IndexExpression representing the access to an
   * indexed property, using varargs.
   */
  // REVIEW: No equivalent to properties in Java.
  public static IndexExpression property(Expression expression,
      PropertyInfo property, Expression... arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates an IndexExpression representing the access to an
   * indexed property.
   */
  // REVIEW: No equivalent to properties in Java.
  public static IndexExpression property(Expression expression, String name,
      Expression... arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberExpression accessing a property.
   */
  public static MemberExpression property(Expression expression, Type type,
      String name) {
    throw Extensions.todo();
  }

  /**
   * Creates a MemberExpression that represents accessing a
   * property or field.
   */
  // REVIEW: Java does not have properties; can only be a field name.
  public static MemberExpression propertyOrField(Expression expression,
      String propertyOfFieldName) {
    throw Extensions.todo();
  }

  /**
   * Creates a UnaryExpression that represents an expression that
   * has a constant value of type Expression.
   */
  public static UnaryExpression quote(Expression expression) {
    return makeUnary(ExpressionType.Quote, expression);
  }

  /**
   * Reduces this node to a simpler expression. If CanReduce
   * returns true, this should return a valid expression. This
   * method can return another node which itself must be reduced.
   */
  public static Expression reduce(Expression expression) {
    throw Extensions.todo();
  }

  /**
   * Reduces this node to a simpler expression. If CanReduce
   * returns true, this should return a valid expression. This
   * method can return another node which itself must be reduced.
   */
  public static Expression reduceAndCheck(Expression expression) {
    throw Extensions.todo();
  }

  /**
   * Reduces the expression to a known node type (that is not an
   * Extension node) or just returns the expression if it is already
   * a known type.
   */
  public static Expression reduceExtensions(Expression expression) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a reference
   * equality comparison.
   */
  public static Expression referenceEqual(Expression left, Expression right) {
    return makeBinary(ExpressionType.Equal, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a reference
   * inequality comparison.
   */
  public static Expression referenceNotEqual(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.NotEqual, left, right);
  }

  /**
   * Creates a UnaryExpression that represents a rethrowing of an
   * exception.
   */
  public static UnaryExpression rethrow() {
    throw Extensions.todo();
  }

  /**
   * Creates a UnaryExpression that represents a rethrowing of an
   * exception with a given type.
   */
  public static UnaryExpression rethrow(Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a GotoExpression representing a return statement.
   */
  public static GotoStatement return_(LabelTarget labelTarget) {
    return return_(labelTarget, (Expression) null);
  }

  /**
   * Creates a GotoExpression representing a return statement. The
   * value passed to the label upon jumping can be specified.
   */
  public static GotoStatement return_(LabelTarget labelTarget,
      Expression expression) {
    return makeGoto(GotoExpressionKind.Return, labelTarget, expression);
  }

  public static GotoStatement makeGoto(GotoExpressionKind kind,
      LabelTarget labelTarget, Expression expression) {
    return new GotoStatement(kind, labelTarget, expression);
  }

  /**
   * Creates a GotoExpression representing a return statement with
   * the specified type.
   */
  public static GotoStatement return_(LabelTarget labelTarget, Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a GotoExpression representing a return statement with
   * the specified type. The value passed to the label upon jumping
   * can be specified.
   */
  public static GotoStatement return_(LabelTarget labelTarget,
      Expression expression, Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents a bitwise
   * right-shift operation.
   */
  public static BinaryExpression rightShift(Expression left, Expression right) {
    return makeBinary(ExpressionType.RightShift, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise
   * right-shift operation.
   */
  public static BinaryExpression rightShift(Expression left, Expression right,
      Method method) {
    return makeBinary(ExpressionType.RightShift, left, right, false, method);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise
   * right-shift assignment operation.
   */
  public static BinaryExpression rightShiftAssign(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.RightShiftAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise
   * right-shift assignment operation.
   */
  public static BinaryExpression rightShiftAssign(Expression left,
      Expression right, Method method) {
    return makeBinary(ExpressionType.RightShiftAssign, left, right, false,
        method);
  }

  /**
   * Creates a BinaryExpression that represents a bitwise
   * right-shift assignment operation.
   */
  public static BinaryExpression rightShiftAssign(Expression left,
      Expression right, Method method, LambdaExpression lambdaExpression) {
    return makeBinary(ExpressionType.RightShiftAssign, left, right, false,
        method, lambdaExpression);
  }

  /**
   * Creates an instance of RuntimeVariablesExpression.
   */
  public static RuntimeVariablesExpression runtimeVariables(
      Iterable<? extends ParameterExpression> expressions) {
    throw Extensions.todo();
  }

  /**
   * Creates an instance of RuntimeVariablesExpression, using varargs.
   */
  public static RuntimeVariablesExpression runtimeVariables(
      ParameterExpression... arguments) {
    throw Extensions.todo();
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * subtraction operation that does not have overflow checking.
   */
  public static BinaryExpression subtract(Expression left, Expression right) {
    return makeBinary(ExpressionType.Subtract, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * subtraction operation that does not have overflow checking.
   */
  public static BinaryExpression subtract(Expression left, Expression right,
      Method method) {
    return makeBinary(ExpressionType.Subtract, left, right, shouldLift(left,
        right, method), method);
  }

  /**
   * Creates a BinaryExpression that represents a subtraction
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression subtractAssign(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.SubtractAssign, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a subtraction
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression subtractAssign(Expression left,
      Expression right, Method method) {
    return makeBinary(ExpressionType.SubtractAssign, left, right, false,
        method);
  }

  /**
   * Creates a BinaryExpression that represents a subtraction
   * assignment operation that does not have overflow checking.
   */
  public static BinaryExpression subtractAssign(Expression left,
      Expression right, Method method, LambdaExpression lambdaExpression) {
    return makeBinary(ExpressionType.SubtractAssign, left, right, false, method,
        lambdaExpression);
  }

  /**
   * Creates a BinaryExpression that represents a subtraction
   * assignment operation that has overflow checking.
   */
  public static BinaryExpression subtractAssignChecked(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.SubtractAssignChecked, left, right);
  }

  /**
   * Creates a BinaryExpression that represents a subtraction
   * assignment operation that has overflow checking.
   */
  public static BinaryExpression subtractAssignChecked(Expression left,
      Expression right, Method method) {
    return makeBinary(ExpressionType.SubtractAssignChecked, left, right, false,
        method);
  }

  /**
   * Creates a BinaryExpression that represents a subtraction
   * assignment operation that has overflow checking.
   */
  public static BinaryExpression subtractAssignChecked(Expression left,
      Expression right, Method method, LambdaExpression lambdaExpression) {
    return makeBinary(ExpressionType.SubtractAssignChecked, left, right, false,
        method, lambdaExpression);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * subtraction operation that has overflow checking.
   */
  public static BinaryExpression subtractChecked(Expression left,
      Expression right) {
    return makeBinary(ExpressionType.SubtractChecked, left, right);
  }

  /**
   * Creates a BinaryExpression that represents an arithmetic
   * subtraction operation that has overflow checking.
   */
  public static BinaryExpression subtractChecked(Expression left,
      Expression right, Method method) {
    return makeBinary(ExpressionType.SubtractChecked, left, right, shouldLift(
        left, right, method), method);
  }

  /**
   * Creates a SwitchExpression that represents a switch statement
   * without a default case.
   */
  public static SwitchStatement switch_(Expression switchValue,
      SwitchCase... cases) {
    return switch_(switchValue, null, null, toList(cases));
  }

  /**
   * Creates a SwitchExpression that represents a switch statement
   * that has a default case.
   */
  public static SwitchStatement switch_(Expression switchValue,
      Expression defaultBody, SwitchCase... cases) {
    return switch_(switchValue, defaultBody, null, toList(cases));
  }

  /**
   * Creates a SwitchExpression that represents a switch statement
   * that has a default case.
   */
  public static SwitchStatement switch_(Expression switchValue,
      Expression defaultBody, Method method,
      Iterable<? extends SwitchCase> cases) {
    throw Extensions.todo();
  }

  /**
   * Creates a SwitchExpression that represents a switch statement
   * that has a default case, using varargs.
   */
  public static SwitchStatement switch_(Expression switchValue,
      Expression defaultBody, Method method, SwitchCase... cases) {
    return switch_(switchValue, defaultBody, method, toList(cases));
  }

  /**
   * Creates a SwitchExpression that represents a switch statement
   * that has a default case.
   */
  public static SwitchStatement switch_(Type type, Expression switchValue,
      Expression defaultBody, Method method,
      Iterable<? extends SwitchCase> cases) {
    throw Extensions.todo();
  }

  /**
   * Creates a SwitchExpression that represents a switch statement
   * that has a default case, using varargs.
   */
  public static SwitchStatement switch_(Type type, Expression switchValue,
      Expression defaultBody, Method method, SwitchCase... cases) {
    return switch_(type, switchValue, defaultBody, method, toList(cases));
  }

  /**
   * Creates a SwitchCase for use in a SwitchExpression.
   */
  public static SwitchCase switchCase(Expression expression,
      Iterable<? extends Expression> body) {
    throw Extensions.todo();
  }

  /**
   * Creates a SwitchCase for use in a SwitchExpression, with varargs.
   */
  public static SwitchCase switchCase(Expression expression,
      Expression... body) {
    return switchCase(expression, toList(body));
  }

  /**
   * Creates an instance of SymbolDocumentInfo.
   */
  public static SymbolDocumentInfo symbolDocument(String fileName) {
    throw Extensions.todo();
  }

  /**
   * Creates an instance of SymbolDocumentInfo.
   */
  public static SymbolDocumentInfo symbolDocument(String fileName,
      UUID language) {
    throw Extensions.todo();
  }

  /**
   * Creates an instance of SymbolDocumentInfo.
   */
  public static SymbolDocumentInfo symbolDocument(String fileName,
      UUID language, UUID vendor) {
    throw Extensions.todo();
  }

  /**
   * Creates an instance of SymbolDocumentInfo.
   */
  public static SymbolDocumentInfo symbolDocument(String filename,
      UUID language, UUID vendor, UUID documentType) {
    throw Extensions.todo();
  }

  /**
   * Creates a statement that represents the throwing of an exception.
   */
  public static ThrowStatement throw_(Expression expression) {
    return new ThrowStatement(expression);
  }

  /**
   * Creates a TryExpression representing a try block with any
   * number of catch statements and neither a fault nor finally
   * block.
   */
  public static TryStatement tryCatch(Statement body,
      Iterable<? extends CatchBlock> handlers) {
    return new TryStatement(body, toList(handlers), null);
  }

  /**
   * Creates a TryExpression representing a try block with any
   * number of catch statements and neither a fault nor finally
   * block, with varargs.
   */
  public static TryStatement tryCatch(Statement body,
      CatchBlock... handlers) {
    return new TryStatement(body, toList(handlers), null);
  }

  /**
   * Creates a TryExpression representing a try block with any
   * number of catch statements and a finally block.
   */
  public static TryStatement tryCatchFinally(Statement body,
      Iterable<? extends CatchBlock> handlers, Statement finally_) {
    return new TryStatement(body, toList(handlers), finally_);
  }

  /**
   * Creates a TryExpression representing a try block with any
   * number of catch statements and a finally block, with varargs.
   */
  public static TryStatement tryCatchFinally(Statement body, Statement finally_,
      CatchBlock... handlers) {
    return new TryStatement(body, toList(handlers), finally_);
  }

  /**
   * Creates a TryExpression representing a try block with a
   * finally block and no catch statements.
   */
  public static TryStatement tryFinally(Statement body, Statement finally_) {
    return new TryStatement(body, Collections.<CatchBlock>emptyList(),
        finally_);
  }

  /**
   * Creates a UnaryExpression that represents an explicit
   * reference or boxing conversion where null is supplied if the
   * conversion fails.
   */
  public static UnaryExpression typeAs(Expression expression, Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a TypeBinaryExpression that compares run-time type
   * identity.
   */
  public static TypeBinaryExpression typeEqual(Expression expression,
      Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a TypeBinaryExpression.
   */
  public static TypeBinaryExpression typeIs(Expression expression, Type type) {
    return new TypeBinaryExpression(ExpressionType.TypeIs, expression, type);
  }

  /**
   * Creates a UnaryExpression that represents a unary plus
   * operation.
   */
  public static UnaryExpression unaryPlus(Expression expression) {
    return makeUnary(ExpressionType.UnaryPlus, expression);
  }

  /**
   * Creates a UnaryExpression that represents a unary plus
   * operation.
   */
  public static UnaryExpression unaryPlus(Expression expression,
      Method method) {
    return makeUnary(ExpressionType.UnaryPlus, expression, expression.getType(),
        method);
  }

  /**
   * Creates a UnaryExpression that represents an explicit
   * unboxing.
   */
  public static UnaryExpression unbox(Expression expression, Type type) {
    return new UnaryExpression(ExpressionType.Unbox, type, expression);
  }

  /**
   * Creates a ParameterExpression node that can be used to
   * identify a parameter or a variable in an expression tree.
   */
  public static ParameterExpression variable(Type type) {
    throw Extensions.todo();
  }

  /**
   * Creates a ParameterExpression node that can be used to
   * identify a parameter or a variable in an expression tree.
   */
  public static ParameterExpression variable(Type type, String name) {
    return new ParameterExpression(0, type, name);
  }

  /**
   * Reduces the node and then calls the visitor delegate on the
   * reduced expression. The method throws an exception if the node
   * is not reducible.
   */
  public static Expression visitChildren(ExpressionVisitor visitor) {
    throw Extensions.todo();
  }

  /**
   * Creates a WhileExpression representing a while loop.
   */
  public static WhileStatement while_(Expression condition, Statement body) {
    return new WhileStatement(condition, body);
  }

  /**
   * Creates a statement that declares a variable.
   */
  public static DeclarationStatement declare(int modifiers,
      ParameterExpression parameter, Expression initializer) {
    return new DeclarationStatement(modifiers, parameter, initializer);
  }

  /**
   * Creates an expression that declares and initializes a variable. No
   * type is required; it is assumed that the variable is the same type as
   * the initializer. You can retrieve the {@link ParameterExpression} from
   * the {@link DeclarationStatement#parameter} field of the result.
   */
  public static DeclarationStatement declare(int modifiers, String name,
      Expression initializer) {
    return declare(modifiers, parameter(initializer.getType(), name),
        initializer);
  }

  /**
   * Creates a statement that executes an expression.
   */
  public static Statement statement(Expression expression) {
    return new GotoStatement(GotoExpressionKind.Sequence, null, expression);
  }

  /** Combines a list of expressions using AND.
   * Returns TRUE if the list is empty.
   * Returns FALSE if any of the conditions are constant FALSE;
   * otherwise returns NULL if any of the conditions are constant NULL. */
  public static Expression foldAnd(List<Expression> conditions) {
    Expression e = null;
    int nullCount = 0;
    for (Expression condition : conditions) {
      if (condition instanceof ConstantExpression) {
        final Boolean value = (Boolean) ((ConstantExpression) condition).value;
        if (value == null) {
          ++nullCount;
          continue;
        } else if (value) {
          continue;
        } else {
          return constant(false);
        }
      }
      if (e == null) {
        e = condition;
      } else {
        e = andAlso(e, condition);
      }
    }
    if (nullCount > 0) {
      return constant(null);
    }
    if (e == null) {
      return constant(true);
    }
    return e;
  }

  /** Combines a list of expressions using OR.
   * Returns FALSE if the list is empty.
   * Returns TRUE if any of the conditions are constant TRUE;
   * otherwise returns NULL if all of the conditions are constant NULL. */
  public static Expression foldOr(List<Expression> conditions) {
    Expression e = null;
    int nullCount = 0;
    for (Expression condition : conditions) {
      if (condition instanceof ConstantExpression) {
        final Boolean value = (Boolean) ((ConstantExpression) condition).value;
        if (value == null) {
          ++nullCount;
          continue;
        } else if (value) {
          return constant(true);
        } else {
          continue;
        }
      }
      if (e == null) {
        e = condition;
      } else {
        e = orElse(e, condition);
      }
    }
    if (e == null) {
      if (nullCount > 0) {
        return constant(null);
      }
      return constant(false);
    }
    return e;
  }

  /**
   * Creates an empty fluent list.
   */
  public static <T> FluentList<T> list() {
    return new FluentArrayList<T>();
  }

  /**
   * Creates a fluent list with given elements.
   */
  public static <T> FluentList<T> list(T... ts) {
    return new FluentArrayList<T>(Arrays.asList(ts));
  }

  /**
   * Creates a fluent list with elements from the given collection.
   */
  public static <T> FluentList<T> list(Iterable<T> ts) {
    return new FluentArrayList<T>(toList(ts));
  }

  // ~ Private helper methods ------------------------------------------------

  private static boolean shouldLift(Expression left, Expression right,
      Method method) {
    // FIXME: Implement the rules in modulo
    return true;
  }

  private static Class deduceType(List<ParameterExpression> parameterList,
      Type type) {
    switch (parameterList.size()) {
    case 0:
      return Function0.class;
    case 1:
      return type == Boolean.TYPE ? Predicate1.class : Function1.class;
    case 2:
      return type == Boolean.TYPE ? Predicate2.class : Function2.class;
    default:
      return Function.class;
    }
  }

  private static <T> List<T> toList(Iterable<? extends T> iterable) {
    if (iterable == null) {
      return null;
    }
    if (iterable instanceof List) {
      return (List<T>) iterable;
    }
    final List<T> list = new ArrayList<T>();
    for (T parameter : iterable) {
      list.add(parameter);
    }
    return list;
  }

  private static <T> List<T> toList(T[] ts) {
    if (ts.length == 0) {
      return Collections.emptyList();
    } else {
      return Arrays.asList(ts);
    }
  }

  private static <T> Collection<T> toCollection(Iterable<T> iterable) {
    if (iterable instanceof Collection) {
      return (Collection<T>) iterable;
    }
    return toList(iterable);
  }

  private static <T> T[] toArray(Iterable<T> iterable, T[] a) {
    return toCollection(iterable).toArray(a);
  }

  static <T extends Expression> Expression accept(T node, Visitor visitor) {
    if (node == null) {
      return null;
    }
    return node.accept(visitor);
  }

  static <T extends Statement> Statement accept(T node, Visitor visitor) {
    if (node == null) {
      return null;
    }
    return node.accept(visitor);
  }

  static List<Statement> acceptStatements(List<Statement> statements,
      Visitor visitor) {
    if (statements.isEmpty()) {
      return statements; // short cut
    }
    final List<Statement> statements1 = new ArrayList<Statement>();
    for (Statement statement : statements) {
      Statement newStatement = statement.accept(visitor);
      if (newStatement instanceof GotoStatement) {
        GotoStatement goto_ = (GotoStatement) newStatement;
        if (goto_.kind == GotoExpressionKind.Sequence
            && goto_.expression == null) {
          // ignore empty statements
          continue;
        }
      }
      statements1.add(newStatement);
    }
    return statements1;
  }

  static List<Node> acceptNodes(List<Node> nodes, Visitor visitor) {
    if (nodes.isEmpty()) {
      return nodes; // short cut
    }
    final List<Node> statements1 = new ArrayList<Node>();
    for (Node node : nodes) {
      statements1.add(node.accept(visitor));
    }
    return statements1;
  }

  static List<Expression> acceptParameterExpressions(
      List<ParameterExpression> parameterExpressions, Visitor visitor) {
    if (parameterExpressions.isEmpty()) {
      return Collections.emptyList(); // short cut
    }
    final List<Expression> parameterExpressions1 = new ArrayList<Expression>();
    for (ParameterExpression parameterExpression : parameterExpressions) {
      parameterExpressions1.add(parameterExpression.accept(visitor));
    }
    return parameterExpressions1;
  }

  static List<DeclarationStatement> acceptDeclarations(
      List<DeclarationStatement> declarations, Visitor visitor) {
    if (declarations == null || declarations.isEmpty()) {
      return declarations; // short cut
    }
    final List<DeclarationStatement> declarations1 =
        new ArrayList<DeclarationStatement>();
    for (DeclarationStatement declaration : declarations) {
      declarations1.add(declaration.accept(visitor));
    }
    return declarations1;
  }

  static List<MemberDeclaration> acceptMemberDeclarations(
      List<MemberDeclaration> memberDeclarations, Visitor visitor) {
    if (memberDeclarations == null || memberDeclarations.isEmpty()) {
      return memberDeclarations; // short cut
    }
    final List<MemberDeclaration> memberDeclarations1 =
        new ArrayList<MemberDeclaration>();
    for (MemberDeclaration memberDeclaration : memberDeclarations) {
      memberDeclarations1.add(memberDeclaration.accept(visitor));
    }
    return memberDeclarations1;
  }

  static List<Expression> acceptExpressions(List<Expression> expressions,
      Visitor visitor) {
    if (expressions.isEmpty()) {
      return expressions; // short cut
    }
    final List<Expression> expressions1 = new ArrayList<Expression>();
    for (Expression expression : expressions) {
      expressions1.add(expression.accept(visitor));
    }
    return expressions1;
  }

  // ~ Classes and interfaces ------------------------------------------------

  // Some interfaces we'd rather not implement yet. They don't seem relevant
  // in the Java world.

  interface PropertyInfo {
  }

  interface RuntimeVariablesExpression {
  }

  interface SymbolDocumentInfo {
  }

  public interface FluentList<T> extends List<T> {
    FluentList<T> append(T t);

    FluentList<T> appendIf(boolean condition, T t);

    FluentList<T> appendIfNotNull(T t);

    FluentList<T> appendAll(Iterable<T> ts);

    FluentList<T> appendAll(T... ts);
  }

  private static class FluentArrayList<T> extends ArrayList<T>
      implements FluentList<T> {
    public FluentArrayList() {
      super();
    }

    public FluentArrayList(Collection<? extends T> c) {
      super(c);
    }

    public FluentList<T> append(T t) {
      add(t);
      return this;
    }

    public FluentList<T> appendIf(boolean condition, T t) {
      if (condition) {
        add(t);
      }
      return this;
    }

    public FluentList<T> appendIfNotNull(T t) {
      if (t != null) {
        add(t);
      }
      return this;
    }

    public FluentList<T> appendAll(Iterable<T> ts) {
      addAll(toCollection(ts));
      return this;
    }

    public FluentList<T> appendAll(T... ts) {
      addAll(Arrays.asList(ts));
      return this;
    }
  }
}

// End Expressions.java
