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
import java.util.*;

/**
 * Utility methods for expressions, including a lot of factory methods.
 */
public class Expressions {
    /** Dispatches to the specific visit method for this node
     * type. For example, MethodCallExpression calls the
     * VisitMethodCall. */
    public static MethodCallExpression accept(ExpressionVisitor visitor) {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * addition operation that does not have overflow checking. */
    public static BinaryExpression add(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(ExpressionType.Add, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * addition operation that does not have overflow checking. The
     * implementing method can be specified. */
    public static BinaryExpression add(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an addition
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression addAssign(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(ExpressionType.AddAssign, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents an addition
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression addAssign(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an addition
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression addAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression0,
        LambdaExpression lambdaExpression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an addition
     * assignment operation that has overflow checking. */
    public static BinaryExpression addAssignChecked(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(
            ExpressionType.AddAssignChecked, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents an addition
     * assignment operation that has overflow checking. */
    public static BinaryExpression addAssignChecked(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an addition
     * assignment operation that has overflow checking. */
    public static BinaryExpression addAssignChecked(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * addition operation that has overflow checking. */
    public static BinaryExpression addChecked(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(ExpressionType.AddChecked, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * addition operation that has overflow checking. The implementing
     * method can be specified. */
    public static BinaryExpression addChecked(
        Expression expression0, Expression expression1, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise AND
     * operation. */
    public static BinaryExpression and(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(ExpressionType.And, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a bitwise AND
     * operation. The implementing method can be specified. */
    public static BinaryExpression and(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a conditional AND
     * operation that evaluates the second operand only if the first
     * operand evaluates to true. */
    public static BinaryExpression andAlso(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.AndAlso, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a conditional AND
     * operation that evaluates the second operand only if the first
     * operand is resolved to true. The implementing method can be
     * specified. */
    public static BinaryExpression andAlso(
        Expression expression0, Expression expression1, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise AND
     * assignment operation. */
    public static BinaryExpression andAssign(
        Expression expression0, Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise AND
     * assignment operation. */
    public static BinaryExpression andAssign(
        Expression expression0, Expression expression1, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise AND
     * assignment operation. */
    public static BinaryExpression andAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        throw Extensions.todo();
    }

    /** Creates an IndexExpression to access a multidimensional
     * array. */
    public static IndexExpression arrayAccess(
        Expression array, Iterable<Expression> indexExpressions)
    {
        throw Extensions.todo();
    }

    /** Creates an IndexExpression to access an array. */
    public static IndexExpression arrayAccess(
        Expression array, Expression indexExpressions[]) {
        throw Extensions.todo();
    }

    /** Creates a MethodCallExpression that represents applying an
     * array index operator to an array of rank more than one. */
    public static MethodCallExpression arrayIndex(
        Expression array, Iterable<Expression> indexExpressions)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents applying an array
     * index operator to an array of rank one. */
    public static BinaryExpression arrayIndex(
        Expression array, Expression indexExpressions)
    {
        throw Extensions.todo();
    }

    /** Creates a MethodCallExpression that represents applying an
     * array index operator to a multidimensional array. */
    public static MethodCallExpression arrayIndex(
        Expression array, Expression indexExpressions[]) {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents an expression for
     * obtaining the length of a one-dimensional array. */
    public static UnaryExpression arrayLength(Expression array) {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an assignment
     * operation. */
    public static BinaryExpression assign(Expression left, Expression right) {
        throw Extensions.todo();
    }

    /** Creates a MemberAssignment that represents the initialization
     * of a field or property. */
    public static MemberAssignment bind(Member member, Expression right) {
        throw Extensions.todo();
    }

    /** Creates a MemberAssignment that represents the initialization
     * of a member by using a property accessor method. */
    public static MemberAssignment bind(Method method, Expression expression) {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains the given expressions
     * and has no variables. */
    public static BlockExpression block(Expression[] expressions) {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains the given expressions
     * and has no variables. */
    public static BlockExpression block(Iterable<Expression> expressions) {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains two expressions and
     * has no variables. */
    public static BlockExpression block(
        Expression expression0, Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains the given variables
     * and expressions. */
    public static BlockExpression block(
        Iterable<ParameterExpression> variables,
        Iterable<Expression> expressions)
    {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains the given variables
     * and expressions. */
    public static BlockExpression block(
        Iterable<ParameterExpression> variables, Expression[] expressions)
    {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains the given expressions,
     * has no variables and has specific result type. */
    public static BlockExpression block(
        Class clazz, Iterable<Expression> expressions)
    {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains the given expressions,
     * has no variables and has specific result type. */
    public static BlockExpression block(Class clazz, Expression[] expressions) {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains three expressions and
     * has no variables. */
    public static BlockExpression block(
        Expression expression0, Expression expression1, Expression expression2)
    {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains the given variables
     * and expressions. */
    public static BlockExpression block(
        Class clazz,
        Iterable<ParameterExpression> variables,
        Iterable<Expression> expressions)
    {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains the given variables
     * and expressions. */
    public static BlockExpression block(
        Class clazz,
        Iterable<ParameterExpression> variables,
        Expression[] expressions)
    {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains four expressions and
     * has no variables. */
    public static BlockExpression block(
        Expression expression0,
        Expression expression1,
        Expression expression2,
        Expression expression3)
    {
        throw Extensions.todo();
    }

    /** Creates a BlockExpression that contains five expressions and
     * has no variables. */
    public static BlockExpression block(
        Expression expression0,
        Expression expression1,
        Expression expression2,
        Expression expression3,
        Expression expression4)
    {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a break statement. */
    public static GotoExpression break_(LabelTarget labelTarget) {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a break statement. The
     * value passed to the label upon jumping can be specified. */
    public static GotoExpression break_(
        LabelTarget labelTarget,
        Expression expression)
    {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a break statement with
     * the specified type. */
    public static GotoExpression break_(LabelTarget labelTarget, Class clazz) {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a break statement with
     * the specified type. The value passed to the label upon jumping
     * can be specified. */
    public static GotoExpression break_(
        LabelTarget labelTarget,
        Expression expression,
        Class clazz)
    {
        throw Extensions.todo();
    }

    /** Creates a MethodCallExpression that represents a call to an
     * instance method that takes no arguments. */
    public static MethodCallExpression call(
        Expression expression,
        Method method)
    {
        return new MethodCallExpression(
            method,
            expression,
            Collections.<Expression>emptyList());
    }

    /** Creates a MethodCallExpression that represents a call to a
     * static method. */
    public static MethodCallExpression call(
        Method method,
        Iterable<Expression> expressions)
    {
        return new MethodCallExpression(
            method,
            null,
            toList(expressions));
    }

    /** Creates a MethodCallExpression that represents a call to a
     * static method that takes one argument. */
    public static MethodCallExpression call(
        Method method,
        Expression expression)
    {
        return new MethodCallExpression(
            method,
            null,
            Collections.singletonList(expression));
    }

    /** Creates a MethodCallExpression that represents a call to a
     * static method that has arguments. */
    public static MethodCallExpression call(
        Method method,
        Expression[] arguments)
    {
        return new MethodCallExpression(
            method,
            null,
            Arrays.asList(arguments));
    }

    /** Creates a MethodCallExpression that represents a call to a
     * method that takes arguments. */
    public static MethodCallExpression call(
        Expression expression,
        Method method,
        Iterable<Expression> arguments)
    {
        return new MethodCallExpression(
            method,
            expression,
            toList(arguments));
    }

    /** Creates a MethodCallExpression that represents a call to a
     * method that takes arguments. */
    public static MethodCallExpression call(
        Expression expression,
        Method method,
        Expression[] arguments)
    {
        return new MethodCallExpression(
            method,
            expression,
            Arrays.asList(arguments));
    }

    /** Creates a MethodCallExpression that represents a call to a
     * static method that takes two arguments. */
    public static MethodCallExpression call(
        Method method,
        Expression argument0,
        Expression argument1)
    {
        return new MethodCallExpression(
            method,
            null,
            Arrays.asList(argument0, argument1));
    }

    /** Creates a MethodCallExpression that represents a call to an
     * instance method that takes two arguments. */
    public static MethodCallExpression call(
        Expression expression,
        Method method,
        Expression argument0,
        Expression argument1)
    {
        return new MethodCallExpression(
            method,
            expression,
            Arrays.asList(argument0, argument1));
    }

    /** Creates a MethodCallExpression that represents a call to an
     * instance method by calling the appropriate factory method. */
    public static MethodCallExpression call(
        Expression expression,
        String methodName,
        Iterable<Class> typeArguments,
        Iterable<Expression> arguments)
    {
        List<Class> classes = new ArrayList<Class>();
        for (Expression argument : arguments) {
            classes.add(argument.getType());
        }
        Method method;
        try {
            method = expression.getType().getMethod(
                methodName, toArray(classes, new Class[0]));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                "while resolving method '" + methodName + "' in class "
                + expression.getType(),
                e);
        }
        return call(expression, method, arguments);
    }

    /** Creates a MethodCallExpression that represents a call to a
     * static method that takes three arguments. */
    public static MethodCallExpression call(
        Method method,
        Expression expression0,
        Expression expression1,
        Expression expression2)
    {
        return new MethodCallExpression(
            method,
            null,
            Arrays.asList(expression0, expression1, expression2));
    }

    /** Creates a MethodCallExpression that represents a call to a
     * static (Shared in Visual Basic) method by calling the
     * appropriate factory method. */
    public static MethodCallExpression call(
        Class clazz,
        String methodName,
        Iterable<Class> typeArguments,
        Iterable<Expression> arguments)
    {
        throw Extensions.todo();
    }

    /** Creates a MethodCallExpression that represents a call to a
     * method that takes three arguments. */
    public static MethodCallExpression call(
        Expression expression,
        Method method,
        Expression argument0,
        Expression argument1,
        Expression argument2)
    {
        return new MethodCallExpression(
            method,
            expression,
            Arrays.asList(argument0, argument1, argument2));
    }

    /** Creates a MethodCallExpression that represents a call to a
     * method that takes four arguments. */
    public static MethodCallExpression call(
        Expression expression,
        Method method,
        Expression argument0,
        Expression argument1,
        Expression argument2,
        Expression argument3)
    {
        return new MethodCallExpression(
            method,
            expression,
            Arrays.asList(argument0, argument1, argument2, argument3));
    }

    /** Creates a MethodCallExpression that represents a call to a
     * static method that takes four arguments. */
    public static MethodCallExpression call(
        Method method,
        Expression argument0,
        Expression argument1,
        Expression argument2,
        Expression argument3)
    {
        return new MethodCallExpression(
            method,
            null,
            Arrays.asList(argument0, argument1, argument2, argument3));
    }

    /** Creates a MethodCallExpression that represents a call to a
     * static method that takes five arguments. */
    public static MethodCallExpression call(
        Method method,
        Expression argument0,
        Expression argument1,
        Expression argument2,
        Expression argument3,
        Expression argument4)
    {
        return new MethodCallExpression(
            method,
            null,
            Arrays.asList(
                argument0, argument1, argument2, argument3, argument4));
    }

    /** Creates a CatchBlock representing a catch statement with a
     * reference to the caught Exception object for use in the handler
     * body. */
    public static CatchBlock catch_(
        ParameterExpression expression0,
        Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a CatchBlock representing a catch statement. */
    public static CatchBlock catch_(Class clazz, Expression expression) {
        throw Extensions.todo();
    }

    /** Creates a CatchBlock representing a catch statement with an
     * Exception filter and a reference to the caught Exception
     * object. */
    public static CatchBlock catch_(
        ParameterExpression expression0,
        Expression expression1,
        Expression expression2)
    {
        throw Extensions.todo();
    }

    /** Creates a CatchBlock representing a catch statement with an
     * Exception filter but no reference to the caught Exception
     * object. */
    public static CatchBlock catch_(
        Class clazz,
        Expression expression0,
        Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a DebugInfoExpression for clearing a sequence
     * point. */
    public static void ClearDebugInfo() {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a coalescing
     * operation. */
    public static BinaryExpression coalesce(
        Expression expression0,
        Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a coalescing
     * operation, given a conversion function. */
    public static BinaryExpression coalesce(
        Expression expression0,
        Expression expression1,
        LambdaExpression lambdaExpression)
    {
        throw Extensions.todo();
    }

    /** Creates a ConditionalExpression that represents a conditional
     * statement. */
    public static ConditionalExpression condition(
        Expression expression0,
        Expression expression1,
        Expression expression2)
    {
        throw Extensions.todo();
    }

    /** Creates a ConditionalExpression that represents a conditional
     * statement. */
    public static ConditionalExpression condition(
        Expression expression0,
        Expression expression1,
        Expression expression2,
        Class clazz)
    {
        throw Extensions.todo();
    }

    /** Creates a ConstantExpression that has the Value property set
     * to the specified value. */
    public static ConstantExpression constant(Object value) {
        return new ConstantExpression(value.getClass(), value);
    }

    /** Creates a ConstantExpression that has the Value and Type
     * properties set to the specified values. */
    public static ConstantExpression constant(Object value, Class clazz) {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a continue statement. */
    public static GotoExpression continue_(LabelTarget labelTarget) {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a continue statement
     * with the specified type. */
    public static GotoExpression continue_(
        LabelTarget labelTarget,
        Class clazz)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents a type conversion
     * operation. */
    public static UnaryExpression convert_(Expression expression, Class clazz) {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents a conversion
     * operation for which the implementing method is specified. */
    public static UnaryExpression convert_(
        Expression expression,
        Class type,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents a conversion
     * operation that throws an exception if the target type is
     * overflowed. */
    public static UnaryExpression convertChecked(
        Expression expression,
        Class type)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents a conversion
     * operation that throws an exception if the target type is
     * overflowed and for which the implementing method is
     * specified. */
    public static UnaryExpression convertChecked_(
        Expression expression,
        Class type,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a DebugInfoExpression with the specified span. */
    public static void debugInfo() {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents the decrementing of
     * the expression by 1. */
    public static UnaryExpression decrement(Expression expression) {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents the decrementing of
     * the expression by 1. */
    public static UnaryExpression decrement(
        Expression expression,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a DefaultExpression that has the Type property set to
     * the specified type. */
    public static DefaultExpression default_() {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * division operation. */
    public static BinaryExpression divide(
        Expression expression0,
        Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * division operation. The implementing method can be
     * specified. */
    public static BinaryExpression divide(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a division
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression divideAssign(
        Expression expression0,
        Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a division
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression divideAssign(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a division
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression divideAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder. */
    public static DynamicExpression dynamic(
        CallSiteBinder binder, Class type, Iterable<Expression> expressions)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder. */
    public static DynamicExpression dynamic(
        CallSiteBinder binder, Class type, Expression expression)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder. */
    public static DynamicExpression dynamic(
        CallSiteBinder binder, Class type, Expression[] expression)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder. */
    public static DynamicExpression dynamic(
        CallSiteBinder binary,
        Class type,
        Expression expression0,
        Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder. */
    public static DynamicExpression dynamic(
        CallSiteBinder binary,
        Class type,
        Expression expression0,
        Expression expression1,
        Expression expression2)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder. */
    public static DynamicExpression dynamic(
        CallSiteBinder binder,
        Class type,
        Expression expression0,
        Expression expression1,
        Expression expression2,
        Expression expression3)
    {
        throw Extensions.todo();
    }

    /** Creates an ElementInit, given an Iterable<T> as the second
     * argument. */
    public static ElementInit elementInit(
        Method method, Iterable<Expression> expressions)
    {
        throw Extensions.todo();
    }

    /** Creates an ElementInit, given an array of values as the second
     * argument. */
    public static ElementInit elementInit(
        Method method, Expression[] expressions)
    {
        throw Extensions.todo();
    }

    /** Creates an empty expression that has Void type. */
    public static DefaultExpression empty() {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an equality
     * comparison. */
    public static BinaryExpression equal(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.Equal, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents an equality
     * comparison. The implementing method can be specified. */
    public static BinaryExpression equal(
        Expression expression0,
        Expression expression1,
        boolean liftToNull,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise XOR
     * operation, using op_ExclusiveOr for user-defined types. */
    public static BinaryExpression exclusiveOr(
        Expression expression0, Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise XOR
     * operation, using op_ExclusiveOr for user-defined types. The
     * implementing method can be specified. */
    public static BinaryExpression exclusiveOr(
        Expression expression0, Expression expression1, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise XOR
     * assignment operation, using op_ExclusiveOr for user-defined
     * types. */
    public static BinaryExpression exclusiveOrAssign(
        Expression expression0, Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise XOR
     * assignment operation, using op_ExclusiveOr for user-defined
     * types. */
    public static BinaryExpression exclusiveOrAssign(
        Expression expression0, Expression expression1, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise XOR
     * assignment operation, using op_ExclusiveOr for user-defined
     * types. */
    public static BinaryExpression exclusiveOrAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberExpression that represents accessing a field. */
    public static MemberExpression field(Expression expression, Field field) {
        return makeMemberAccess(expression, field);
    }

    /** Creates a MemberExpression that represents accessing a field
     * given the name of the field. */
    public static MemberExpression field(
        Expression expression, String fieldName)
    {
        Field field;
        try {
            field = expression.getType().getField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(
                "while resolving field '" + fieldName + "' in class "
                + expression.getType(),
                e);
        }
        return makeMemberAccess(expression, field);
    }

    /** Creates a MemberExpression that represents accessing a field. */
    public static MemberExpression field(
        Expression expression, Class type, String fieldName)
    {
        try {
            Field field = type.getField(fieldName);
            return makeMemberAccess(expression, field);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(
                "Unknown field '" + fieldName + "' in class " + type, e);
        }
    }

    /** Creates a Type object that represents a generic System.Action
     * delegate type that has specific type arguments. */
    public static Class getActionType(Class[] typeArgs) {
        throw Extensions.todo();
    }

    /** Gets a Type object that represents a generic System.Func or
     * System.Action delegate type that has specific type
     * arguments. */
    public static Class getDelegateType(Class[] typeArgs) {
        throw Extensions.todo();
    }

    /** Creates a Type object that represents a generic System.Func
     * delegate type that has specific type arguments. The last type
     * argument specifies the return type of the created delegate. */
    public static Class getFuncType(Class[] typeArgs) {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a "go to" statement. */
    public static GotoExpression goto_(LabelTarget labelTarget) {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a "go to" statement. The
     * value passed to the label upon jumping can be specified. */
    public static GotoExpression goto_(
        LabelTarget labelTarget, Expression expression)
    {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a "go to" statement with
     * the specified type. */
    public static GotoExpression goto_(LabelTarget labelTarget, Class type) {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a "go to" statement with
     * the specified type. The value passed to the label upon jumping
     * can be specified. */
    public static GotoExpression goto_(
        LabelTarget labelTarget, Expression expression, Class type)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a "greater than"
     * numeric comparison. */
    public static BinaryExpression greaterThan(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.GreaterThan, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a "greater than"
     * numeric comparison. The implementing method can be
     * specified. */
    public static BinaryExpression greaterThan(
        Expression expression0,
        Expression expression1,
        boolean liftToNull,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a "greater than or
     * equal" numeric comparison. */
    public static BinaryExpression greaterThanOrEqual(
        Expression expression0, Expression expression1)
    {
        return makeBinary(
            ExpressionType.GreaterThanOrEqual, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a "greater than or
     * equal" numeric comparison. */
    public static BinaryExpression greaterThanOrEqual(
        Expression expression0,
        Expression expression1,
        boolean liftToNull,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a ConditionalExpression that represents a conditional
     * block with an if statement. */
    public static ConditionalExpression ifThen(
        Expression test, Expression ifTrue)
    {
        throw Extensions.todo();
    }

    /** Creates a ConditionalExpression that represents a conditional
     * block with if and else statements. */
    public static ConditionalExpression ifThenElse(
        Expression test, Expression ifTrue, Expression ifFalse)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents the incrementing of
     * the expression value by 1. */
    public static UnaryExpression increment(Expression expression) {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents the incrementing of
     * the expression by 1. */
    public static UnaryExpression increment(
        Expression expression, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates an InvocationExpression that applies a delegate or
     * lambda expression to a list of argument expressions. */
    public static InvocationExpression invoke(
        Expression expression, Iterable<Expression> arguments)
    {
        throw Extensions.todo();
    }

    /** Creates an InvocationExpression that applies a delegate or
     * lambda expression to a list of argument expressions. */
    public static InvocationExpression invoke(
        Expression expression, Expression arguments[]) {
        throw Extensions.todo();
    }

    /** Returns whether the expression evaluates to false. */
    public static UnaryExpression isFalse(Expression expression) {
        throw Extensions.todo();
    }

    /** Returns whether the expression evaluates to false. */
    public static UnaryExpression isFalse(
        Expression expression, Method method)
    {
        throw Extensions.todo();
    }

    /** Returns whether the expression evaluates to true. */
    public static UnaryExpression isTrue(Expression expression) {
        throw Extensions.todo();
    }

    /** Returns whether the expression evaluates to true. */
    public static UnaryExpression isTrue(Expression expression, Method method) {
        throw Extensions.todo();
    }

    /** Creates a LabelTarget representing a label with X type and
     * no name. */
    public static LabelTarget label() {
        throw Extensions.todo();
    }

    /** Creates a LabelExpression representing a label without a
     * default value. */
    public static LabelExpression label(LabelTarget labelTarget) {
        throw Extensions.todo();
    }

    /** Creates a LabelTarget representing a label with X type and
     * the given name. */
    public static LabelTarget label(String name) {
        throw Extensions.todo();
    }

    /** Creates a LabelTarget representing a label with the given
     * type. */
    public static LabelTarget label(Class type) {
        throw Extensions.todo();
    }

    /** Creates a LabelExpression representing a label with the given
     * default value. */
    public static LabelExpression label(
        LabelTarget labelTarget, Expression expression)
    {
        throw Extensions.todo();
    }

    /** Creates a LabelTarget representing a label with the given type
     * and name. */
    public static LabelTarget label(Class type, String name) {
        throw Extensions.todo();
    }

    /** Creates a FunctionExpression from an actual function. */
    public static <F extends Function<?>>
    FunctionExpression<F> lambda(F function) {
        // REVIEW: Check that that function class is non-inner, has a public
        // default constructor, etc.?

        //noinspection unchecked
        return new FunctionExpression<F>(function);
    }

    /** Creates a LambdaExpression by first constructing a delegate
     * type. */
    public static <F extends Function<?>>
    FunctionExpression<F> lambda(
        Expression body,
        Iterable<ParameterExpression> parameters)
    {
        final List<ParameterExpression> parameterList = toList(parameters);
        Class<F> type = deduceType(parameterList);
        return new FunctionExpression<F>(type, body, parameterList);
    }

    private static Class deduceType(List<ParameterExpression> parameterList) {
        switch (parameterList.size()) {
        case 0:
            return Function0.class;
        case 1:
            return Function1.class;
        case 2:
            return Function2.class;
        default:
            return Function.class;
        }
    }

    private static <T> List<T> toList(Iterable<T> iterable) {
        if (iterable instanceof List) {
            return (List<T>) iterable;
        }
        final List<T> list = new ArrayList<T>();
        for (T parameter : iterable) {
            list.add(parameter);
        }
        return list;
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

    /** Creates an Expression<TDelegate> where the delegate type is
     * known at compile time. */
    public static <TDelegate extends Function<?>>
    FunctionExpression<TDelegate> lambda(
        Expression body,
        ParameterExpression... parameters)
    {
        return lambda(body, Arrays.asList(parameters));
    }

    /** Creates a LambdaExpression by first constructing a delegate
     * type. */
    public static <T, F extends Function<? extends T>>
    FunctionExpression<F> lambda(
        Expression body,
        boolean tailCall,
        Iterable<ParameterExpression> parameters)
    {
        throw Extensions.todo();
    }

    /** Creates a LambdaExpression by first constructing a delegate
     * type. */
    public static <T, F extends Function<? extends T>>
    FunctionExpression<F> lambda(
        Expression body,
        boolean tailCall,
        ParameterExpression... parameters)
    {
        return lambda(body, tailCall, Arrays.asList(parameters));
    }

    /** Creates a LambdaExpression by first constructing a delegate
     * type. */
    public static <T, F extends Function<? extends T>>
    FunctionExpression<F> lambdaE(
        Expression body,
        String name,
        Iterable<ParameterExpression> parameters)
    {
        throw Extensions.todo();
    }

    /** Creates a LambdaExpression by first constructing a delegate
     * type. It can be used when the delegate type is not known at
     * compile time. */
    public static <T, F extends Function<? extends T>>
    FunctionExpression<F> lambda(
        Class<F> type,
        Expression body,
        Iterable<ParameterExpression> parameters)
    {
        return new FunctionExpression<F>(type, body, toList(parameters));
    }

    /** Creates a LambdaExpression by first constructing a delegate
     * type. It can be used when the delegate type is not known at
     * compile time. */
    public static <T, F extends Function<? extends T>>
    FunctionExpression<F> lambda(
        Class<F> type,
        Expression body,
        ParameterExpression... parameters)
    {
        return lambda(type, body, Arrays.asList(parameters));
    }

    /** Creates a LambdaExpression by first constructing a delegate
     * type. */
    public static <T, F extends Function<? extends T>>
    FunctionExpression<F> lambdaE(
        Expression body,
        String name,
        boolean tailCall,
        Iterable<ParameterExpression> parameters)
    {
        throw Extensions.todo();
    }

    /** Creates a LambdaExpression by first constructing a delegate
     * type. */
    public static <T, F extends Function<? extends T>>
    FunctionExpression<F> lambda(
        Class type,
        Expression expression,
        boolean tailCall,
        Iterable<ParameterExpression> parameters)
    {
        throw Extensions.todo();
    }

    /** Creates a LambdaExpression by first constructing a delegate
     * type. */
    public static <T, F extends Function<? extends T>>
    FunctionExpression<F> lambda(
        Class type,
        Expression body,
        boolean tailCall,
        ParameterExpression... parameters)
    {
        return lambda(type, body, tailCall, Arrays.asList(parameters));
    }

    /** Creates a LambdaExpression by first constructing a delegate
     * type. */
    public static <T, F extends Function<? extends T>>
    FunctionExpression<F> lambda(
        Class type,
        Expression body,
        String name,
        Iterable<ParameterExpression> parameters)
    {
        throw Extensions.todo();
    }

    /** Creates a LambdaExpression lambdaExpression by first constructing a
     * delegate type. */
    public static <T, F extends Function<? extends T>>
    FunctionExpression<F> lambda(
        Class type,
        Expression body,
        String name,
        boolean tailCall,
        Iterable<ParameterExpression> parameters)
    {
        throw Extensions.todo();
    }

    /** Creates an Expression<TDelegate> where the delegate type is
     * known at compile time. */
    public static <TDelegate extends Function<?>>
    FunctionExpression<TDelegate> lambda(
        Expression body,
        String name,
        Iterable<ParameterExpression> parameters)
    {
        throw Extensions.todo();
    }

    /** Creates an Expression<TDelegate> where the delegate type is
     * known at compile time. */
    public static <TDelegate extends Function<?>>
    FunctionExpression<TDelegate> lambda(
        Expression body,
        String name,
        boolean tailCall,
        Iterable<ParameterExpression> parameters)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise
     * left-shift operation. */
    public static BinaryExpression leftShift(
        Expression expression0, Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise
     * left-shift operation. */
    public static BinaryExpression leftShift(
        Expression expression0, Expression expression1, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise
     * left-shift assignment operation. */
    public static BinaryExpression leftShiftAssign(
        Expression expression0, Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise
     * left-shift assignment operation. */
    public static BinaryExpression leftShiftAssign(
        Expression expression0, Expression expression1, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise
     * left-shift assignment operation. */
    public static BinaryExpression leftShiftAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a "less than"
     * numeric comparison. */
    public static BinaryExpression lessThan(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.LessThan, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a "less than"
     * numeric comparison. */
    public static BinaryExpression lessThan(
        Expression expression0,
        Expression expression1,
        boolean liftToNull,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a " less than or
     * equal" numeric comparison. */
    public static BinaryExpression lessThanOrEqual(
        Expression expression0, Expression expression1)
    {
        return makeBinary(
            ExpressionType.LessThanOrEqual, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a "less than or
     * equal" numeric comparison. */
    public static BinaryExpression lessThanOrEqual(
        Expression expression0,
        Expression expression1,
        boolean liftToNull,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberListBinding where the member is a field or
     * property. */
    public static MemberListBinding listBind(
        Member member, Iterable<ElementInit> elementInits)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberListBinding where the member is a field or
     * property. */
    public static MemberListBinding listBind(
        Member member, ElementInit[] elementInits)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberListBinding based on a specified property
     * accessor method. */
    public static MemberListBinding listBind(
        Method method, Iterable<ElementInit> elementInits)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberListBinding object based on a specified
     * property accessor method. */
    public static MemberListBinding listBind(
        Method method, ElementInit[] elementInits)
    {
        throw Extensions.todo();
    }

    /** Creates a ListInitExpression that uses specified ElementInit
     * objects to initialize a collection. */
    public static ListInitExpression listInit(
        NewExpression newExpression, Iterable<ElementInit> elementInits)
    {
        throw Extensions.todo();
    }

    /** Creates a ListInitExpression that uses a method named "Add" to
     * add elements to a collection. */
    public static ListInitExpression listInitE(
        NewExpression newExpression, Iterable<Expression> arguments)
    {
        throw Extensions.todo();
    }

    /** Creates a ListInitExpression that uses specified ElementInit
     * objects to initialize a collection. */
    public static ListInitExpression listInit(
        NewExpression newExpression, ElementInit[] elementInits)
    {
        throw Extensions.todo();
    }

    /** Creates a ListInitExpression that uses a method named "Add" to
     * add elements to a collection. */
    public static ListInitExpression listInit(
        NewExpression newExpression, Expression[] arguments)
    {
        throw Extensions.todo();
    }

    /** Creates a ListInitExpression that uses a specified method to
     * add elements to a collection. */
    public static ListInitExpression listInit(
        NewExpression newExpression,
        Method method,
        Iterable<Expression> arguments)
    {
        throw Extensions.todo();
    }

    /** Creates a ListInitExpression that uses a specified method to
     * add elements to a collection. */
    public static ListInitExpression listInit(
        NewExpression newExpression, Method method, Expression[] arguments)
    {
        throw Extensions.todo();
    }

    /** Creates a LoopExpression with the given body. */
    public static LoopExpression loop(Expression body) {
        throw Extensions.todo();
    }

    /** Creates a LoopExpression with the given body and break
     * target. */
    public static LoopExpression loop(
        Expression body, LabelTarget breakTarget)
    {
        throw Extensions.todo();
    }

    /** Creates a LoopExpression with the given body. */
    public static LoopExpression loop(
        Expression body, LabelTarget breakTarget, LabelTarget continueTarget)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression, given the left and right operands,
     * by calling an appropriate factory method. */
    public static BinaryExpression makeBinary(
        ExpressionType binaryType,
        Expression expression0,
        Expression expression1)
    {
        final Class type;
        switch (binaryType) {
        case Equal:
        case NotEqual:
        case LessThan:
        case LessThanOrEqual:
        case GreaterThan:
        case GreaterThanOrEqual:
            type = Boolean.TYPE;
            break;
        default:
            type = expression0.getType();
            break;
        }
        return new BinaryExpression(
            binaryType, type, expression0, expression1);
    }

    /** Creates a BinaryExpression, given the left operand, right
     * operand and implementing method, by calling the appropriate
     * factory method. */
    public static BinaryExpression makeBinary(
        ExpressionType binaryType,
        Expression expression0,
        Expression expression1,
        boolean liftToNull,
        Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression, given the left operand, right
     * operand, implementing method and type conversion function, by
     * calling the appropriate factory method. */
    public static BinaryExpression makeBinary(
        ExpressionType binaryType,
        Expression expression0,
        Expression expression1,
        boolean liftToNull,
        Method method,
        LambdaExpression lambdaExpression)
    {
        throw Extensions.todo();
    }

    /** Creates a CatchBlock representing a catch statement with the
     * specified elements. */
    public static CatchBlock makeCatchBlock(
        Class type,
        ParameterExpression variable,
        Expression body,
        Expression filter)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder. */
    public static DynamicExpression makeDynamic(
        Class type, CallSiteBinder binder, Iterable<Expression> arguments)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder and one
     * argument. */
    public static DynamicExpression makeDynamic(
        Class type, CallSiteBinder binder, Expression argument)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder. */
    public static DynamicExpression MakeDynamic(
        Class type, CallSiteBinder binder, Expression[] arguments)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder and two
     * arguments. */
    public static DynamicExpression makeDynamic(
        Class type,
        CallSiteBinder binder,
        Expression expression0,
        Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder and three
     * arguments. */
    public static DynamicExpression makeDynamic(
        Class type,
        CallSiteBinder binder,
        Expression expression0,
        Expression expression1,
        Expression expression2)
    {
        throw Extensions.todo();
    }

    /** Creates a DynamicExpression that represents a dynamic
     * operation bound by the provided CallSiteBinder and four
     * arguments. */
    public static DynamicExpression makeDynamic(
        Class type,
        CallSiteBinder binder,
        Expression expression0,
        Expression expression1,
        Expression expression2,
        Expression expression3)
    {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a jump of the specified
     * GotoExpressionKind. The value passed to the label upon jumping
     * can also be specified. */
    public static GotoExpression makeGoto(
        GotoExpressionKind kind,
        LabelTarget target,
        Expression value,
        Class type)
    {
        throw Extensions.todo();
    }

    /** Creates an IndexExpression that represents accessing an
     * indexed property in an object. */
    public static IndexExpression makeIndex(
        Expression instance,
        PropertyInfo indexer,
        Iterable<Expression> arguments)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberExpression that represents accessing a field. */
    public static MemberExpression makeMemberAccess(
        Expression expression,
        Field member)
    {
        return new MemberExpression(expression, member);
    }

    /** Creates a TryExpression representing a try block with the
     * specified elements. */
    public static TryExpression makeTry(
        Class type,
        Expression body,
        Expression finally_,
        Expression fault,
        Iterable<CatchBlock> handlers)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression, given an operand, by calling the
     * appropriate factory method. */
    public static UnaryExpression makeUnary(
        ExpressionType expressionType, Expression expression, Class type)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression, given an operand and implementing
     * method, by calling the appropriate factory method. */
    public static UnaryExpression makeUnary(
        ExpressionType expressionType,
        Expression expression,
        Class type,
        Method method)
    {
        assert type != null;
        throw Extensions.todo();
    }

    /** Creates a MemberMemberBinding that represents the recursive
     * initialization of members of a field or property. */
    public static MemberMemberBinding memberBind(
        Member member, Iterable<MemberBinding> bindings)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberMemberBinding that represents the recursive
     * initialization of members of a field or property. */
    public static MemberMemberBinding memberBind(
        Member member, MemberBinding[] bindings)
    {
        return memberBind(member, Arrays.asList(bindings));
    }

    /** Creates a MemberMemberBinding that represents the recursive
     * initialization of members of a member that is accessed by using
     * a property accessor method. */
    public static MemberMemberBinding memberBind(
        Method method, Iterable<MemberBinding> bindings)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberMemberBinding that represents the recursive
     * initialization of members of a member that is accessed by using
     * a property accessor method. */
    public static MemberMemberBinding memberBind(
        Method method, MemberBinding[] bindings)
    {
        return memberBind(method, Arrays.asList(bindings));
    }

    /** Represents an expression that creates a new object and
     * initializes a property of the object. */
    public static MemberInitExpression memberInit(
        NewExpression newExpression, Iterable<MemberBinding> bindings)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberInitExpression. */
    public static MemberInitExpression memberInit(
        NewExpression newExpression, MemberBinding[] bindings)
    {
        return memberInit(newExpression, Arrays.asList(bindings));
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * remainder operation. */
    public static BinaryExpression modulo(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.Modulo, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * remainder operation. */
    public static BinaryExpression modulo(
        Expression expression0, Expression expression1, Method method)
    {
        return makeBinary(
            ExpressionType.Modulo,
            expression0,
            expression1,
            shouldLift(expression0, expression1, method),
            method);
    }

    private static boolean shouldLift(
        Expression expression0, Expression expression1, Method method)
    {
        // FIXME: Implement the rules in modulo
        return true;
    }

    /** Creates a BinaryExpression that represents a remainder
     * assignment operation. */
    public static BinaryExpression moduloAssign(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(
            ExpressionType.ModuloAssign,
            expression0,
            expression1);
    }

    /** Creates a BinaryExpression that represents a remainder
     * assignment operation. */
    public static BinaryExpression moduloAssign(
        Expression expression0, Expression expression1, Method method)
    {
        return makeBinary(
            ExpressionType.ModuloAssign,
            expression0,
            expression1,
            false,
            method);
    }

    /** Creates a BinaryExpression that represents a remainder
     * assignment operation. */
    public static BinaryExpression moduloAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        return makeBinary(
            ExpressionType.ModuloAssign,
            expression0,
            expression1,
            false,
            method,
            lambdaExpression);
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * multiplication operation that does not have overflow
     * checking. */
    public static BinaryExpression multiply(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.Multiply, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * multiplication operation that does not have overflow
     * checking. */
    public static BinaryExpression multiply(
        Expression expression0, Expression expression1, Method method)
    {
        return makeBinary(
            ExpressionType.Multiply,
            expression0,
            expression1,
            shouldLift(expression0, expression1, method),
            method);
    }

    /** Creates a BinaryExpression that represents a multiplication
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression multiplyAssign(
        Expression expression0, Expression expression1)
    {
        return makeBinary(
            ExpressionType.MultiplyAssign, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a multiplication
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression multiplyAssign(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        return makeBinary(
            ExpressionType.MultiplyAssign,
            expression0,
            expression1,
            false,
            method);
    }

    /** Creates a BinaryExpression that represents a multiplication
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression multiplyAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        return makeBinary(
            ExpressionType.MultiplyAssign,
            expression0,
            expression1,
            false,
            method,
            lambdaExpression);
    }

    /** Creates a BinaryExpression that represents a multiplication
     * assignment operation that has overflow checking. */
    public static BinaryExpression multiplyAssignChecked(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(
            ExpressionType.MultiplyAssignChecked, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a multiplication
     * assignment operation that has overflow checking. */
    public static BinaryExpression multiplyAssignChecked(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        return makeBinary(
            ExpressionType.MultiplyAssignChecked,
            expression0,
            expression1,
            false,
            method);
    }

    /** Creates a BinaryExpression that represents a multiplication
     * assignment operation that has overflow checking. */
    public static BinaryExpression multiplyAssignChecked(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        return makeBinary(
            ExpressionType.MultiplyAssignChecked,
            expression0,
            expression1,
            false,
            method,
            lambdaExpression);
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * multiplication operation that has overflow checking. */
    public static BinaryExpression multiplyChecked(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(
            ExpressionType.MultiplyChecked, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * multiplication operation that has overflow checking. */
    public static BinaryExpression multiplyChecked(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        return makeBinary(
            ExpressionType.MultiplyChecked,
            expression0,
            expression1,
            shouldLift(expression0, expression1, method),
            method);
    }

    /** Creates a UnaryExpression that represents an arithmetic
     * negation operation. */
    public static UnaryExpression negate(Expression expression) {
        return makeUnary(ExpressionType.Negate, expression, null);
    }

    /** Creates a UnaryExpression that represents an arithmetic
     * negation operation. */
    public static UnaryExpression negate(
        Expression expression,
        Method method)
    {
        return makeUnary(ExpressionType.Negate, expression, null, method);
    }

    /** Creates a UnaryExpression that represents an arithmetic
     * negation operation that has overflow checking. */
    public static UnaryExpression negateChecked(Expression expression)  {
        return makeUnary(ExpressionType.NegateChecked, expression, null);
    }

    /** Creates a UnaryExpression that represents an arithmetic
     * negation operation that has overflow checking. The implementing
     * method can be specified. */
    public static UnaryExpression negateChecked(
        Expression expression,
        Method method)
    {
        return makeUnary(
            ExpressionType.NegateChecked, expression, null, method);
    }

    /** Creates a NewExpression that represents calling the specified
     * constructor that takes no arguments. */
    public static NewExpression new_(Constructor constructor) {
        throw Extensions.todo();
    }

    /** Creates a NewExpression that represents calling the
     * parameterless constructor of the specified type. */
    public static NewExpression new_(Class type) {
        throw Extensions.todo();
    }

    /** Creates a NewExpression that represents calling the specified
     * constructor with the specified arguments. */
    public static NewExpression new_(
        Constructor constructor, Iterable<Expression> expressions)
    {
        throw Extensions.todo();
    }

    /** Creates a NewExpression that represents calling the specified
     * constructor with the specified arguments. */
    public static NewExpression new_(
        Constructor constructor, Expression[] expressions)
    {
        return new_(constructor, Arrays.asList(expressions));
    }

    /** Creates a NewExpression that represents calling the specified
     * constructor with the specified arguments. The members that
     * access the constructor initialized fields are specified. */
    public static NewExpression new_(
        Constructor constructor,
        Iterable<Expression> expressions,
        Iterable<Member> members)
    {
        throw Extensions.todo();
    }

    /** Creates a NewExpression that represents calling the specified
     * constructor with the specified arguments. The members that
     * access the constructor initialized fields are specified as an
     * array. */
    public static NewExpression new_(
        Constructor constructor,
        Iterable<Expression> expressions,
        Member[] members)
    {
        return new_(constructor, expressions, Arrays.asList(members));
    }

    /** Creates a NewArrayExpression that represents creating an array
     * that has a specified rank. */
    public static NewArrayExpression newArrayBounds(
        Class type, Iterable<Expression> expressions)
    {
        throw Extensions.todo();
    }

    /** Creates a NewArrayExpression that represents creating an array
     * that has a specified rank. */
    public static NewArrayExpression newArrayBounds(
        Class type, Expression[] expressions)
    {
        return newArrayBounds(type, Arrays.asList(expressions));
    }

    /** Creates a NewArrayExpression that represents creating a
     * one-dimensional array and initializing it from a list of
     * elements. */
    public static NewArrayExpression newArrayInit(
        Class type, Iterable<Expression> expressions)
    {
        throw Extensions.todo();
    }

    /** Creates a NewArrayExpression that represents creating a
     * one-dimensional array and initializing it from a list of
     * elements. */
    public static NewArrayExpression newArrayInit(
        Class type, Expression[] expressions)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents a bitwise complement
     * operation. */
    public static UnaryExpression not(Expression expression) {
        return makeUnary(ExpressionType.Not, expression, expression.getType());
    }

    /** Creates a UnaryExpression that represents a bitwise complement
     * operation. The implementing method can be specified. */
    public static UnaryExpression not(Expression expression, Method method) {
        return makeUnary(ExpressionType.Not, expression, null, method);
    }

    /** Creates a BinaryExpression that represents an inequality
     * comparison. */
    public static BinaryExpression notEqual(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.NotEqual, expression0,  expression1);
    }

    /** Creates a BinaryExpression that represents an inequality
     * comparison. */
    public static BinaryExpression notEqual(
        Expression expression0,
        Expression expression1,
        boolean liftToNull,
        Method method)
    {
        return makeBinary(
            ExpressionType.NotEqual,
            expression0,
            expression1,
            liftToNull,
            method);
    }

    /** Returns the expression representing the ones complement. */
    public static UnaryExpression onesComplement(Expression expression)  {
        return makeUnary(
            ExpressionType.OnesComplement, expression, expression.getType());
    }

    /** Returns the expression representing the ones complement. */
    public static UnaryExpression onesComplement(
        Expression expression,
        Method method)
    {
        return makeUnary(
            ExpressionType.OnesComplement,
            expression,
            expression.getType(),
            method);
    }

    /** Creates a BinaryExpression that represents a bitwise OR
     * operation. */
    public static BinaryExpression or(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.Or, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a bitwise OR
     * operation. */
    public static BinaryExpression or(
        Expression expression0, Expression expression1, Method method)
    {
        return makeBinary(
            ExpressionType.Or, expression0, expression1, false, method);
    }

    /** Creates a BinaryExpression that represents a bitwise OR
     * assignment operation. */
    public static BinaryExpression orAssign(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.OrAssign, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a bitwise OR
     * assignment operation. */
    public static BinaryExpression orAssign(
        Expression expression0, Expression expression1, Method method)
    {
        return makeBinary(ExpressionType.OrAssign, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a bitwise OR
     * assignment operation. */
    public static BinaryExpression orAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        return makeBinary(
            ExpressionType.OrAssign,
            expression0,
            expression1,
            false,
            method,
            lambdaExpression);
    }

    /** Creates a BinaryExpression that represents a conditional OR
     * operation that evaluates the second operand only if the first
     * operand evaluates to false. */
    public static BinaryExpression orElse(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.OrElse, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a conditional OR
     * operation that evaluates the second operand only if the first
     * operand evaluates to false. */
    public static BinaryExpression orElse(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        return makeBinary(
            ExpressionType.OrElse, expression0, expression1, false, method);
    }

    /** Creates a ParameterExpression node that can be used to
     * identify a parameter or a variable in an expression tree. */
    public static ParameterExpression parameter(Class type) {
        return new ParameterExpression(type);
    }

    /** Creates a ParameterExpression node that can be used to
     * identify a parameter or a variable in an expression tree. */
    public static ParameterExpression parameter(Class type, String name) {
        return new ParameterExpression(type, name);
    }

    /** Creates a UnaryExpression that represents the assignment of
     * the expression followed by a subsequent decrement by 1 of the
     * original expression. */
    public static UnaryExpression postDecrementAssign(Expression expression) {
        return makeUnary(
            ExpressionType.PostDecrementAssign,
            expression,
            expression.getType());
    }

    /** Creates a UnaryExpression that represents the assignment of
     * the expression followed by a subsequent decrement by 1 of the
     * original expression. */
    public static UnaryExpression postDecrementAssign(
        Expression expression,
        Method method)
    {
        return makeUnary(
            ExpressionType.PostDecrementAssign, expression,
            expression.getType(), method);
    }

    /** Creates a UnaryExpression that represents the assignment of
     * the expression followed by a subsequent increment by 1 of the
     * original expression. */
    public static UnaryExpression postIncrementAssign(Expression expression) {
        return makeUnary(
            ExpressionType.PostIncrementAssign,
            expression,
            expression.getType());
    }

    /** Creates a UnaryExpression that represents the assignment of
     * the expression followed by a subsequent increment by 1 of the
     * original expression. */
    public static UnaryExpression PostIncrementAssign(
        Expression expression, Method method)
    {
        return makeUnary(
            ExpressionType.PostIncrementAssign,
            expression,
            expression.getType(),
            method);
    }

    /** Creates a BinaryExpression that represents raising a number to
     * a power. */
    // REVIEW: In Java this is a call to a lib function, Math.pow.
    public static BinaryExpression power(
        Expression expression0, Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents raising a number to
     * a power. */
    // REVIEW: In Java this is a call to a lib function, Math.pow.
    public static BinaryExpression power(
        Expression expression0, Expression expression1, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents raising an
     * expression to a power and assigning the result back to the
     * expression. */
    // REVIEW: In Java this is a call to a lib function, Math.pow.
    public static BinaryExpression powerAssign(
        Expression expression0, Expression expression1)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents raising an
     * expression to a power and assigning the result back to the
     * expression. */
    // REVIEW: In Java this is a call to a lib function, Math.pow.
    public static BinaryExpression powerAssign(
        Expression expression0, Expression expression1, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents raising an
     * expression to a power and assigning the result back to the
     * expression. */
    public static BinaryExpression powerAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that decrements the expression by 1
     * and assigns the result back to the expression. */
    public static UnaryExpression PreDecrementAssign(Expression expression) {
        return makeUnary(
            ExpressionType.PreDecrementAssign,
            expression,
            expression.getType());
    }

    /** Creates a UnaryExpression that decrements the expression by 1
     * and assigns the result back to the expression. */
    public static UnaryExpression preDecrementAssign(
        Expression expression,
        Method method)
    {
        return makeUnary(
            ExpressionType.PreDecrementAssign,
            expression,
            expression.getType(),
            method);
    }

    /** Creates a UnaryExpression that increments the expression by 1
     * and assigns the result back to the expression. */
    public static UnaryExpression preIncrementAssign(Expression expression)  {
        return makeUnary(
            ExpressionType.PreIncrementAssign,
            expression,
            expression.getType());
    }

    /** Creates a UnaryExpression that increments the expression by 1
     * and assigns the result back to the expression. */
    public static UnaryExpression preIncrementAssign(
        Expression expression,
        Method method)
    {
        return makeUnary(
            ExpressionType.PreIncrementAssign,
            expression,
            expression.getType(),
            method);
    }

    /** Creates a MemberExpression that represents accessing a
     * property by using a property accessor method. */
    // REVIEW: No equivalent to properties in Java.
    public static MemberExpression property(
        Expression expression, Method method)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberExpression that represents accessing a
     * property. */
    // REVIEW: No equivalent to properties in Java.
    public static MemberExpression property(
        Expression expression, PropertyInfo property)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberExpression that represents accessing a
     * property. */
    // REVIEW: No equivalent to properties in Java.
    public static MemberExpression property(
        Expression expression, String name)
    {
        throw Extensions.todo();
    }

    /** Creates an IndexExpression representing the access to an
     * indexed property. */
    // REVIEW: No equivalent to properties in Java.
    public static IndexExpression property(
        Expression expression,
        PropertyInfo property,
        Iterable<Expression> arguments)
    {
        throw Extensions.todo();
    }

    /** Creates an IndexExpression representing the access to an
     * indexed property. */
    // REVIEW: No equivalent to properties in Java.
    public static IndexExpression property(
        Expression expression, PropertyInfo property, Expression[] arguments)
    {
        throw Extensions.todo();
    }

    /** Creates an IndexExpression representing the access to an
     * indexed property. */
    // REVIEW: No equivalent to properties in Java.
    public static IndexExpression Property(
        Expression expression, String name, Expression[] arguments)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberExpression accessing a property. */
    public static MemberExpression property(
        Expression expression, Class type, String name)
    {
        throw Extensions.todo();
    }

    /** Creates a MemberExpression that represents accessing a
     * property or field. */
    // REVIEW: Java does not have properties; can only be a field name.
    public static MemberExpression propertyOrField(
        Expression expression, String propertyOfFieldName)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents an expression that
     * has a constant value of type Expression. */
    public static UnaryExpression quote(Expression expression) {
        return makeUnary(
            ExpressionType.Quote, expression, expression.getType());
    }

    /** Reduces this node to a simpler expression. If CanReduce
     * returns true, this should return a valid expression. This
     * method can return another node which itself must be reduced. */
    public static Expression reduce(Expression expression) {
        throw Extensions.todo();
    }

    /** Reduces this node to a simpler expression. If CanReduce
     * returns true, this should return a valid expression. This
     * method can return another node which itself must be reduced. */
    public static Expression reduceAndCheck(Expression expression) {
        throw Extensions.todo();
    }

    /** Reduces the expression to a known node type (that is not an
     * Extension node) or just returns the expression if it is already
     * a known type. */
    public static Expression reduceExtensions(Expression expression) {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a reference
     * equality comparison. */
    public static Expression referenceEqual(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.Equal, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a reference
     * inequality comparison. */
    public static Expression referenceNotEqual(
        Expression expression0, Expression expression1)
    {
        return makeBinary(ExpressionType.NotEqual, expression0, expression1);
    }

    /** Creates a UnaryExpression that represents a rethrowing of an
     * exception. */
    public static UnaryExpression rethrow() {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents a rethrowing of an
     * exception with a given type. */
    public static UnaryExpression rethrow(Class type) {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a return statement. */
    public static GotoExpression return_(LabelTarget labelTarget) {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a return statement. The
     * value passed to the label upon jumping can be specified. */
    public static GotoExpression return_(
        LabelTarget labelTarget, Expression expression)
    {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a return statement with
     * the specified type. */
    public static GotoExpression return_(LabelTarget labelTarget, Class type) {
        throw Extensions.todo();
    }

    /** Creates a GotoExpression representing a return statement with
     * the specified type. The value passed to the label upon jumping
     * can be specified. */
    public static GotoExpression return_(
        LabelTarget labelTarget, Expression expression, Class type)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents a bitwise
     * right-shift operation. */
    public static BinaryExpression rightShift(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(ExpressionType.RightShift, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a bitwise
     * right-shift operation. */
    public static BinaryExpression rightShift(
        Expression expression0, Expression expression1, Method method)
    {
        return makeBinary(
            ExpressionType.RightShift, expression0, expression1, false, method);
    }

    /** Creates a BinaryExpression that represents a bitwise
     * right-shift assignment operation. */
    public static BinaryExpression rightShiftAssign(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(
            ExpressionType.RightShiftAssign, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a bitwise
     * right-shift assignment operation. */
    public static BinaryExpression rightShiftAssign(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        return makeBinary(
            ExpressionType.RightShiftAssign,
            expression0,
            expression1,
            false,
            method);
    }

    /** Creates a BinaryExpression that represents a bitwise
     * right-shift assignment operation. */
    public static BinaryExpression rightShiftAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        return makeBinary(
            ExpressionType.RightShiftAssign,
            expression0,
            expression1,
            false,
            method,
            lambdaExpression);
    }

    /** Creates an instance of RuntimeVariablesExpression. */
    public static RuntimeVariablesExpression runtimeVariables(
        Iterable<ParameterExpression> expressions)
    {
        throw Extensions.todo();
    }

    /** Creates an instance of RuntimeVariablesExpression. */
    public static RuntimeVariablesExpression runtimeVariables(
        ParameterExpression[] arguments)
    {
        throw Extensions.todo();
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * subtraction operation that does not have overflow checking. */
    public static BinaryExpression subtract(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(ExpressionType.Subtract, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * subtraction operation that does not have overflow checking. */
    public static BinaryExpression subtract(
        Expression expression0, Expression expression1, Method method)
    {
        return makeBinary(
            ExpressionType.Subtract,
            expression0,
            expression1,
            shouldLift(expression0, expression1, method),
            method);
    }

    /** Creates a BinaryExpression that represents a subtraction
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression subtractAssign(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(
            ExpressionType.SubtractAssign, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a subtraction
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression subtractAssign(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        return makeBinary(
            ExpressionType.SubtractAssign,
            expression0,
            expression1,
            false,
            method);
    }

    /** Creates a BinaryExpression that represents a subtraction
     * assignment operation that does not have overflow checking. */
    public static BinaryExpression subtractAssign(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        return makeBinary(
            ExpressionType.SubtractAssign,
            expression0,
            expression1,
            false,
            method,
            lambdaExpression);
    }

    /** Creates a BinaryExpression that represents a subtraction
     * assignment operation that has overflow checking. */
    public static BinaryExpression subtractAssignChecked(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(
            ExpressionType.SubtractAssignChecked, expression0, expression1);
    }

    /** Creates a BinaryExpression that represents a subtraction
     * assignment operation that has overflow checking. */
    public static BinaryExpression subtractAssignChecked(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        return makeBinary(
            ExpressionType.SubtractAssignChecked,
            expression0,
            expression1,
            false,
            method);
    }

    /** Creates a BinaryExpression that represents a subtraction
     * assignment operation that has overflow checking. */
    public static BinaryExpression subtractAssignChecked(
        Expression expression0,
        Expression expression1,
        Method method,
        LambdaExpression lambdaExpression)
    {
        return makeBinary(
            ExpressionType.SubtractAssignChecked,
            expression0,
            expression1,
            false,
            method,
            lambdaExpression);
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * subtraction operation that has overflow checking. */
    public static BinaryExpression subtractChecked(
        Expression expression0,
        Expression expression1)
    {
        return makeBinary(
            ExpressionType.SubtractChecked,
            expression0,
            expression1);
    }

    /** Creates a BinaryExpression that represents an arithmetic
     * subtraction operation that has overflow checking. */
    public static BinaryExpression subtractChecked(
        Expression expression0,
        Expression expression1,
        Method method)
    {
        return makeBinary(
            ExpressionType.SubtractChecked,
            expression0,
            expression1,
            shouldLift(expression0, expression1, method),
            method);
    }

    /** Creates a SwitchExpression that represents a switch statement
     * without a default case. */
    public static SwitchExpression switch_(
        Expression switchValue, SwitchCase[] cases)
    {
        return switch_(switchValue, null, null, Arrays.asList(cases));
    }

    /** Creates a SwitchExpression that represents a switch statement
     * that has a default case. */
    public static SwitchExpression switch_(
        Expression switchValue, Expression defaultBody, SwitchCase[] cases)
    {
        return switch_(switchValue, defaultBody, null, Arrays.asList(cases));
    }

    /** Creates a SwitchExpression that represents a switch statement
     * that has a default case. */
    public static SwitchExpression switch_(
        Expression switchValue,
        Expression defaultBody,
        Method method,
        Iterable<SwitchCase> cases)
    {
        throw Extensions.todo();
    }

    /** Creates a SwitchExpression that represents a switch statement
     * that has a default case. */
    public static SwitchExpression switch_(
        Expression switchValue,
        Expression defaultBody,
        Method method,
        SwitchCase[] cases)
    {
        return switch_(switchValue, defaultBody, method, Arrays.asList(cases));
    }

    /** Creates a SwitchExpression that represents a switch statement
     * that has a default case. */
    public static SwitchExpression switch_(
        Class type,
        Expression switchValue,
        Expression defaultBody,
        Method method,
        Iterable<SwitchCase> cases)
    {
        throw Extensions.todo();
    }

    /** Creates a SwitchExpression that represents a switch statement
     * that has a default case.. */
    public static SwitchExpression switch_(
        Class type,
        Expression switchValue,
        Expression defaultBody,
        Method method,
        SwitchCase[] cases)
    {
        return switch_(
            type, switchValue, defaultBody, method, Arrays.asList(cases));
    }

    /** Creates a SwitchCase object to be used in a SwitchExpression
     * object. */
    public static SwitchCase switchCase(
        Expression expression, Iterable<Expression> body)
    {
        throw Extensions.todo();
    }

    /** Creates a SwitchCase for use in a SwitchExpression. */
    public static SwitchCase switchCase(
        Expression expression, Expression [] body)
    {
        return switchCase(expression, Arrays.asList(body));
    }

    /** Creates an instance of SymbolDocumentInfo. */
    public static SymbolDocumentInfo symbolDocument(String fileName) {
        throw Extensions.todo();
    }

    /** Creates an instance of SymbolDocumentInfo. */
    public static SymbolDocumentInfo symbolDocument(
        String fileName,
        UUID language)
    {
        throw Extensions.todo();
    }

    /** Creates an instance of SymbolDocumentInfo. */
    public static SymbolDocumentInfo symbolDocument(
        String fileName, UUID language, UUID vendor)
    {
        throw Extensions.todo();
    }

    /** Creates an instance of SymbolDocumentInfo. */
    public static SymbolDocumentInfo symbolDocument(
        String filename, UUID language, UUID vendor, UUID documentType)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents a throwing of an
     * exception. */
    public static UnaryExpression throw_(Expression expression) {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents a throwing of an
     * exception with a given type. */
    public static UnaryExpression throw_(Expression expression, Class type) {
        throw Extensions.todo();
    }

    /** Creates a TryExpression representing a try block with any
     * number of catch statements and neither a fault nor finally
     * block. */
    public static TryExpression tryCatch(
        Expression body, CatchBlock[] handlers)
    {
        throw Extensions.todo();
    }

    /** Creates a TryExpression representing a try block with any
     * number of catch statements and a finally block. */
    public static TryExpression tryCatchFinally(
        Expression body, CatchBlock[] handlers)
    {
        throw Extensions.todo();
    }

    /** Creates a TryExpression representing a try block with a fault
     * block and no catch statements. */
    public static TryExpression tryFault(Expression body, Expression fault) {
        throw Extensions.todo();
    }

    /** Creates a TryExpression representing a try block with a
     * finally block and no catch statements. */
    public static TryExpression tryFinally(Expression body, Expression fault) {
        throw Extensions.todo();
    }

    /** Creates a Type object that represents a generic System.Action
     * delegate type that has specific type arguments. */
    public static boolean tryGetActionType(
        Class[] typeArgs, Class[] outActionType)
    {
        throw Extensions.todo();
    }

    /** Creates a Type object that represents a generic System.Func
     * delegate type that has specific type arguments. The last type
     * argument specifies the return type of the created delegate. */
    public static boolean tryGetFuncType(
        Class[] typeArgs, Class[] outFuncType)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents an explicit
     * reference or boxing conversion where null is supplied if the
     * conversion fails. */
    public static UnaryExpression typeAs(Expression expression, Class type) {
        throw Extensions.todo();
    }

    /** Creates a TypeBinaryExpression that compares run-time type
     * identity. */
    public static TypeBinaryExpression typeEqual(
        Expression expression, Class type)
    {
        throw Extensions.todo();
    }

    /** Creates a TypeBinaryExpression. */
    public static TypeBinaryExpression typeIs(
        Expression expression, Class type)
    {
        throw Extensions.todo();
    }

    /** Creates a UnaryExpression that represents a unary plus
     * operation. */
    public static UnaryExpression unaryPlus(Expression expression) {
        return makeUnary(
            ExpressionType.UnaryPlus, expression, expression.getType());
    }

    /** Creates a UnaryExpression that represents a unary plus
     * operation. */
    public static UnaryExpression unaryPlus(
        Expression expression,
        Method method)
    {
        return makeUnary(
            ExpressionType.UnaryPlus, expression, expression.getType(), method);
    }

    /** Creates a UnaryExpression that represents an explicit
     * unboxing. */
    public static UnaryExpression unbox(Expression expression, Class type)  {
        return makeUnary(ExpressionType.Unbox, expression, type);
    }

    /** Creates a ParameterExpression node that can be used to
     * identify a parameter or a variable in an expression tree. */
    public static ParameterExpression variable(Class type) {
        throw Extensions.todo();
    }

    /** Creates a ParameterExpression node that can be used to
     * identify a parameter or a variable in an expression tree. */
    public static ParameterExpression variable(Class type, String name) {
        return new ParameterExpression(type, name);
    }

    /** Reduces the node and then calls the visitor delegate on the
     * reduced expression. The method throws an exception if the node
     * is not reducible.*/
    public static Expression VisitChildren(ExpressionVisitor visitor) {
        throw Extensions.todo();
    }

    // Some interfaces we'd rather not implement yet. They don't seem relevant
    // in the Java world.

    interface PropertyInfo {}
    interface RuntimeVariablesExpression {}
    interface SymbolDocumentInfo {}
}

// End Expressions.java
