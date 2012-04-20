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

/**
 * <p>Analogous to LINQ's System.Linq.Expressions.ExpressionType.</p>
 */
public enum ExpressionType {

    /** An addition operation, such as a + b, without overflow
     * checking, for numeric operands. */
    Add,

    /** An addition operation, such as (a + b), with overflow
     * checking, for numeric operands. */
    AddChecked,

    /** A bitwise or logical AND operation, such as (a & b) in C# and
     * (a And b) in Visual Basic. */
    And,

    /** A conditional AND operation that evaluates the second operand
     * only if the first operand evaluates to true. It corresponds to
     * (a && b) in C# and (a AndAlso b) in Visual Basic. */
    AndAlso,

    /** An operation that obtains the length of a one-dimensional
     * array, such as array.Length. */
    ArrayLength,

    /** An indexing operation in a one-dimensional array, such as
     * array[index] in C# or array(index) in Visual Basic. */
    ArrayIndex,

    /** A method call, such as in the obj.sampleMethod()
     * expression. */
    Call,

    /** A node that represents a null coalescing operation, such
     * as (a ?? b) in C# or If(a, b) in Visual Basic. */
    Coalesce,

    /** A conditional operation, such as a > b ? a : b in C# or
     * If(a > b, a, b) in Visual Basic. */
    Conditional,

    /** A constant value. */
    Constant,

    /** A cast or conversion operation, such as (SampleType)obj in
     * C#or CType(obj, SampleType) in Visual Basic. For a numeric
     * conversion, if the converted value is too large for the
     * destination type, no exception is thrown. */
    Convert,

    /** A cast or conversion operation, such as (SampleType)obj in
     * C#or CType(obj, SampleType) in Visual Basic. For a numeric
     * conversion, if the converted value does not fit the
     * destination type, an exception is thrown. */
    ConvertChecked,

    /** A division operation, such as (a / b), for numeric
     * operands. */
    Divide,

    /** A node that represents an equality comparison, such as (a
     * == b) in C# or (a = b) in Visual Basic. */
    Equal,

    /** A bitwise or logical XOR operation, such as (a ^ b) in C#
     * or (a Xor b) in Visual Basic. */
    ExclusiveOr,

    /** A "greater than" comparison, such as (a > b). */
    GreaterThan,

    /** A "greater than or equal to" comparison, such as (a >=
     * b). */
    GreaterThanOrEqual,

    /** An operation that invokes a delegate or lambda expression,
     * such as sampleDelegate.Invoke(). */
    Invoke,

    /** A lambda expression, such as a => a + a in C# or
     * Function(a) a + a in Visual Basic. */
    Lambda,

    /** A bitwise left-shift operation, such as (a << b). */
    LeftShift,

    /** A "less than" comparison, such as (a < b). */
    LessThan,

    /** A "less than or equal to" comparison, such as (a <= b). */
    LessThanOrEqual,

    /** An operation that creates a new IEnumerable object and
     * initializes it from a list of elements, such as new
     * List<SampleType>(){ a, b, c } in C# or Dim sampleList = {
     * a, b, c } in Visual Basic. */
    ListInit,

    /** An operation that reads from a field or property, such as
     * obj.SampleProperty. */
    MemberAccess,

    /** An operation that creates a new object and initializes one
     * or more of its members, such as new Point { X = 1, Y = 2 }
     * in C# or New Point With {.X = 1, .Y = 2} in Visual
     * Basic. */
    MemberInit,

    /** An arithmetic remainder operation, such as (a % b) in C#
     * or (a Mod b) in Visual Basic. */
    Modulo,

    /** A multiplication operation, such as (a * b), without
     * overflow checking, for numeric operands. */
    Multiply,

    /** An multiplication operation, such as (a * b), that has
     * overflow checking, for numeric operands. */
    MultiplyChecked,

    /** An arithmetic negation operation, such as (-a). The object
     * a should not be modified in place. */
    Negate,

    /** A unary plus operation, such as (+a). The result of a
     * predefined unary plus operation is the value of the
     * operand, but user-defined implementations might have
     * unusual results. */
    UnaryPlus,

    /** An arithmetic negation operation, such as (-a), that has
     * overflow checking. The object a should not be modified in
     * place. */
    NegateChecked,

    /** An operation that calls a constructor to create a new
     * object, such as new SampleType(). */
    New,

    /** An operation that creates a new one-dimensional array and
     * initializes it from a list of elements, such as new
     * SampleType[]{a, b, c} in C# or New SampleType(){a, b, c} in
     * Visual Basic. */
    NewArrayInit,

    /** An operation that creates a new array, in which the bounds
     * for each dimension are specified, such as new
     * SampleType[dim1, dim2] in C# or New SampleType(dim1, dim2)
     * in Visual Basic. */
    NewArrayBounds,

    /** A bitwise complement or logical negation operation. In C#,
     * it is equivalent to (~a) for integral types and to (!a) for
     * Boolean values. In Visual Basic, it is equivalent to (Not
     * a). The object a should not be modified in place. */
    Not,

    /** An inequality comparison, such as (a != b) in C# or (a <>
     * b) in Visual Basic. */
    NotEqual,

    /** A bitwise or logical OR operation, such as (a | b) in C#
     * or (a Or b) in Visual Basic. */
    Or,

    /** A short-circuiting conditional OR operation, such as (a ||
     * b) in C# or (a OrElse b) in Visual Basic. */
    OrElse,

    /** A reference to a parameter or variable that is defined in
     * the context of the expression. For more information, see
     * ParameterExpression. */
    Parameter,

    /** A mathematical operation that raises a number to a power,
     * such as (a ^ b) in Visual Basic. */
    Power,

    /** An expression that has a constant value of type
     * Expression. A Quote node can contain references to
     * parameters that are defined in the context of the
     * expression it represents. */
    Quote,

    /** A bitwise right-shift operation, such as (a >> b). */
    RightShift,

    /** A subtraction operation, such as (a - b), without overflow
     * checking, for numeric operands. */
    Subtract,

    /** An arithmetic subtraction operation, such as (a - b), that
     * has overflow checking, for numeric operands. */
    SubtractChecked,

    /** An explicit reference or boxing conversion in which null
     * is supplied if the conversion fails, such as (obj as
     * SampleType) in C# or TryCast(obj, SampleType) in Visual
     * Basic. */
    TypeAs,

    /** A type test, such as obj is SampleType in C# or TypeOf obj
     * is SampleType in Visual Basic. */
    TypeIs,

    /** An assignment operation, such as (a = b). */
    Assign,

    /** A block of expressions. */
    Block,

    /** Debugging information. */
    DebugInfo,

    /** A unary decrement operation, such as (a - 1) in C# and
     * Visual Basic. The object a should not be modified in
     * place. */
    Decrement,

    /** A dynamic operation. */
    Dynamic,

    /** A default value. */
    Default,

    /** An extension expression. */
    Extension,

    /** A "go to" expression, such as goto Label in C# or GoTo
     * Label in Visual Basic. */
    Goto,

    /** A unary increment operation, such as (a + 1) in C# and
     * Visual Basic. The object a should not be modified in
     * place. */
    Increment,

    /** An index operation or an operation that accesses a
     * property that takes arguments. */
    Index,

    /** A label. */
    Label,

    /** A list of run-time variables. For more information, see
     * RuntimeVariablesExpression. */
    RuntimeVariables,

    /** A loop, such as for or while. */
    Loop,

    /** A switch operation, such as switch in C# or Select Case in
     * Visual Basic. */
    Switch,

    /** An operation that throws an exception, such as throw new
     * Exception(). */
    Throw,

    /** A try-catch expression. */
    Try,

    /** An unbox value type operation, such as unbox and unbox.any
     * instructions in MSIL. */
    Unbox,

    /** An addition compound assignment operation, such as (a +=
     * b), without overflow checking, for numeric operands. */
    AddAssign,

    /** A bitwise or logical AND compound assignment operation,
     * such as (a &= b) in C#. */
    AndAssign,

    /** An division compound assignment operation, such as (a /=
     * b), for numeric operands. */
    DivideAssign,

    /** A bitwise or logical XOR compound assignment operation,
     * such as (a ^= b) in C#. */
    ExclusiveOrAssign,

    /** A bitwise left-shift compound assignment, such as (a <<=
     * b). */
    LeftShiftAssign,

    /** An arithmetic remainder compound assignment operation,
     * such as (a %= b) in C#. */
    ModuloAssign,

    /** A multiplication compound assignment operation, such as (a
     * *= b), without overflow checking, for numeric operands. */
    MultiplyAssign,

    /** A bitwise or logical OR compound assignment, such as (a |=
     * b) in C#. */
    OrAssign,

    /** A compound assignment operation that raises a number to a
     * power, such as (a ^= b) in Visual Basic. */
    PowerAssign,

    /** A bitwise right-shift compound assignment operation, such
     * as (a >>= b). */
    RightShiftAssign,

    /** A subtraction compound assignment operation, such as (a -=
     * b), without overflow checking, for numeric operands. */
    SubtractAssign,

    /** An addition compound assignment operation, such as (a +=
     * b), with overflow checking, for numeric operands. */
    AddAssignChecked,

    /** A multiplication compound assignment operation, such as (a
     * *= b), that has overflow checking, for numeric operands. */
    MultiplyAssignChecked,

    /** A subtraction compound assignment operation, such as (a -=
     * b), that has overflow checking, for numeric operands. */
    SubtractAssignChecked,

    /** A unary prefix increment, such as (++a). The object a
     * should be modified in place. */
    PreIncrementAssign,

    /** A unary prefix decrement, such as (--a). The object a
     * should be modified in place. */
    PreDecrementAssign,

    /** A unary postfix increment, such as (a++). The object a
     * should be modified in place. */
    PostIncrementAssign,

    /** A unary postfix decrement, such as (a--). The object a
     * should be modified in place. */
    PostDecrementAssign,

    /** An exact type test. */
    TypeEqual,

    /** A ones complement operation, such as (~a) in C#. */
    OnesComplement,

    /** A true condition value. */
    IsTrue,

    /** A false condition value. */
    IsFalse,

}
