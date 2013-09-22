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

  // Operator precedence and associativity is as follows.
  //
  //  Priority Operators  Operation
  //  ======== ========== ========================================
  //  1 left   [ ]        array index
  //           ()         method call
  //           .          member access
  //  2 right  ++         pre- or postfix increment
  //           --         pre- or postfix decrement
  //           + -        unary plus, minus
  //           ~          bitwise NOT
  //           !          boolean (logical) NOT
  //           (type)     type cast
  //           new        object creation
  //  3 left   * / %      multiplication, division, remainder
  //  4 left   + -        addition, subtraction
  //           +          string concatenation
  //  5 left   <<         signed bit shift left
  //           >>         signed bit shift right
  //           >>>        unsigned bit shift right
  //  6 left   < <=       less than, less than or equal to
  //           > >=       greater than, greater than or equal to
  //           instanceof reference test
  //  7 left   ==         equal to
  //           !=         not equal to
  //  8 left   &          bitwise AND
  //           &          boolean (logical) AND
  //  9 left   ^          bitwise XOR
  //           ^          boolean (logical) XOR
  //  10 left  |          bitwise OR
  //           |          boolean (logical) OR
  //  11 left  &&         boolean (logical) AND
  //  12 left  ||         boolean (logical) OR
  //  13 right ? :        conditional right
  //  14 right =          assignment
  //           *= /= += -= %=
  //           <<= >>= >>>=
  //           &= ^= |=   combined assignment

  /**
   * An addition operation, such as a + b, without overflow
   * checking, for numeric operands.
   */
  Add(" + ", false, 4, false),

  /**
   * An addition operation, such as (a + b), with overflow
   * checking, for numeric operands.
   */
  AddChecked(" + ", false, 4, false),

  /**
   * A bitwise or logical AND operation, such as (a &amp; b) in C# and
   * (a And b) in Visual Basic.
   */
  And(" & ", false, 8, false),

  /**
   * A conditional AND operation that evaluates the second operand
   * only if the first operand evaluates to true. It corresponds to
   * (a && b) in C# and (a AndAlso b) in Visual Basic.
   */
  AndAlso(" && ", false, 11, false),

  /**
   * An operation that obtains the length of a one-dimensional
   * array, such as array.Length.
   */
  ArrayLength,

  /**
   * An indexing operation in a one-dimensional array, such as
   * array[index] in C# or array(index) in Visual Basic.
   */
  ArrayIndex,

  /**
   * A method call, such as in the obj.sampleMethod()
   * expression.
   */
  Call(".", false, 1, false),

  /**
   * A node that represents a null coalescing operation, such
   * as (a ?? b) in C# or If(a, b) in Visual Basic.
   */
  Coalesce,

  /**
   * A conditional operation, such as a > b ? a : b in C# or
   * If(a > b, a, b) in Visual Basic.
   */
  Conditional(" ? ", " : ", false, 13, true),

  /**
   * A constant value.
   */
  Constant,

  /**
   * A cast or conversion operation, such as (SampleType)obj in
   * C#or CType(obj, SampleType) in Visual Basic. For a numeric
   * conversion, if the converted value is too large for the
   * destination type, no exception is thrown.
   */
  Convert(null, false, 2, true),

  /**
   * A cast or conversion operation, such as (SampleType)obj in
   * C#or CType(obj, SampleType) in Visual Basic. For a numeric
   * conversion, if the converted value does not fit the
   * destination type, an exception is thrown.
   */
  ConvertChecked,

  /**
   * A division operation, such as (a / b), for numeric
   * operands.
   */
  Divide(" / ", false, 3, false),

  /**
   * A node that represents an equality comparison, such as (a
   * == b) in C# or (a = b) in Visual Basic.
   */
  Equal(" == ", false, 7, false),

  /**
   * A bitwise or logical XOR operation, such as (a ^ b) in C#
   * or (a Xor b) in Visual Basic.
   */
  ExclusiveOr(" ^ ", false, 9, false),

  /**
   * A "greater than" comparison, such as (a &gt; b).
   */
  GreaterThan(" > ", false, 6, false),

  /**
   * A "greater than or equal to" comparison, such as (a &gt;=
   * b).
   */
  GreaterThanOrEqual(" >= ", false, 6, false),

  /**
   * An operation that invokes a delegate or lambda expression,
   * such as sampleDelegate.Invoke().
   */
  Invoke,

  /**
   * A lambda expression, such as a =&gt; a + a in C# or
   * Function(a) a + a in Visual Basic.
   */
  Lambda,

  /**
   * A bitwise left-shift operation, such as (a &lt;&lt; b).
   */
  LeftShift(" << ", false, 5, false),

  /**
   * A "less than" comparison, such as (a &lt; b).
   */
  LessThan(" < ", false, 6, false),

  /**
   * A "less than or equal to" comparison, such as (a &lt;= b).
   */
  LessThanOrEqual(" <= ", false, 6, false),

  /**
   * An operation that creates a new IEnumerable object and
   * initializes it from a list of elements, such as new
   * List&lt;SampleType&gt;(){ a, b, c } in C# or Dim sampleList = {
   * a, b, c } in Visual Basic.
   */
  ListInit,

  /**
   * An operation that reads from a field or property, such as
   * obj.SampleProperty.
   */
  MemberAccess(".", false, 1, false),

  /**
   * An operation that creates a new object and initializes one
   * or more of its members, such as new Point { X = 1, Y = 2 }
   * in C# or New Point With {.X = 1, .Y = 2} in Visual
   * Basic.
   */
  MemberInit,

  /**
   * An arithmetic remainder operation, such as (a % b) in C#
   * or (a Mod b) in Visual Basic.
   */
  Modulo(" % ", false, 3, false),

  /**
   * A multiplication operation, such as (a * b), without
   * overflow checking, for numeric operands.
   */
  Multiply(" * ", false, 3, false),

  /**
   * An multiplication operation, such as (a * b), that has
   * overflow checking, for numeric operands.
   */
  MultiplyChecked(" * ", false, 3, false),

  /**
   * An arithmetic negation operation, such as (-a). The object
   * a should not be modified in place.
   */
  Negate("- ", false, 2, true),

  /**
   * A unary plus operation, such as (+a). The result of a
   * predefined unary plus operation is the value of the
   * operand, but user-defined implementations might have
   * unusual results.
   */
  UnaryPlus("+ ", false, 2, true),

  /**
   * An arithmetic negation operation, such as (-a), that has
   * overflow checking. The object a should not be modified in
   * place.
   */
  NegateChecked("-", false, 2, true),

  /**
   * An operation that calls a constructor to create a new
   * object, such as new SampleType().
   */
  New,

  /**
   * An operation that creates a new one-dimensional array and
   * initializes it from a list of elements, such as new
   * SampleType[]{a, b, c} in C# or New SampleType(){a, b, c} in
   * Visual Basic.
   */
  NewArrayInit,

  /**
   * An operation that creates a new array, in which the bounds
   * for each dimension are specified, such as new
   * SampleType[dim1, dim2] in C# or New SampleType(dim1, dim2)
   * in Visual Basic.
   */
  NewArrayBounds,

  /**
   * A bitwise complement or logical negation operation. In C#,
   * it is equivalent to (~a) for integral types and to (!a) for
   * Boolean values. In Visual Basic, it is equivalent to (Not
   * a). The object a should not be modified in place.
   */
  Not("!", false, 2, true),

  /**
   * An inequality comparison, such as (a != b) in C# or (a &lt;&gt;
   * b) in Visual Basic.
   */
  NotEqual(" != ", false, 7, false),

  /**
   * A bitwise or logical OR operation, such as (a | b) in C#
   * or (a Or b) in Visual Basic.
   */
  Or(" | ", false, 10, false),

  /**
   * A short-circuiting conditional OR operation, such as (a ||
   * b) in C# or (a OrElse b) in Visual Basic.
   */
  OrElse(" || ", false, 12, false),

  /**
   * A reference to a parameter or variable that is defined in
   * the context of the expression. For more information, see
   * ParameterExpression.
   */
  Parameter,

  /**
   * A mathematical operation that raises a number to a power,
   * such as (a ^ b) in Visual Basic.
   */
  Power,

  /**
   * An expression that has a constant value of type
   * Expression. A Quote node can contain references to
   * parameters that are defined in the context of the
   * expression it represents.
   */
  Quote,

  /**
   * A bitwise right-shift operation, such as (a &gt;*gt; b).
   */
  RightShift(" >> ", false, 5, false),

  /**
   * A subtraction operation, such as (a - b), without overflow
   * checking, for numeric operands.
   */
  Subtract(" - ", false, 4, false),

  /**
   * An arithmetic subtraction operation, such as (a - b), that
   * has overflow checking, for numeric operands.
   */
  SubtractChecked(" - ", false, 4, false),

  /**
   * An explicit reference or boxing conversion in which null
   * is supplied if the conversion fails, such as (obj as
   * SampleType) in C# or TryCast(obj, SampleType) in Visual
   * Basic.
   */
  TypeAs,

  /**
   * A type test, such as obj is SampleType in C# or TypeOf obj
   * is SampleType in Visual Basic.
   */
  TypeIs(" instanceof ", false, 6, false),

  /**
   * An assignment operation, such as (a = b).
   */
  Assign(" = ", false, 14, true),

  /**
   * A block of expressions.
   */
  Block,

  /**
   * Debugging information.
   */
  DebugInfo,

  /**
   * A unary decrement operation, such as (a - 1) in C# and
   * Visual Basic. The object a should not be modified in
   * place.
   */
  Decrement,

  /**
   * A dynamic operation.
   */
  Dynamic,

  /**
   * A default value.
   */
  Default,

  /**
   * An extension expression.
   */
  Extension,

  /**
   * A "go to" expression, such as goto Label in C# or GoTo
   * Label in Visual Basic.
   */
  Goto,

  /**
   * A unary increment operation, such as (a + 1) in C# and
   * Visual Basic. The object a should not be modified in
   * place.
   */
  Increment,

  /**
   * An index operation or an operation that accesses a
   * property that takes arguments.
   */
  Index,

  /**
   * A label.
   */
  Label,

  /**
   * A list of run-time variables. For more information, see
   * RuntimeVariablesExpression.
   */
  RuntimeVariables,

  /**
   * A loop, such as for or while.
   */
  Loop,

  /**
   * A switch operation, such as switch in C# or Select Case in
   * Visual Basic.
   */
  Switch,

  /**
   * An operation that throws an exception, such as throw new
   * Exception().
   */
  Throw,

  /**
   * A try-catch expression.
   */
  Try,

  /**
   * An unbox value type operation, such as unbox and unbox.any
   * instructions in MSIL.
   */
  Unbox,

  /**
   * An addition compound assignment operation, such as (a +=
   * b), without overflow checking, for numeric operands.
   */
  AddAssign(" += ", false, 14, true),

  /**
   * A bitwise or logical AND compound assignment operation,
   * such as (a &amp;= b) in C#.
   */
  AndAssign(" &= ", false, 14, true),

  /**
   * An division compound assignment operation, such as (a /=
   * b), for numeric operands.
   */
  DivideAssign(" /= ", false, 14, true),

  /**
   * A bitwise or logical XOR compound assignment operation,
   * such as (a ^= b) in C#.
   */
  ExclusiveOrAssign(" ^= ", false, 14, true),

  /**
   * A bitwise left-shift compound assignment, such as (a &lt;&lt;=
   * b).
   */
  LeftShiftAssign(" <<= ", false, 14, true),

  /**
   * An arithmetic remainder compound assignment operation,
   * such as (a %= b) in C#.
   */
  ModuloAssign(" %= ", false, 14, true),

  /**
   * A multiplication compound assignment operation, such as (a
   * *= b), without overflow checking, for numeric operands.
   */
  MultiplyAssign(" *= ", false, 14, true),

  /**
   * A bitwise or logical OR compound assignment, such as (a |=
   * b) in C#.
   */
  OrAssign(" |= ", false, 14, true),

  /**
   * A compound assignment operation that raises a number to a
   * power, such as (a ^= b) in Visual Basic.
   */
  PowerAssign,

  /**
   * A bitwise right-shift compound assignment operation, such
   * as (a &gt;&gt;= b).
   */
  RightShiftAssign(" >>= ", false, 14, true),

  /**
   * A subtraction compound assignment operation, such as (a -=
   * b), without overflow checking, for numeric operands.
   */
  SubtractAssign(" -= ", false, 14, true),

  /**
   * An addition compound assignment operation, such as (a +=
   * b), with overflow checking, for numeric operands.
   */
  AddAssignChecked(" += ", false, 14, true),

  /**
   * A multiplication compound assignment operation, such as (a
   * *= b), that has overflow checking, for numeric operands.
   */
  MultiplyAssignChecked(" *= ", false, 14, true),

  /**
   * A subtraction compound assignment operation, such as (a -=
   * b), that has overflow checking, for numeric operands.
   */
  SubtractAssignChecked(" -= ", false, 14, true),

  /**
   * A unary prefix increment, such as (++a). The object a
   * should be modified in place.
   */
  PreIncrementAssign("++", false, 2, true),

  /**
   * A unary prefix decrement, such as (--a). The object a
   * should be modified in place.
   */
  PreDecrementAssign("--", false, 2, true),

  /**
   * A unary postfix increment, such as (a++). The object a
   * should be modified in place.
   */
  PostIncrementAssign("++", true, 2, true),

  /**
   * A unary postfix decrement, such as (a--). The object a
   * should be modified in place.
   */
  PostDecrementAssign("--", true, 2, true),

  /**
   * An exact type test.
   */
  TypeEqual,

  /**
   * A ones complement operation, such as (~a) in C#.
   */
  OnesComplement("~", false, 2, true),

  /**
   * A true condition value.
   */
  IsTrue,

  /**
   * A false condition value.
   */
  IsFalse,

  /**
   * Declaration of a variable.
   */
  Declaration,

  For,

  While;

  final String op;
  final String op2;
  final boolean postfix;
  final int lprec;
  final int rprec;

  ExpressionType() {
    this(null, false, 0, false);
  }

  ExpressionType(String op, boolean postfix, int prec, boolean right) {
    this(op, null, postfix, prec, right);
  }

  ExpressionType(String op, String op2, boolean postfix, int prec,
      boolean right) {
    this.op = op;
    this.op2 = op2;
    this.postfix = postfix;
    this.lprec = (20 - prec) * 2 + (right ? 1 : 0);
    this.rprec = (20 - prec) * 2 + (right ? 0 : 1);
  }
}

// End ExpressionType.java
