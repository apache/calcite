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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

import static net.hydromatic.linq4j.expressions.ExpressionType.Equal;
import static net.hydromatic.linq4j.expressions.ExpressionType.NotEqual;

/**
 * Visitor that optimizes expressions.
 *
 * <p>The optimizations are essential, not mere tweaks. Without
 * optimization, expressions such as {@code false == null} will be left in,
 * which are invalid to Janino (because it does not automatically box
 * primitives).</p>
 */
public class OptimizeVisitor extends Visitor {
  public static final ConstantExpression FALSE_EXPR =
      Expressions.constant(false);
  public static final ConstantExpression TRUE_EXPR =
      Expressions.constant(true);
  public static final MemberExpression BOXED_FALSE_EXPR =
      Expressions.field(null, Boolean.class, "FALSE");
  public static final MemberExpression BOXED_TRUE_EXPR =
      Expressions.field(null, Boolean.class, "TRUE");
  public static final Statement EMPTY_STATEMENT = Expressions.statement(null);

  private static final Set<Method> KNOWN_NON_NULL_METHODS =
      new HashSet<Method>();

  static {
    for (Class aClass : new Class[]{Boolean.class, Byte.class, Short.class,
      Integer.class, Long.class, String.class}) {
      for (Method method : aClass.getMethods()) {
        if ("valueOf".equals(method.getName())
            && Modifier.isStatic(method.getModifiers())) {
          KNOWN_NON_NULL_METHODS.add(method);
        }
      }
    }
  }

  private static final Map<ExpressionType, ExpressionType>
  NOT_BINARY_COMPLEMENT =
      new EnumMap<ExpressionType, ExpressionType>(ExpressionType.class);

  static {
    addComplement(ExpressionType.Equal, ExpressionType.NotEqual);
    addComplement(ExpressionType.GreaterThanOrEqual, ExpressionType.LessThan);
    addComplement(ExpressionType.GreaterThan, ExpressionType.LessThanOrEqual);
  }

  private static void addComplement(ExpressionType eq, ExpressionType ne) {
    NOT_BINARY_COMPLEMENT.put(eq, ne);
    NOT_BINARY_COMPLEMENT.put(ne, eq);
  }

  private static final Method BOOLEAN_VALUEOF_BOOL =
      Types.lookupMethod(Boolean.class, "valueOf", boolean.class);

  @Override
  public Expression visit(
      TernaryExpression ternary,
      Expression expression0,
      Expression expression1,
      Expression expression2) {
    switch (ternary.getNodeType()) {
    case Conditional:
      Boolean always = always(expression0);
      if (always != null) {
        // true ? y : z  ===  y
        // false ? y : z  === z
        return always
            ? expression1
            : expression2;
      }
      if (expression1.equals(expression2)) {
        // a ? b : b   ===   b
        return expression1;
      }
      // !a ? b : c == a ? c : b
      if (expression0 instanceof UnaryExpression) {
        UnaryExpression una = (UnaryExpression) expression0;
        if (una.getNodeType() == ExpressionType.Not) {
          return Expressions.makeTernary(ternary.getNodeType(),
              una.expression, expression2, expression1);
        }
      }

      // a ? true : b  === a || b
      // a ? false : b === !a && b
      always = always(expression1);
      if (always != null && isKnownNotNull(expression2)) {
        return (always
                 ? Expressions.orElse(expression0, expression2)
                 : Expressions.andAlso(Expressions.not(expression0),
            expression2)).accept(this);
      }

      // a ? b : true  === !a || b
      // a ? b : false === a && b
      always = always(expression2);
      if (always != null && isKnownNotNull(expression1)) {
        return (always
                 ? Expressions.orElse(Expressions.not(expression0),
                    expression1)
                 : Expressions.andAlso(expression0, expression1)).accept(this);
      }

      if (expression0 instanceof BinaryExpression
          && (expression0.getNodeType() == ExpressionType.Equal
              || expression0.getNodeType() == ExpressionType.NotEqual)) {
        BinaryExpression cmp = (BinaryExpression) expression0;
        Expression expr = null;
        if (eq(cmp.expression0, expression2)
            && eq(cmp.expression1, expression1)) {
          // a == b ? b : a === a (hint: if a==b, then a == b ? a : a)
          // a != b ? b : a === b (hint: if a==b, then a != b ? b : b)
          expr = expression0.getNodeType() == ExpressionType.Equal
              ? expression2 : expression1;
        }
        if (eq(cmp.expression0, expression1)
            && eq(cmp.expression1, expression2)) {
          // a == b ? a : b === b (hint: if a==b, then a == b ? b : b)
          // a != b ? a : b === a (hint: if a==b, then a == b ? a : a)
          expr = expression0.getNodeType() == ExpressionType.Equal
              ? expression2 : expression1;
        }
        if (expr != null) {
          return expr;
        }
      }
    }
    return super.visit(ternary, expression0, expression1, expression2);
  }

  @Override
  public Expression visit(
      BinaryExpression binary,
      Expression expression0,
      Expression expression1) {
    //
    Expression result;
    switch (binary.getNodeType()) {
    case AndAlso:
    case OrElse:
      if (eq(expression0, expression1)) {
        return expression0;
      }
    }
    switch (binary.getNodeType()) {
    case Equal:
    case NotEqual:
      if (eq(expression0, expression1)) {
        return binary.getNodeType() == Equal ? TRUE_EXPR : FALSE_EXPR;
      } else if (expression0 instanceof ConstantExpression && expression1
        instanceof ConstantExpression) {
        ConstantExpression c0 = (ConstantExpression) expression0;
        ConstantExpression c1 = (ConstantExpression) expression1;
        if (c0.getType() == c1.getType()
            || !(Primitive.is(c0.getType()) || Primitive.is(c1.getType()))) {
          return binary.getNodeType() == NotEqual ? TRUE_EXPR : FALSE_EXPR;
        }
      }
      if (expression0 instanceof TernaryExpression
          && expression0.getNodeType() == ExpressionType.Conditional) {
        TernaryExpression ternary = (TernaryExpression) expression0;
        Expression expr = null;
        if (eq(ternary.expression1, expression1)) {
          // (a ? b : c) == b === a || c == b
          expr = Expressions.orElse(ternary.expression0,
              Expressions.equal(ternary.expression2, expression1));
        } else if (eq(ternary.expression2, expression1)) {
          // (a ? b : c) == c === !a || b == c
          expr = Expressions.orElse(Expressions.not(ternary.expression0),
              Expressions.equal(ternary.expression1, expression1));
        }
        if (expr != null) {
          if (binary.getNodeType() == ExpressionType.NotEqual) {
            expr = Expressions.not(expr);
          }
          return expr.accept(this);
        }
      }
      // drop down
    case AndAlso:
    case OrElse:
      result = visit0(binary, expression0, expression1);
      if (result != null) {
        return result;
      }
      result = visit0(binary, expression1, expression0);
      if (result != null) {
        return result;
      }
    }
    return super.visit(binary, expression0, expression1);
  }

  private Expression visit0(
      BinaryExpression binary,
      Expression expression0,
      Expression expression1) {
    Boolean always;
    switch (binary.getNodeType()) {
    case AndAlso:
      always = always(expression0);
      if (always != null) {
        return always
            ? expression1
            : FALSE_EXPR;
      }
      break;
    case OrElse:
      always = always(expression0);
      if (always != null) {
        // true or x  --> true
        // false or x --> x
        return always
            ? TRUE_EXPR
            : expression1;
      }
      break;
    case Equal:
      if (isConstantNull(expression1)
          && Primitive.is(expression0.getType())) {
        return FALSE_EXPR;
      }
      // a == true  -> a
      // a == false -> !a
      always = always(expression0);
      if (always != null) {
        return always ? expression1 : Expressions.not(expression1);
      }
      break;
    case NotEqual:
      if (isConstantNull(expression1)
          && Primitive.is(expression0.getType())) {
        return TRUE_EXPR;
      }
      // a != true  -> !a
      // a != false -> a
      always = always(expression0);
      if (always != null) {
        return always ? Expressions.not(expression1) : expression1;
      }
      break;
    }
    return null;
  }

  @Override
  public Expression visit(UnaryExpression unaryExpression,
      Expression expression) {
    switch (unaryExpression.getNodeType()) {
    case Convert:
      if (expression.getType() == unaryExpression.getType()) {
        return expression;
      }
      if (expression instanceof ConstantExpression) {
        return Expressions.constant(((ConstantExpression) expression).value,
            unaryExpression.getType());
      }
      break;
    case Not:
      Boolean always = always(expression);
      if (always != null) {
        return always ? FALSE_EXPR : TRUE_EXPR;
      }
      if (expression instanceof UnaryExpression) {
        UnaryExpression arg = (UnaryExpression) expression;
        if (arg.getNodeType() == ExpressionType.Not) {
          return arg.expression;
        }
      }
      if (expression instanceof BinaryExpression) {
        BinaryExpression bin = (BinaryExpression) expression;
        ExpressionType comp = NOT_BINARY_COMPLEMENT.get(bin.getNodeType());
        if (comp != null) {
          return Expressions.makeBinary(comp, bin.expression0, bin.expression1);
        }
      }
    }
    return super.visit(unaryExpression, expression);
  }

  @Override
  public Statement visit(ConditionalStatement conditionalStatement,
      List<Node> list) {
    // if (false) { <-- remove branch
    // } if (true) { <-- stop here
    // } else if (...)
    // } else {...}
    boolean optimal = true;
    for (int i = 0; i < list.size() - 1 && optimal; i += 2) {
      Boolean always = always((Expression) list.get(i));
      if (always == null) {
        continue;
      }
      if (i == 0 && always) {
        // when the very first test is always true, just return its statement
        return (Statement) list.get(1);
      }
      optimal = false;
    }
    if (optimal) {
      // Nothing to optimize
      return super.visit(conditionalStatement, list);
    }
    List<Node> newList = new ArrayList<Node>(list.size());
    // Iterate over all the tests, except the latest "else"
    for (int i = 0; i < list.size() - 1; i += 2) {
      Expression test = (Expression) list.get(i);
      Node stmt = list.get(i + 1);
      Boolean always = always(test);
      if (always == null) {
        newList.add(test);
        newList.add(stmt);
        continue;
      }
      if (always) {
        // No need to verify other tests
        newList.add(stmt);
        break;
      }
    }
    // We might have dangling "else", however if we have just single item
    // it means we have if (false) else if(false) else if (true) {...} code.
    // Then we just return statement from true branch
    if (list.size() == 1) {
      return (Statement) list.get(0);
    }
    // Add "else" from original list
    if (newList.size() % 2 == 0 && list.size() % 2 == 1) {
      Node elseBlock = list.get(list.size() - 1);
      if (newList.isEmpty()) {
        return (Statement) elseBlock;
      }
      newList.add(elseBlock);
    }
    if (newList.isEmpty()) {
      return EMPTY_STATEMENT;
    }
    return super.visit(conditionalStatement, newList);
  }

  @Override
  public Expression visit(MethodCallExpression methodCallExpression,
      Expression targetExpression,
      List<Expression> expressions) {
    if (BOOLEAN_VALUEOF_BOOL.equals(methodCallExpression.method)) {
      Boolean always = always(expressions.get(0));
      if (always != null) {
        return always ? TRUE_EXPR : FALSE_EXPR;
      }
    }
    return super.visit(methodCallExpression, targetExpression, expressions);
  }

  private boolean isConstantNull(Expression expression) {
    return expression instanceof ConstantExpression
        && ((ConstantExpression) expression).value == null;
  }

  /**
   * Returns whether an expression always evaluates to true or false.
   * Assumes that expression has already been optimized.
   */
  private static Boolean always(Expression x) {
    if (x.equals(FALSE_EXPR) || x.equals(BOXED_FALSE_EXPR)) {
      return Boolean.FALSE;
    }
    if (x.equals(TRUE_EXPR) || x.equals(BOXED_TRUE_EXPR)) {
      return Boolean.TRUE;
    }
    return null;
  }

  /**
   * Returns whether an expression always returns a non-null result.
   * For instance, primitive types cannot contain null values.
   *
   * @param expression expression to test
   * @return true when the expression is known to be not-null
   */
  protected boolean isKnownNotNull(Expression expression) {
    return Primitive.is(expression.getType())
        || always(expression) != null
        || (expression instanceof MethodCallExpression
            && KNOWN_NON_NULL_METHODS.contains(
                ((MethodCallExpression) expression).method));
  }

  /**
   * Treats two expressions equal even if they represent different null types
   */
  private static boolean eq(Expression a, Expression b) {
    return a.equals(b)
        || (a instanceof ConstantExpression
            && b instanceof ConstantExpression
            && ((ConstantExpression) a).value
                == ((ConstantExpression) b).value);
  }
}

// End OptimizeVisitor.java
