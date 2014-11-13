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

import net.hydromatic.linq4j.function.Deterministic;
import net.hydromatic.linq4j.function.NonDeterministic;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Factors out deterministic expressions to final static fields.
 * Instances of this class should not be reused, so new visitor should be
 * created for optimizing a new expression tree.
 */
public class DeterministicCodeOptimizer extends ClassDeclarationFinder {
  /**
   * The map contains known to be effectively-final expression.
   * The map uses identity equality.
   * Typically the key is {@code ParameterExpression}, however there might be
   * non-factored to final field expression that is known to be constant.
   * For instance, cast expression will not be factored to a field,
   * but we still need to track its constant status.
   */
  protected final Map<Expression, Boolean> constants =
      new IdentityHashMap<Expression, Boolean>();

  /**
   * The map that de-duplicates expressions, so the same expressions may reuse
   * the same final static fields.
   */
  protected final Map<Expression, ParameterExpression> dedup =
      new HashMap<Expression, ParameterExpression>();

  /**
   * The map of all the added final static fields. Allows to identify if the
   * name is occupied or not.
   */
  protected final Map<String, ParameterExpression> fieldsByName =
      new HashMap<String, ParameterExpression>();

  // Pre-compiled patterns for generation names for the final static fields
  private static final Pattern NON_ASCII = Pattern.compile("[^0-9a-zA-Z$]+");
  private static final String FIELD_PREFIX = "$L4J$C$";
  private static final Pattern PREFIX_PATTERN =
      Pattern.compile(Pattern.quote(FIELD_PREFIX));

  private static final Set<Class> DETERMINISTIC_CLASSES =
      new HashSet<Class>(
          Arrays.<Class>asList(Byte.class, Boolean.class, Short.class,
              Integer.class, Long.class, BigInteger.class, BigDecimal.class,
              String.class, Math.class));

  /**
   * Creates a child optimizer.
   * Typically a child is created for each class declaration,
   * so each optimizer collects fields for exactly one class.
   *
   * @param parent parent optimizer
   */
  public DeterministicCodeOptimizer(ClassDeclarationFinder parent) {
    super(parent);
  }

  /**
   * Optimizes {@code new Type()} constructs,
   *
   * @param newExpression expression to optimize
   * @return optimized expression
   */
  @Override
  protected Expression tryOptimizeNewInstance(NewExpression newExpression) {
    if (newExpression.type instanceof Class
        && isConstant(newExpression.arguments)
        && isConstructorDeterministic(newExpression)) {
      // Reuse instance creation when class is immutable: new BigInteger(3)
      return createField(newExpression);
    }
    return newExpression;
  }

  @Override
  public Expression visit(BinaryExpression binaryExpression,
      Expression expression0, Expression expression1) {
    Expression result = super.visit(binaryExpression, expression0, expression1);
    if (binaryExpression.getNodeType().modifiesLvalue) {
      return result;
    }

    if (isConstant(expression0) && isConstant(expression1)) {
      return createField(result);
    }
    return result;
  }

  @Override
  public Expression visit(TernaryExpression ternaryExpression,
      Expression expression0, Expression expression1, Expression expression2) {
    Expression result =
        super.visit(ternaryExpression, expression0, expression1, expression2);

    if (isConstant(expression0)
        && isConstant(expression1)
        && isConstant(expression2)) {
      return createField(result);
    }
    return result;
  }

  @Override
  public Expression visit(UnaryExpression unaryExpression,
      Expression expression) {
    Expression result = super.visit(unaryExpression, expression);

    if (isConstant(expression)) {
      constants.put(result, true);
      if (result.getNodeType() != ExpressionType.Convert) {
        return createField(result);
      }
    }
    return result;
  }

  @Override
  public Expression visit(TypeBinaryExpression typeBinaryExpression,
      Expression expression) {
    Expression result = super.visit(typeBinaryExpression, expression);

    if (isConstant(expression)) {
      constants.put(result, true);
    }
    return result;
  }

  /**
   * Optimized method call, possibly converting it to final static field.
   *
   * @param methodCallExpression method call to optimize
   * @return optimized expression
   */
  protected Expression tryOptimizeMethodCall(MethodCallExpression
      methodCallExpression) {
    if (isConstant(methodCallExpression.targetExpression)
        && isConstant(methodCallExpression.expressions)
        && isMethodDeterministic(methodCallExpression.method)) {
      return createField(methodCallExpression);
    }
    return methodCallExpression;
  }

  @Override
  public Expression visit(MethodCallExpression methodCallExpression,
      Expression targetExpression, List<Expression> expressions) {
    Expression result =
        super.visit(methodCallExpression, targetExpression, expressions);

    result = tryOptimizeMethodCall((MethodCallExpression) result);
    return result;
  }

  @Override
  public Expression visit(MemberExpression memberExpression,
      Expression expression) {
    Expression result = super.visit(memberExpression, expression);

    if (isConstant(expression)
        && Modifier.isFinal(memberExpression.field.getModifiers())) {
      constants.put(result, true);
    }
    return result;
  }

  @Override
  public MemberDeclaration visit(FieldDeclaration fieldDeclaration,
      Expression initializer) {
    if (Modifier.isStatic(fieldDeclaration.modifier)) {
      // Avoid optimization of static fields, since we'll have to track order
      // of static declarations.
      return fieldDeclaration;
    }
    return super.visit(fieldDeclaration, initializer);
  }

  /**
   * Processes the list of declarations and learns final static ones as
   * effectively constant.
   *
   * @param memberDeclarations list of declarations to search finals from
   */
  @Override
  protected void learnFinalStaticDeclarations(
      List<MemberDeclaration> memberDeclarations) {
    for (MemberDeclaration decl : memberDeclarations) {
      if (decl instanceof FieldDeclaration) {
        FieldDeclaration field = (FieldDeclaration) decl;
        if (Modifier.isStatic(field.modifier)
            && Modifier.isFinal(field.modifier)
            && field.initializer != null) {
          constants.put(field.parameter, true);
          fieldsByName.put(field.parameter.name, field.parameter);
          dedup.put(field.initializer, field.parameter);
        }
      }
    }
  }

  /**
   * Finds if there exists ready for reuse declaration for given expression.
   *
   * @param expression input expression
   * @return parameter of the already existing declaration, or null
   */
  protected ParameterExpression findDeclaredExpression(Expression expression) {
    if (!dedup.isEmpty()) {
      ParameterExpression pe = dedup.get(expression);
      if (pe != null) {
        return pe;
      }
    }
    return parent == null ? null : parent.findDeclaredExpression(expression);
  }

  /**
   * Creates final static field to hold the given expression.
   * The method might reuse existing declarations if appropriate.
   *
   * @param expression expression to store in final field
   * @return expression for the given input expression
   */
  protected Expression createField(Expression expression) {
    ParameterExpression pe = findDeclaredExpression(expression);
    if (pe != null) {
      return pe;
    }

    String name = inventFieldName(expression);
    pe = Expressions.parameter(expression.getType(), name);
    FieldDeclaration decl =
        Expressions.fieldDecl(Modifier.FINAL | Modifier.STATIC, pe, expression);
    dedup.put(expression, pe);
    addedDeclarations.add(decl);
    constants.put(pe, true);
    fieldsByName.put(name, pe);
    return pe;
  }

  /**
   * Generates field name to store given expression.
   * The expression is converted to string and all the non-ascii/numeric
   * characters are replaced with underscores and {@code "_$L4J$C$"} suffix is
   * added to avoid conflicts with other variables.
   * When multiple variables are mangled to the same name,
   * counter is used to avoid conflicts.
   *
   * @param expression input expression
   * @return unique name to store given expression
   */
  protected String inventFieldName(Expression expression) {
    String exprText = expression.toString();
    exprText = PREFIX_PATTERN.matcher(exprText).replaceAll("");
    exprText = FIELD_PREFIX + NON_ASCII.matcher(exprText).replaceAll("_");
    if (exprText.length() > 70) {
      exprText = exprText.substring(0, 70)
          + Integer.toHexString(exprText.hashCode());
    }
    String fieldName = exprText;
    for (int i = 0; hasField(fieldName); i++) {
      fieldName = exprText + i;
    }
    return fieldName;
  }

  /**
   * Verifies if the expression is effectively constant.
   * It is assumed the expression is simple (e.g. {@code ConstantExpression} or
   * {@code ParameterExpression}).
   * The method verifies parent chain since the expression might be defined
   * in enclosing class.
   *
   * @param expression expression to test
   * @return true when the expression is known to be constant
   */
  @Override
  protected boolean isConstant(Expression expression) {
    return expression == null
        || expression instanceof ConstantExpression
        || !constants.isEmpty() && constants.containsKey(expression)
        || parent != null && parent.isConstant(expression);
  }

  /**
   * Checks if given method is deterministic (i.e. returns the same output
   * given the same inputs).
   *
   * @param method method to test
   * @return true when the method is deterministic
   */
  protected boolean isMethodDeterministic(Method method) {
    return (allMethodsDeterministic(method.getDeclaringClass())
            && !method.isAnnotationPresent(NonDeterministic.class))
           || method.isAnnotationPresent(Deterministic.class);
  }

  /**
   * Checks if new instance creation can be reused. For instance {@code new
   * BigInteger("42")} is effectively final and can be reused.
   *
   *
   * @param newExpression method to test
   * @return true when the method is deterministic
   */
  protected boolean isConstructorDeterministic(NewExpression newExpression) {
    return allMethodsDeterministic((Class) newExpression.type);
  }

  /**
   * Checks if all the methods in given class are deterministic (i.e. return
   * the same value given the same inputs)
   *
   * @param klass class to test
   * @return true when all the methods including constructors are deterministic
   */
  protected boolean allMethodsDeterministic(Class klass) {
    return DETERMINISTIC_CLASSES.contains(klass)
           || klass.isAnnotationPresent(Deterministic.class);
  }

  /**
   * Verifies if the variable name is already in use.
   * Only the variables that are explicitly added to {@code fieldsByName} are
   * verified. The method verifies parent chain.
   *
   * @param name name of the variable to test
   * @return true if the name is used by one of static final fields
   */
  @Override
  protected boolean hasField(String name) {
    return !fieldsByName.isEmpty() && fieldsByName.containsKey(name)
        || parent != null && parent.hasField(name);
  }

  /**
   * Creates child visitor. It is used to traverse nested class declarations.
   *
   * @return new Visitor that is used to optimize class declarations
   */
  protected DeterministicCodeOptimizer goDeeper() {
    return new DeterministicCodeOptimizer(this);
  }
}

// End DeterministicCodeOptimizer.java
