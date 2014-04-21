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

import net.hydromatic.linq4j.function.Function1;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for optimizers that factor ou deterministic expressions to
 * final static fields.
 * Instances of this class should not be reused, so new visitor should be
 * created for optimizing a new expression tree.
 */
public class ClassDeclarationFinder extends Visitor {
  protected final ClassDeclarationFinder parent;

  /**
   * The list of new final static fields to be added to the current class.
   */
  protected final List<MemberDeclaration> addedDeclarations =
      new ArrayList<MemberDeclaration>();

  private final Function1<ClassDeclarationFinder, ClassDeclarationFinder>
  childFactory;

  private static final Function1<ClassDeclarationFinder,
      ClassDeclarationFinder> DEFAULT_CHILD_FACTORY =
      new Function1<ClassDeclarationFinder, ClassDeclarationFinder>() {
      public ClassDeclarationFinder apply(ClassDeclarationFinder a0) {
        return new DeterministicCodeOptimizer(a0);
      }
    };

  /**
   * Creates visitor that uses default optimizer.
   *
   * @return optimizing visitor
   */
  public static ClassDeclarationFinder create() {
    return create(DEFAULT_CHILD_FACTORY);
  }

  /**
   * Creates visitor that uses given class as optimizer.
   * The implementation should support ({@code ClassDeclarationFinder})
   * constructor.
   *
   * @param optimizingClass class that implements optimizations
   * @return optimizing visitor
   */
  public static ClassDeclarationFinder create(
      final Class<? extends ClassDeclarationFinder> optimizingClass) {
    return create(newChildCreator(optimizingClass));
  }

  /**
   * Creates visitor that uses given factory to create optimizers.
   *
   * @param childFactory factory that creates optimizers
   * @return optimizing visitor
   */
  public static ClassDeclarationFinder create(
      Function1<ClassDeclarationFinder, ClassDeclarationFinder> childFactory) {
    return new ClassDeclarationFinder(childFactory);
  }

  /**
   * Creates factory that creates instances of optimizing visitors.
   * The implementation should support ({@code ClassDeclarationFinder})
   * constructor.
   *
   * @param optimizingClass class that implements optimizations
   * @return factory that creates instances of given classes
   */
  private static Function1<ClassDeclarationFinder, ClassDeclarationFinder>
  newChildCreator(Class<? extends ClassDeclarationFinder> optimizingClass) {
    try {
      final Constructor<? extends ClassDeclarationFinder> constructor =
          optimizingClass.getConstructor(ClassDeclarationFinder.class);
      return new Function1<ClassDeclarationFinder, ClassDeclarationFinder>() {
        public ClassDeclarationFinder apply(ClassDeclarationFinder a0) {
          try {
            return constructor.newInstance(a0);
          } catch (InstantiationException e) {
            throw new IllegalStateException(
                "Unable to create optimizer via " + constructor, e);
          } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                "Unable to create optimizer via " + constructor, e);
          } catch (InvocationTargetException e) {
            throw new IllegalStateException(
                "Unable to create optimizer via " + constructor, e);
          }
        }
      };
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Given class " + optimizingClass
          + "does not support (ClassDeclarationFinder) constructor", e);
    }
  }

  /**
   * Creates optimizer with no parent.
   */
  private ClassDeclarationFinder(
      Function1<ClassDeclarationFinder, ClassDeclarationFinder> childFactory) {
    this.parent = null;
    this.childFactory = childFactory;
  }

  /**
   * Creates a child optimizer.
   * Typically a child is created for each class declaration,
   * so each optimizer collects fields for exactly one class.
   *
   * @param parent parent optimizer
   */
  protected ClassDeclarationFinder(ClassDeclarationFinder parent) {
    this.parent = parent;
    this.childFactory = parent.childFactory;
  }

  /**
   * Creates optimizer local to the newly generated anonymous class.
   *
   * @param newExpression expression to optimize
   * @return nested visitor if anonymous class is given
   */
  @Override
  public Visitor preVisit(NewExpression newExpression) {
    if (newExpression.memberDeclarations == null) {
      return this;
    }
    ClassDeclarationFinder visitor = goDeeper();
    visitor.learnFinalStaticDeclarations(newExpression.memberDeclarations);
    return visitor;
  }

  /**
   * Creates optimizer local to the newly generated class.
   *
   * @param classDeclaration expression to optimize
   * @return nested visitor
   */
  @Override
  public Visitor preVisit(ClassDeclaration classDeclaration) {
    ClassDeclarationFinder visitor = goDeeper();
    visitor.learnFinalStaticDeclarations(classDeclaration.memberDeclarations);
    return visitor;
  }

  @Override
  public Expression visit(NewExpression newExpression,
      List<Expression> arguments, List<MemberDeclaration> memberDeclarations) {
    if (parent == null) {
      // Unable to optimize since no wrapper class exists to put fields to.
      arguments = newExpression.arguments;
    } else if (memberDeclarations != null) {
      // Arguments to new Test(1+2) { ... } should be optimized via parent
      // optimizer.
      arguments = Expressions.acceptExpressions(newExpression.arguments,
          parent);
    }

    Expression result =
        super.visit(newExpression, arguments, memberDeclarations);

    if (memberDeclarations == null) {
      return tryOptimizeNewInstance((NewExpression) result);
    }

    memberDeclarations = optimizeDeclarations(memberDeclarations);
    return super.visit((NewExpression) result, arguments,
        memberDeclarations);
  }

  /**
   * Processes the list of declarations when class expression detected.
   * Sub-classes might figure out the existing fields for reuse.
   *
   * @param memberDeclarations list of declarations to process.
   */
  protected void learnFinalStaticDeclarations(
      List<MemberDeclaration> memberDeclarations) {
  }

  /**
   * Optimizes {@code new Type()} constructs.
   *
   * @param newExpression expression to optimize
   * @return always returns un-optimized expression
   */
  protected Expression tryOptimizeNewInstance(NewExpression newExpression) {
    return newExpression;
  }

  @Override
  public ClassDeclaration visit(ClassDeclaration classDeclaration,
      List<MemberDeclaration> memberDeclarations) {
    memberDeclarations = optimizeDeclarations(memberDeclarations);
    return super.visit(classDeclaration, memberDeclarations);
  }

  /**
   * Adds new declarations (e.g. final static fields) to the list of existing
   * ones.
   *
   * @param memberDeclarations existing list of declarations
   * @return new list of declarations or the same if no modifications required
   */
  protected List<MemberDeclaration> optimizeDeclarations(
      List<MemberDeclaration> memberDeclarations) {
    if (addedDeclarations.isEmpty()) {
      return memberDeclarations;
    }
    List<MemberDeclaration> newDecls =
        new ArrayList<MemberDeclaration>(memberDeclarations.size()
            + addedDeclarations.size());
    newDecls.addAll(memberDeclarations);
    newDecls.addAll(addedDeclarations);
    return newDecls;
  }

  /**
   * Verifies if the expression is effectively constant.
   * This method should be overridden in sub-classes.
   *
   * @param expression expression to test
   * @return always returns false
   */
  protected boolean isConstant(Expression expression) {
    return false;
  }

  /**
   * Verifies if all the expressions in given list are  effectively constant.
   *
   * @param list list of expressions to test
   * @return true when all the expressions are known to be constant
   */
  protected boolean isConstant(Iterable<? extends Expression> list) {
    for (Expression expression : list) {
      if (!isConstant(expression)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Finds if there exists ready for reuse declaration for given expression.
   * This method should be overridden in sub-classes.
   *
   * @param expression input expression
   * @return always returns null
   */
  protected ParameterExpression findDeclaredExpression(Expression expression) {
    return null;
  }

  /**
   * Verifies if the variable name is already in use.
   * This method should be overridden in sub-classes.
   *
   * @param name name of the variable to test
   * @return always returns false
   */
  protected boolean hasField(String name) {
    return false;
  }

  /**
   * Creates child visitor. It is used to traverse nested class declarations.
   *
   * @return new {@code Visitor} that is used to optimize class declarations
   */
  protected ClassDeclarationFinder goDeeper() {
    return childFactory.apply(this);
  }
}

// End ClassDeclarationFinder.java
