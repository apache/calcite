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

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Builder for {@link BlockStatement}.
 *
 * <p>Has methods that help ensure that variable names are unique.</p>
 */
public class BlockBuilder {
  final List<Statement> statements = new ArrayList<Statement>();
  final Set<String> variables = new HashSet<String>();
  /** Contains final-fine-to-reuse-declarations.
   * An entry to this map is added when adding final declaration of a
   * statement with optimize=true parameter. */
  final Map<Expression, DeclarationStatement> expressionForReuse =
      new HashMap<Expression, DeclarationStatement>();

  private final boolean optimizing;
  private final BlockBuilder parent;

  private static final Visitor OPTIMIZE_VISITOR = new OptimizeVisitor();

  /**
   * Creates a non-optimizing BlockBuilder.
   */
  public BlockBuilder() {
    this(true);
  }

  /**
   * Creates a BlockBuilder.
   *
   * @param optimizing Whether to eliminate common sub-expressions
   */
  public BlockBuilder(boolean optimizing) {
    this(optimizing, null);
  }

  /**
   * Creates a BlockBuilder.
   *
   * @param optimizing Whether to eliminate common sub-expressions
   */
  public BlockBuilder(boolean optimizing, BlockBuilder parent) {
    this.optimizing = optimizing;
    this.parent = parent;
  }

  /**
   * Clears this BlockBuilder.
   */
  public void clear() {
    statements.clear();
    variables.clear();
    expressionForReuse.clear();
  }

  /**
   * Appends a block to a list of statements and returns an expression
   * (possibly a variable) that represents the result of the newly added
   * block.
   */
  public Expression append(String name, BlockStatement block) {
    return append(name, block, true);
  }

  /**
   * Appends an expression to a list of statements, optionally optimizing it
   * to a variable if it is used more than once.
   *
   * @param name Suggested variable name
   * @param block Expression
   * @param optimize Whether to try to optimize by assigning the expression to
   * a variable. Do not do this if the expression has
   * side-effects or a time-dependent value.
   */
  public Expression append(String name, BlockStatement block,
      boolean optimize) {
    if (statements.size() > 0) {
      Statement lastStatement = statements.get(statements.size() - 1);
      if (lastStatement instanceof GotoStatement) {
        // convert "return expr;" into "expr;"
        statements.set(statements.size() - 1, Expressions.statement(
            ((GotoStatement) lastStatement).expression));
      }
    }
    Expression result = null;
    final Map<ParameterExpression, Expression> replacements =
        new IdentityHashMap<ParameterExpression, Expression>();
    final Visitor visitor = new SubstituteVariableVisitor(replacements);
    for (int i = 0; i < block.statements.size(); i++) {
      Statement statement = block.statements.get(i);
      if (!replacements.isEmpty()) {
        // Save effort, and only substitute variables if there are some.
        statement = statement.accept(visitor);
      }
      if (statement instanceof DeclarationStatement) {
        DeclarationStatement declaration = (DeclarationStatement) statement;
        if (variables.contains(declaration.parameter.name)) {
          Expression x = append(
              newName(declaration.parameter.name, optimize),
              declaration.initializer);
          statement = null;
          result = x;
          if (declaration.parameter != x) {
            // declaration.parameter can be equal to x if exactly the same
            // declaration was present in BlockBuilder
            replacements.put(declaration.parameter, x);
          }
        } else {
          add(statement);
        }
      } else {
        add(statement);
      }
      if (i == block.statements.size() - 1) {
        if (statement instanceof DeclarationStatement) {
          result = ((DeclarationStatement) statement).parameter;
        } else if (statement instanceof GotoStatement) {
          statements.remove(statements.size() - 1);
          result = append_(name, ((GotoStatement) statement).expression,
              optimize);
          if (isSimpleExpression(result)) {
            // already simple; no need to declare a variable or
            // even to evaluate the expression
          } else {
            DeclarationStatement declare = Expressions.declare(Modifier.FINAL,
                newName(name, optimize), result);
            add(declare);
            result = declare.parameter;
          }
        } else {
          // not an expression -- result remains null
        }
      }
    }
    return result;
  }

  /**
   * Appends an expression to a list of statements, and returns an expression
   * (possibly a variable) that represents the result of the newly added
   * block.
   */
  public Expression append(String name, Expression expression) {
    return append(name, expression, true);
  }

  /**
   * Appends an expression to a list of statements, if it is not null.
   */
  public Expression appendIfNotNull(String name, Expression expression) {
    if (expression == null) {
      return null;
    }
    return append(name, expression, true);
  }

  /**
   * Appends an expression to a list of statements, optionally optimizing if
   * the expression is used more than once.
   */
  public Expression append(String name, Expression expression,
      boolean optimize) {
    if (statements.size() > 0) {
      Statement lastStatement = statements.get(statements.size() - 1);
      if (lastStatement instanceof GotoStatement) {
        // convert "return expr;" into "expr;"
        statements.set(statements.size() - 1, Expressions.statement(
            ((GotoStatement) lastStatement).expression));
      }
    }
    return append_(name, expression, optimize);
  }

  private Expression append_(String name, Expression expression,
      boolean optimize) {
    if (isSimpleExpression(expression)) {
      // already simple; no need to declare a variable or
      // even to evaluate the expression
      return expression;
    }
    if (optimizing && optimize) {
      DeclarationStatement decl = getComputedExpression(expression);
      if (decl != null) {
        return decl.parameter;
      }
    }
    DeclarationStatement declare = Expressions.declare(Modifier.FINAL, newName(
        name, optimize), expression);
    add(declare);
    return declare.parameter;
  }

  /**
   * Checks if experssion is simple enough for always inline
   * @param expr expression to test
   * @return true when given expression is safe to always inline
   */
  protected boolean isSimpleExpression(Expression expr) {
    if (expr instanceof ParameterExpression
        || expr instanceof ConstantExpression) {
      return true;
    }
    if (expr instanceof UnaryExpression) {
      UnaryExpression una = (UnaryExpression) expr;
      return una.getNodeType() == ExpressionType.Convert
          && isSimpleExpression(una.expression);
    }
    return false;
  }

  protected boolean isSafeForReuse(DeclarationStatement decl) {
    return (decl.modifiers & Modifier.FINAL) != 0;
  }

  protected void addExpressionForReuse(DeclarationStatement decl) {
    if (isSafeForReuse(decl)) {
      Expression expr = normalizeDeclaration(decl);
      expressionForReuse.put(expr, decl);
    }
  }

  /**
   * Prepares declaration for inlining: adds cast
   * @param decl inlining candidate
   * @return normalized expression
   */
  private Expression normalizeDeclaration(DeclarationStatement decl) {
    Expression expr = decl.initializer;
    Type declType = decl.parameter.getType();
    if (expr == null) {
      expr = Expressions.constant(null, declType);
    } else if (expr.getType() != declType) {
      expr = Expressions.convert_(expr, declType);
    }
    return expr;
  }

  /**
   * Returns the reference to ParameterExpression if given expression was
   * already computed and stored to local variable
   * @param expr expression to test
   * @return existing ParameterExpression or null
   */
  public DeclarationStatement getComputedExpression(Expression expr) {
    if (parent != null) {
      DeclarationStatement decl = parent.getComputedExpression(expr);
      if (decl != null) {
        return decl;
      }
    }
    return optimizing ? expressionForReuse.get(expr) : null;
  }

  public void add(Statement statement) {
    statements.add(statement);
    if (statement instanceof DeclarationStatement) {
      DeclarationStatement decl = (DeclarationStatement) statement;
      String name = decl.parameter.name;
      if (!variables.add(name)) {
        throw new AssertionError("duplicate variable " + name);
      }
      addExpressionForReuse(decl);
    }
  }

  public void add(Expression expression) {
    add(Expressions.return_(null, expression));
  }

  /**
   * Returns a block consisting of the current list of statements.
   */
  public BlockStatement toBlock() {
    if (optimizing) {
      // We put an artificial limit of 10 iterations just to prevent an endless
      // loop. Optimize should not loop forever, however it is hard to prove if
      // it always finishes in reasonable time.
      for (int i = 0; i < 10; i++) {
        if (!optimize(createOptimizeVisitor(), true)) {
          break;
        }
      }
      optimize(createFinishingOptimizeVisitor(), false);
    }
    return Expressions.block(statements);
  }

  /**
   * Optimizes the list of statements. If an expression is used only once,
   * it is inlined.
   *
   * @return whether any optimizations were made
   */
  private boolean optimize(Visitor optimizer, boolean performInline) {
    int optimizeCount = 0;
    final UseCounter useCounter = new UseCounter();
    for (Statement statement : statements) {
      if (statement instanceof DeclarationStatement && performInline) {
        DeclarationStatement decl = (DeclarationStatement) statement;
        useCounter.map.put(decl.parameter, new Slot());
      }
      // We are added only counters up to current statement.
      // It is fine to count usages as the latter declarations cannot be used
      // in more recent statements.
      if (!useCounter.map.isEmpty()) {
        statement.accept(useCounter);
      }
    }
    final Map<ParameterExpression, Expression> subMap =
        new IdentityHashMap<ParameterExpression, Expression>(
            useCounter.map.size());
    final SubstituteVariableVisitor visitor = new SubstituteVariableVisitor(
        subMap);
    final ArrayList<Statement> oldStatements = new ArrayList<Statement>(
        statements);
    statements.clear();

    for (Statement oldStatement : oldStatements) {
      if (oldStatement instanceof DeclarationStatement) {
        DeclarationStatement statement = (DeclarationStatement) oldStatement;
        final Slot slot = useCounter.map.get(statement.parameter);
        int count = slot == null ? Integer.MAX_VALUE - 10 : slot.count;
        if (count > 1 && isSimpleExpression(statement.initializer)) {
          // Inline simple final constants
          count = 1;
        }
        if (!isSafeForReuse(statement)) {
          // Don't inline variables that are not final. They might be assigned
          // more than once.
          count = 100;
        }
        if (statement.parameter.name.startsWith("_")) {
          // Don't inline variables whose name begins with "_". This
          // is a hacky way to prevent inlining. E.g.
          //   final int _count = collection.size();
          //   foo(collection);
          //   return collection.size() - _count;
          count = Integer.MAX_VALUE;
        }
        if (statement.initializer instanceof NewExpression
            && ((NewExpression) statement.initializer).memberDeclarations
                != null) {
          // Don't inline anonymous inner classes. Janino gets
          // confused referencing variables from deeply nested
          // anonymous classes.
          count = Integer.MAX_VALUE;
        }
        Expression normalized = normalizeDeclaration(statement);
        expressionForReuse.remove(normalized);
        switch (count) {
        case 0:
          // Only declared, never used. Throw away declaration.
          break;
        case 1:
          // declared, used once. inline it.
          subMap.put(statement.parameter, normalized);
          break;
        default:
          Statement beforeOptimize = oldStatement;
          if (!subMap.isEmpty()) {
            oldStatement = oldStatement.accept(visitor); // remap
          }
          oldStatement = oldStatement.accept(optimizer);
          if (beforeOptimize != oldStatement) {
            ++optimizeCount;
            if (count != Integer.MAX_VALUE
                && oldStatement instanceof DeclarationStatement
                && isSafeForReuse((DeclarationStatement) oldStatement)
                && isSimpleExpression(
                  ((DeclarationStatement) oldStatement).initializer)) {
              // Allow to inline the expression that became simple after
              // optimizations.
              DeclarationStatement newDecl =
                  (DeclarationStatement) oldStatement;
              subMap.put(newDecl.parameter, normalizeDeclaration(newDecl));
              oldStatement = OptimizeVisitor.EMPTY_STATEMENT;
            }
          }
          if (oldStatement != OptimizeVisitor.EMPTY_STATEMENT) {
            if (oldStatement instanceof DeclarationStatement) {
              addExpressionForReuse((DeclarationStatement) oldStatement);
            }
            statements.add(oldStatement);
          }
          break;
        }
      } else {
        Statement beforeOptimize = oldStatement;
        if (!subMap.isEmpty()) {
          oldStatement = oldStatement.accept(visitor); // remap
        }
        oldStatement = oldStatement.accept(optimizer);
        if (beforeOptimize != oldStatement) {
          ++optimizeCount;
        }
        if (oldStatement != OptimizeVisitor.EMPTY_STATEMENT) {
          statements.add(oldStatement);
        }
      }
    }
    return optimizeCount > 0;
  }

  /**
   * Creates a visitor that will be used during block optimization.
   * Subclasses might provide more specific optimizations (e.g. partial
   * evaluation).
   *
   * @return visitor used to optimize the statements when converting to block
   */
  protected Visitor createOptimizeVisitor() {
    return OPTIMIZE_VISITOR;
  }

  /**
   * Creates a final optimization visitor.
   * Typically, the visitor will factor out constant expressions.
   *
   * @return visitor that is used to finalize the optimization
   */
  protected Visitor createFinishingOptimizeVisitor() {
    return ClassDeclarationFinder.create();
  }

  /**
   * Creates a name for a new variable, unique within this block, controlling
   * whether the variable can be inlined later.
   */
  private String newName(String suggestion, boolean optimize) {
    if (!optimize && !suggestion.startsWith("_")) {
      // "_" prefix reminds us not to consider the variable for inlining
      suggestion = '_' + suggestion;
    }
    return newName(suggestion);
  }

  /**
   * Creates a name for a new variable, unique within this block.
   */
  public String newName(String suggestion) {
    int i = 0;
    String candidate = suggestion;
    while (hasVariable(candidate)) {
      candidate = suggestion + (i++);
    }
    return candidate;
  }

  public boolean hasVariable(String name) {
    return variables.contains(name)
        || (parent != null && parent.hasVariable(name));
  }

  public BlockBuilder append(Expression expression) {
    add(expression);
    return this;
  }

  private static class SubstituteVariableVisitor extends Visitor {
    private final Map<ParameterExpression, Expression> map;
    private final Map<ParameterExpression, Boolean> actives =
        new IdentityHashMap<ParameterExpression, Boolean>();

    public SubstituteVariableVisitor(Map<ParameterExpression, Expression> map) {
      this.map = map;
    }

    @Override
    public Expression visit(ParameterExpression parameterExpression) {
      Expression e = map.get(parameterExpression);
      if (e != null) {
        try {
          final Boolean put = actives.put(parameterExpression, true);
          if (put != null) {
            throw new AssertionError(
                "recursive expansion of " + parameterExpression + " in "
                + actives.keySet());
          }
          // recursively substitute
          return e.accept(this);
        } finally {
          actives.remove(parameterExpression);
        }
      }
      return super.visit(parameterExpression);
    }

    @Override
    public Expression visit(UnaryExpression unaryExpression, Expression
        expression) {
      if (unaryExpression.getNodeType().modifiesLvalue) {
        expression = unaryExpression.expression; // avoid substitution
        if (expression instanceof ParameterExpression) {
          // avoid "optimization of" int t=1; t++; to 1++
          return unaryExpression;
        }
      }
      return super.visit(unaryExpression, expression);
    }

    @Override public Expression visit(BinaryExpression binaryExpression,
        Expression expression0, Expression expression1) {
      if (binaryExpression.getNodeType().modifiesLvalue) {
        expression0 = binaryExpression.expression0; // avoid substitution
        if (expression0 instanceof ParameterExpression) {
          // If t is a declaration used only once, replace
          //   int t;
          //   int v = (t = 1) != a ? c : d;
          // with
          //   int v = 1 != a ? c : d;
          if (map.containsKey(expression0)) {
            return expression1.accept(this);
          }
        }
      }
      return super.visit(binaryExpression, expression0, expression1);
    }
  }

  private static class UseCounter extends Visitor {
    private final Map<ParameterExpression, Slot> map =
        new IdentityHashMap<ParameterExpression, Slot>();

    @Override
    public Expression visit(ParameterExpression parameter) {
      final Slot slot = map.get(parameter);
      if (slot != null) {
        // Count use of parameter, if it's registered. It's OK if
        // parameter is not registered. It might be beyond the control
        // of this block.
        slot.count++;
      }
      return super.visit(parameter);
    }
  }

  /**
   * Holds the number of times a declaration was used.
   */
  private static class Slot {
    private int count;
  }
}

// End BlockBuilder.java
