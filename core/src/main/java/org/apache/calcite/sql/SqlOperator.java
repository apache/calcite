/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlCallFactories;
import org.apache.calcite.sql.fun.SqlCallFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlOperator</code> is a type of node in a SQL parse tree (it is NOT a
 * node in a SQL parse tree). It includes functions, operators such as '=', and
 * syntactic constructs such as 'case' statements. Operators may represent
 * query-level expressions (e.g. {@link SqlSelectOperator} or row-level
 * expressions (e.g. {@link org.apache.calcite.sql.fun.SqlBetweenOperator}.
 *
 * <p>Operators have <em>formal operands</em>, meaning ordered (and optionally
 * named) placeholders for the values they operate on. For example, the division
 * operator takes two operands; the first is the numerator and the second is the
 * denominator. In the context of subclass {@link SqlFunction}, formal operands
 * are referred to as <em>parameters</em>.
 *
 * <p>When an operator is instantiated via a {@link SqlCall}, it is supplied
 * with <em>actual operands</em>. For example, in the expression <code>3 /
 * 5</code>, the literal expression <code>3</code> is the actual operand
 * corresponding to the numerator, and <code>5</code> is the actual operand
 * corresponding to the denominator. In the context of SqlFunction, actual
 * operands are referred to as <em>arguments</em>
 *
 * <p>In many cases, the formal/actual distinction is clear from context, in
 * which case we drop these qualifiers.
 */
public abstract class SqlOperator {
  //~ Static fields/initializers ---------------------------------------------

  public static final String NL = System.lineSeparator();

  /**
   * Maximum precedence.
   */
  public static final int MDX_PRECEDENCE = 200;

  //~ Instance fields --------------------------------------------------------

  /**
   * The name of the operator/function. Ex. "OVERLAY" or "TRIM"
   */
  private final String name;

  /**
   * See {@link SqlKind}. It's possible to have a name that doesn't match the
   * kind
   */
  public final SqlKind kind;

  /**
   * The precedence with which this operator binds to the expression to the
   * left. This is less than the right precedence if the operator is
   * left-associative.
   */
  private final int leftPrec;

  /**
   * The precedence with which this operator binds to the expression to the
   * right. This is more than the left precedence if the operator is
   * left-associative.
   */
  private final int rightPrec;

  /** Used to infer the return type of a call to this operator. */
  private final @Nullable SqlReturnTypeInference returnTypeInference;

  /** Used to infer types of unknown operands. */
  private final @Nullable SqlOperandTypeInference operandTypeInference;

  /** Used to validate operand types. */
  private final @Nullable SqlOperandTypeChecker operandTypeChecker;

  /** Used to create {@link org.apache.calcite.sql.SqlCall}. */
  private final SqlCallFactory callFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an operator.
   */
  protected SqlOperator(
      String name,
      SqlKind kind,
      int leftPrecedence,
      int rightPrecedence,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      SqlCallFactory callFactory) {
    this.name = name;
    this.kind = requireNonNull(kind, "kind");
    this.leftPrec = leftPrecedence;
    this.rightPrec = rightPrecedence;
    this.returnTypeInference = returnTypeInference;
    if (operandTypeInference == null
        && operandTypeChecker != null) {
      operandTypeInference = operandTypeChecker.typeInference();
    }
    this.operandTypeInference = operandTypeInference;
    this.operandTypeChecker = operandTypeChecker;
    this.callFactory = callFactory;
  }

  /**
   * Creates an operator.
   */
  protected SqlOperator(
      String name,
      SqlKind kind,
      int leftPrecedence,
      int rightPrecedence,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker) {
    this(
        name,
        kind,
        leftPrecedence,
        rightPrecedence,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker,
        SqlCallFactories.SQL_BASIC_CALL_FACTORY);
  }

  /**
   * Creates an operator specifying left/right associativity.
   */
  protected SqlOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean leftAssoc,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker) {
    this(
        name,
        kind,
        leftPrec(prec, leftAssoc),
        rightPrec(prec, leftAssoc),
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  protected static int leftPrec(int prec, boolean leftAssoc) {
    assert (prec % 2) == 0;
    if (!leftAssoc) {
      ++prec;
    }
    return prec;
  }

  protected static int rightPrec(int prec, boolean leftAssoc) {
    assert (prec % 2) == 0;
    if (leftAssoc) {
      ++prec;
    }
    return prec;
  }

  public @Nullable SqlOperandTypeChecker getOperandTypeChecker() {
    return operandTypeChecker;
  }

  /**
   * Returns a constraint on the number of operands expected by this operator.
   * Subclasses may override this method; when they don't, the range is
   * derived from the {@link SqlOperandTypeChecker} associated with this
   * operator.
   *
   * @return acceptable range
   */
  public SqlOperandCountRange getOperandCountRange() {
    if (operandTypeChecker != null) {
      return operandTypeChecker.getOperandCountRange();
    }

    // If you see this error you need to override this method
    // or give operandTypeChecker a value.
    throw Util.needToImplement(this);
  }

  public String getName() {
    return name;
  }

  /**
   * Returns the fully-qualified name of this operator.
   */
  public SqlIdentifier getNameAsId() {
    return new SqlIdentifier(getName(), SqlParserPos.ZERO);
  }

  @Pure
  public SqlKind getKind() {
    return kind;
  }

  @Override public String toString() {
    return name;
  }

  public int getLeftPrec() {
    return leftPrec;
  }

  public int getRightPrec() {
    return rightPrec;
  }

  /**
   * Returns the syntactic type of this operator, never null.
   */
  public abstract SqlSyntax getSyntax();

  public final SqlCallFactory getSqlCallFactory() {
    return callFactory;
  }

  /**
   * Creates a call to this operator with a list of operands.
   *
   * <p>The position of the resulting call is the union of the {@code pos}
   * and the positions of all of the operands.
   *
   * @param functionQualifier Function qualifier (e.g. "DISTINCT"), or null
   * @param pos               Parser position of the identifier of the call
   * @param operands          List of operands
   */
  public final SqlCall createCall(
      @Nullable SqlLiteral functionQualifier,
      SqlParserPos pos,
      Iterable<? extends @Nullable SqlNode> operands) {
    return createCall(functionQualifier, pos,
        Iterables.toArray(operands, SqlNode.class));
  }

  /**
   * Creates a call to this operator with an array of operands.
   *
   * <p>The position of the resulting call is the union of the {@code pos}
   * and the positions of all of the operands.
   *
   * @param functionQualifier Function qualifier (e.g. "DISTINCT"), or null
   * @param pos               Parser position of the identifier of the call
   * @param operands          Array of operands
   */
  public SqlCall createCall(
      @Nullable SqlLiteral functionQualifier,
      SqlParserPos pos,
      @Nullable SqlNode... operands) {
    return callFactory.create(this, functionQualifier, pos, operands);
  }

  /** Not supported. Choose between
   * {@link #createCall(SqlLiteral, SqlParserPos, SqlNode...)} and
   * {@link #createCall(SqlParserPos, List)}. The ambiguity arises because
   * {@link SqlNodeList} extends {@link SqlNode}
   * and also implements {@code List<SqlNode>}. */
  @Deprecated
  public static SqlCall createCall(
      @Nullable SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNodeList operands) {
    throw new UnsupportedOperationException();
  }

  /**
   * Creates a call to this operator with an array of operands.
   *
   * <p>The position of the resulting call is the union of the <code>
   * pos</code> and the positions of all of the operands.
   *
   * @param pos      Parser position
   * @param operands List of arguments
   * @return call to this operator
   */
  public final SqlCall createCall(
      SqlParserPos pos,
      @Nullable SqlNode... operands) {
    return createCall(null, pos, operands);
  }

  /**
   * Creates a call to this operator with a list of operands contained in a
   * {@link SqlNodeList}.
   *
   * <p>The position of the resulting call is inferred from the SqlNodeList.
   *
   * @param nodeList List of arguments
   * @return call to this operator
   */
  public final SqlCall createCall(
      SqlNodeList nodeList) {
    return createCall(
        null,
        nodeList.getParserPosition(),
        nodeList.toArray(new SqlNode[0]));
  }

  /**
   * Creates a call to this operator with a list of operands.
   *
   * <p>The position of the resulting call is the union of the {@code pos}
   * and the positions of all of the operands.
   */
  public final SqlCall createCall(
      SqlParserPos pos,
      List<? extends @Nullable SqlNode> operandList) {
    return createCall(
        null,
        pos,
        operandList.toArray(new SqlNode[0]));
  }

  /** Not supported. Choose between
   * {@link #createCall(SqlParserPos, SqlNode...)} and
   * {@link #createCall(SqlParserPos, List)}. The ambiguity arises because
   * {@link SqlNodeList} extends {@link SqlNode}
   * and also implements {@code List<SqlNode>}. */
  @Deprecated
  public SqlCall createCall(
      SqlParserPos pos,
      SqlNodeList operands) {
    throw new UnsupportedOperationException();
  }

  /**
   * Rewrites a call to this operator. Some operators are implemented as
   * trivial rewrites (e.g. NULLIF becomes CASE). However, we don't do this at
   * createCall time because we want to preserve the original SQL syntax as
   * much as possible; instead, we do this before the call is validated (so
   * the trivial operator doesn't need its own implementation of type
   * derivation methods). The default implementation is to just return the
   * original call without any rewrite.
   *
   * @param validator Validator
   * @param call      Call to be rewritten
   * @return rewritten call
   */
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    return call;
  }

  /**
   * Writes a SQL representation of a call to this operator to a writer,
   * including parentheses if the operators on either side are of greater
   * precedence.
   *
   * <p>The default implementation of this method delegates to
   * {@link SqlSyntax#unparse}.
   */
  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    getSyntax().unparse(writer, this, call, leftPrec, rightPrec);
  }

  @Deprecated // to be removed before 2.0
  protected void unparseListClause(SqlWriter writer, SqlNode clause) {
    final SqlNodeList nodeList =
        clause instanceof SqlNodeList
            ? (SqlNodeList) clause
            : SqlNodeList.of(clause);
    writer.list(SqlWriter.FrameTypeEnum.SIMPLE, SqlWriter.COMMA, nodeList);
  }

  @Deprecated // to be removed before 2.0
  protected void unparseListClause(
      SqlWriter writer,
      SqlNode clause,
      @Nullable SqlKind sepKind) {
    final SqlNodeList nodeList =
        clause instanceof SqlNodeList
            ? (SqlNodeList) clause
            : SqlNodeList.of(clause);
    final SqlBinaryOperator sepOp;
    if (sepKind == null) {
      sepOp = SqlWriter.COMMA;
    } else {
      switch (sepKind) {
      case AND:
        sepOp = SqlStdOperatorTable.AND;
        break;
      case OR:
        sepOp = SqlStdOperatorTable.OR;
        break;
      default:
        throw new AssertionError();
      }
    }
    writer.list(SqlWriter.FrameTypeEnum.SIMPLE, sepOp, nodeList);
  }

  @Override public boolean equals(@Nullable Object obj) {
    if (!(obj instanceof SqlOperator)) {
      return false;
    }
    if (!obj.getClass().equals(this.getClass())) {
      return false;
    }
    SqlOperator other = (SqlOperator) obj;
    return name.equals(other.name) && kind == other.kind;
  }

  public boolean isName(String testName, boolean caseSensitive) {
    return caseSensitive ? name.equals(testName) : name.equalsIgnoreCase(testName);
  }

  @Override public int hashCode() {
    return Objects.hash(kind, name);
  }

  /**
   * Validates a call to this operator.
   *
   * <p>This method should not perform type-derivation or perform validation
   * related to types. That is done later, by
   * {@link #deriveType(SqlValidator, SqlValidatorScope, SqlCall)}. This method
   * should focus on structural validation.
   *
   * <p>A typical implementation of this method first validates the operands,
   * then performs some operator-specific logic. The default implementation
   * just validates the operands.
   *
   * <p>This method is the default implementation of {@link SqlCall#validate};
   * but note that some sub-classes of {@link SqlCall} never call this method.
   *
   * @param call         the call to this operator
   * @param validator    the active validator
   * @param scope        validator scope
   * @param operandScope validator scope in which to validate operands to this
   *                     call; usually equal to scope, but not always because
   *                     some operators introduce new scopes
   * @see SqlNode#validateExpr(SqlValidator, SqlValidatorScope)
   * @see #deriveType(SqlValidator, SqlValidatorScope, SqlCall)
   */
  public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    assert call.getOperator() == this;
    for (SqlNode operand : call.getOperandList()) {
      operand.validateExpr(validator, operandScope);
    }
  }

  /**
   * Validates the operands of a call, inferring the return type in the
   * process.
   *
   * @param validator active validator
   * @param scope     validation scope
   * @param call      call to be validated
   * @return inferred type
   */
  public final RelDataType validateOperands(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
    // Let subclasses know what's up.
    preValidateCall(validator, scope, call);

    // Check the number of operands
    checkOperandCount(validator, operandTypeChecker, call);

    SqlCallBinding opBinding = new SqlCallBinding(validator, scope, call);

    checkOperandTypes(
        opBinding,
        true);

    // Now infer the result type.
    RelDataType ret = inferReturnType(opBinding);
    validator.setValidatedNodeType(call, ret);
    return ret;
  }

  /**
   * Receives notification that validation of a call to this operator is
   * beginning. Subclasses can supply custom behavior; default implementation
   * does nothing.
   *
   * @param validator invoking validator
   * @param scope     validation scope
   * @param call      the call being validated
   */
  protected void preValidateCall(SqlValidator validator,
      SqlValidatorScope scope, SqlCall call) {
  }

  /**
   * Infers the return type of an invocation of this operator; only called
   * after the number and types of operands have already been validated.
   * Subclasses must either override this method or supply an instance of
   * {@link SqlReturnTypeInference} to the constructor.
   *
   * @param opBinding description of invocation (not necessarily a
   * {@link SqlCall})
   * @return inferred return type
   */
  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    if (returnTypeInference != null) {
      RelDataType returnType = returnTypeInference.inferReturnType(opBinding);
      if (returnType == null) {
        throw opBinding.newError(
            RESOURCE.cannotInferReturnType(
                opBinding.getOperator().toString(),
                opBinding.collectOperandTypes().toString()));
      }

      if (operandTypeInference != null
          && opBinding instanceof SqlCallBinding
          && this instanceof SqlFunction) {
        final SqlCallBinding callBinding = (SqlCallBinding) opBinding;
        final List<RelDataType> operandTypes = opBinding.collectOperandTypes();
        if (operandTypes.stream().anyMatch(t -> t.getSqlTypeName() == SqlTypeName.ANY)) {
          final RelDataType[] operandTypes2 = operandTypes.toArray(new RelDataType[0]);
          operandTypeInference.inferOperandTypes(callBinding, returnType, operandTypes2);
          ((SqlValidatorImpl) callBinding.getValidator())
              .callToOperandTypesMap
              .put(callBinding.getCall(), ImmutableList.copyOf(operandTypes2));
        }
      }

      return returnType;
    }

    // Derived type should have overridden this method, since it didn't
    // supply a type inference rule.
    throw Util.needToImplement(this);
  }

  /**
   * Derives the type of a call to this operator.
   *
   * <p>This method is an intrinsic part of the validation process so, unlike
   * {@link #inferReturnType}, specific operators would not typically override
   * this method.
   *
   * @param validator Validator
   * @param scope     Scope of validation
   * @param call      Call to this operator
   * @return Type of call
   */
  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    for (SqlNode operand : call.getOperandList()) {
      RelDataType nodeType = validator.deriveType(scope, operand);
      assert nodeType != null;
    }

    final List<SqlNode> args = constructOperandList(validator, call, null);

    final List<RelDataType> argTypes =
        constructArgTypeList(validator, scope, call, args, false);

    // Always disable type coercion for builtin operator operands,
    // they are handled by the TypeCoercion specifically.
    final SqlOperator sqlOperator =
        SqlUtil.lookupRoutine(validator.getOperatorTable(),
            validator.getTypeFactory(), getNameAsId(),
            argTypes, null, null, getSyntax(), getKind(),
            validator.getCatalogReader().nameMatcher(), false);

    if (sqlOperator == null) {
      throw validator.handleUnresolvedFunction(call, this, argTypes, null);
    }

    ((SqlBasicCall) call).setOperator(castNonNull(sqlOperator));
    RelDataType type = call.getOperator().validateOperands(validator, scope, call);

    // Validate and determine coercibility and resulting collation
    // name of binary operator if needed.
    type = adjustType(validator, call, type);
    SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type);
    return type;
  }

  protected @Nullable List<String> constructArgNameList(SqlCall call) {
    // If any arguments are named, construct a map.
    final ImmutableList.Builder<String> nameBuilder = ImmutableList.builder();
    for (SqlNode operand : call.getOperandList()) {
      if (operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
        final List<SqlNode> operandList = ((SqlCall) operand).getOperandList();
        nameBuilder.add(((SqlIdentifier) operandList.get(1)).getSimple());
      }
    }
    ImmutableList<String> argNames = nameBuilder.build();

    if (argNames.isEmpty()) {
      return null;
    } else {
      return argNames;
    }
  }

  protected List<SqlNode> constructOperandList(
      SqlValidator validator,
      SqlCall call,
      @Nullable List<String> argNames) {
    if (argNames == null) {
      return call.getOperandList();
    }
    if (argNames.size() < call.getOperandList().size()) {
      throw validator.newValidationError(call,
          RESOURCE.someButNotAllArgumentsAreNamed());
    }
    final int duplicate = Util.firstDuplicate(argNames);
    if (duplicate >= 0) {
      throw validator.newValidationError(call,
          RESOURCE.duplicateArgumentName(argNames.get(duplicate)));
    }
    final ImmutableList.Builder<SqlNode> argBuilder = ImmutableList.builder();
    for (SqlNode operand : call.getOperandList()) {
      if (operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
        final List<SqlNode> operandList = ((SqlCall) operand).getOperandList();
        argBuilder.add(operandList.get(0));
      }
    }
    return argBuilder.build();
  }

  protected List<RelDataType> constructArgTypeList(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call,
      List<SqlNode> args,
      boolean convertRowArgToColumnList) {
    // Scope for operands. Usually the same as 'scope'.
    final SqlValidatorScope operandScope = scope.getOperandScope(call);

    final ImmutableList.Builder<RelDataType> argTypeBuilder =
        ImmutableList.builder();
    for (int i = 0; i < args.size(); i++) {
      SqlNode operand = args.get(i);
      RelDataType nodeType;
      // for row arguments that should be converted to ColumnList
      // types, set the nodeType to a ColumnList type but defer
      // validating the arguments of the row constructor until we know
      // for sure that the row argument maps to a ColumnList type
      if (operand.getKind() == SqlKind.ROW && convertRowArgToColumnList) {
        RelDataTypeFactory typeFactory = validator.getTypeFactory();
        nodeType = typeFactory.createSqlType(SqlTypeName.COLUMN_LIST);
        validator.setValidatedNodeType(operand, nodeType);
      } else {
        nodeType = deriveOperandType(validator, operandScope, i, operand);
      }
      argTypeBuilder.add(nodeType);
    }

    return argTypeBuilder.build();
  }

  protected RelDataType deriveOperandType(SqlValidator validator,
      SqlValidatorScope scope, int i, SqlNode operand) {
    return validator.deriveType(scope, operand);
  }

  /**
   * Returns whether this operator should be surrounded by space when
   * unparsed.
   *
   * @return whether this operator should be surrounded by space
   */
  boolean needsSpace() {
    return true;
  }

  /**
   * Validates and determines coercibility and resulting collation name of
   * binary operator if needed.
   */
  protected RelDataType adjustType(
      SqlValidator validator,
      final SqlCall call,
      RelDataType type) {
    return type;
  }

  /**
   * Infers the type of a call to this operator with a given set of operand
   * types. Shorthand for {@link #inferReturnType(SqlOperatorBinding)}.
   */
  public final RelDataType inferReturnType(
      RelDataTypeFactory typeFactory,
      List<RelDataType> operandTypes) {
    return inferReturnType(
        new ExplicitOperatorBinding(
            typeFactory,
            this,
            operandTypes));
  }

  /**
   * Checks that the operand values in a {@link SqlCall} to this operator are
   * valid. Subclasses must either override this method or supply an instance
   * of {@link SqlOperandTypeChecker} to the constructor.
   *
   * @param callBinding    description of call
   * @param throwOnFailure whether to throw an exception if check fails
   *                       (otherwise returns false in that case)
   * @return whether check succeeded
   */
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    // Check that all of the operands are of the right type.
    if (null == operandTypeChecker) {
      // If you see this you must either give operandTypeChecker a value
      // or override this method.
      throw Util.needToImplement(this);
    }

    if (kind != SqlKind.ARGUMENT_ASSIGNMENT) {
      for (Ord<SqlNode> operand : Ord.zip(callBinding.operands())) {
        if (operand.e != null
            && operand.e.getKind() == SqlKind.DEFAULT
            && !operandTypeChecker.isOptional(operand.i)) {
          throw callBinding.newValidationError(RESOURCE.defaultForOptionalParameter());
        }
      }
    }

    return operandTypeChecker.checkOperandTypes(
        callBinding,
        throwOnFailure);
  }

  protected void checkOperandCount(
      SqlValidator validator,
      @Nullable SqlOperandTypeChecker argType,
      SqlCall call) {
    SqlOperandCountRange od = call.getOperator().getOperandCountRange();
    if (od.isValidCount(call.operandCount())) {
      return;
    }
    if (od.getMin() == od.getMax()) {
      throw validator.newValidationError(call,
          RESOURCE.invalidArgCount(call.getOperator().getName(), od.getMin()));
    } else {
      throw validator.newValidationError(call, RESOURCE.wrongNumOfArguments());
    }
  }

  /**
   * Returns whether the given operands are valid. If not valid and
   * {@code fail}, throws an assertion error.
   *
   * <p>Similar to {@link #checkOperandCount}, but some operators may have
   * different valid operands in {@link SqlNode} and {@code RexNode} formats
   * (some examples are CAST and AND), and this method throws internal errors,
   * not user errors.
   */
  public boolean validRexOperands(int count, Litmus litmus) {
    return true;
  }

  /**
   * Returns a template describing how the operator signature is to be built.
   * E.g for the binary + operator the template looks like "{1} {0} {2}" {0}
   * is the operator, subsequent numbers are operands.
   *
   * @param operandsCount is used with functions that can take a variable
   *                      number of operands
   * @return signature template, or null to indicate that a default template
   * will suffice
   */
  public @Nullable String getSignatureTemplate(final int operandsCount) {
    return null;
  }

  /**
   * Returns a string describing the expected operand types of a call, e.g.
   * "SUBSTR(VARCHAR, INTEGER, INTEGER)".
   */
  public final String getAllowedSignatures() {
    return getAllowedSignatures(name);
  }

  /**
   * Returns a string describing the expected operand types of a call, e.g.
   * "SUBSTRING(VARCHAR, INTEGER, INTEGER)" where the name (SUBSTRING in this
   * example) can be replaced by a specified name.
   */
  public String getAllowedSignatures(String opNameToUse) {
    requireNonNull(operandTypeChecker,
        "If you see this, assign operandTypeChecker a value "
        + "or override this function");
    return operandTypeChecker.getAllowedSignatures(this, opNameToUse)
        .trim();
  }

  public @Nullable SqlOperandTypeInference getOperandTypeInference() {
    return operandTypeInference;
  }

  /**
   * Returns whether this operator is an aggregate function. By default,
   * subclass type is used (an instance of SqlAggFunction is assumed to be an
   * aggregator; anything else is not).
   *
   * <p>Per SQL:2011, there are <dfn>aggregate functions</dfn> and
   * <dfn>window functions</dfn>.
   * Every aggregate function (e.g. SUM) is also a window function.
   * There are window functions that are not aggregate functions, e.g. RANK,
   * NTILE, LEAD, FIRST_VALUE.
   *
   * <p>Collectively, aggregate and window functions are called <dfn>analytic
   * functions</dfn>. Despite its name, this method returns true for every
   * analytic function.
   *
   * @see #requiresOrder()
   *
   * @return whether this operator is an analytic function (aggregate function
   * or window function)
   */
  @Pure
  public boolean isAggregator() {
    return false;
  }

  /** Returns whether this is a window function that requires an OVER clause.
   *
   * <p>For example, returns true for {@code RANK}, {@code DENSE_RANK} and
   * other ranking functions; returns false for {@code SUM}, {@code COUNT},
   * {@code MIN}, {@code MAX}, {@code AVG} (they can be used as non-window
   * aggregate functions).
   *
   * <p>If {@code requiresOver} returns true, then {@link #isAggregator()} must
   * also return true.
   *
   * @see #allowsFraming()
   * @see #requiresOrder()
   */
  public boolean requiresOver() {
    return false;
  }

  /**
   * Returns whether this is a window function that requires ordering.
   *
   * <p>Per SQL:2011, 2, 6.10: "If &lt;ntile function&gt;, &lt;lead or lag
   * function&gt;, RANK or DENSE_RANK is specified, then the window ordering
   * clause shall be present."
   *
   * @see #isAggregator()
   */
  public boolean requiresOrder() {
    return false;
  }

  /**
   * Returns whether this is a window function that allows framing (i.e. a
   * ROWS or RANGE clause in the window specification).
   */
  public boolean allowsFraming() {
    return true;
  }

  /**
   * Returns whether this is a group function.
   *
   * <p>Group functions can only appear in the GROUP BY clause.
   *
   * <p>Examples are {@code HOP}, {@code TUMBLE}, {@code SESSION}.
   *
   * <p>Group functions have auxiliary functions, e.g. {@code HOP_START}, but
   * these are not group functions.
   */
  public boolean isGroup() {
    return false;
  }

  /**
   * Returns whether this is an group auxiliary function.
   *
   * <p>Examples are {@code HOP_START} and {@code HOP_END} (both auxiliary to
   * {@code HOP}).
   *
   * @see #isGroup()
   */
  public boolean isGroupAuxiliary() {
    return false;
  }

  /**
   * Accepts a {@link SqlVisitor}, visiting each operand of a call. Returns
   * null.
   *
   * @param visitor Visitor
   * @param call    Call to visit
   */
  public <R> @Nullable R acceptCall(SqlVisitor<R> visitor, SqlCall call) {
    for (SqlNode operand : call.getOperandList()) {
      if (operand == null) {
        continue;
      }
      operand.accept(visitor);
    }
    return null;
  }

  /**
   * Accepts a {@link SqlVisitor}, directing an
   * {@link org.apache.calcite.sql.util.SqlBasicVisitor.ArgHandler}
   * to visit an operand of a call.
   *
   * <p>The argument handler allows fine control about how the operands are
   * visited, and how the results are combined.
   *
   * @param visitor         Visitor
   * @param call            Call to visit
   * @param onlyExpressions If true, ignores operands which are not
   *                        expressions. For example, in the call to the
   *                        <code>AS</code> operator
   * @param argHandler      Called for each operand
   */
  public <R> void acceptCall(
      SqlVisitor<R> visitor,
      SqlCall call,
      boolean onlyExpressions,
      SqlBasicVisitor.ArgHandler<R> argHandler) {
    List<SqlNode> operands = call.getOperandList();
    for (int i = 0; i < operands.size(); i++) {
      argHandler.visitChild(visitor, call, i, operands.get(i));
    }
  }

  /** Returns the return type inference strategy for this operator, or null if
   * return type inference is implemented by a subclass override. */
  public @Nullable SqlReturnTypeInference getReturnTypeInference() {
    return returnTypeInference;
  }

  /** Returns the operator that is the logical inverse of this operator.
   *
   * <p>For example, {@code SqlStdOperatorTable.LIKE.not()} returns
   * {@code SqlStdOperatorTable.NOT_LIKE}, and vice versa.
   *
   * <p>By default, returns {@code null}, which means there is no inverse
   * operator.
   *
   * @see #reverse */
  public @Nullable SqlOperator not() {
    return null;
  }

  /** Returns the operator that has the same effect as this operator
   * if its arguments are reversed.
   *
   * <p>For example, {@code SqlStdOperatorTable.GREATER_THAN.reverse()} returns
   * {@code SqlStdOperatorTable.LESS_THAN}, and vice versa,
   * because {@code a > b} is equivalent to {@code b < a}.
   *
   * <p>{@code SqlStdOperatorTable.EQUALS.reverse()} returns itself.
   *
   * <p>By default, returns {@code null}, which means there is no inverse
   * operator.
   *
   * @see SqlOperator#not()
   * @see SqlKind#reverse()
   */
  public @Nullable SqlOperator reverse() {
    return null;
  }

  /**
   * Returns the {@link Strong.Policy} strategy for this operator, or null if
   * there is no particular strategy, in which case this policy will be deducted
   * from the operator's {@link SqlKind}.
   *
   * @see Strong
   */
  @Pure
  public @Nullable Supplier<Strong.Policy> getStrongPolicyInference() {
    return null;
  }

  /**
   * Returns whether this is a safe operator.
   *
   * <p>If an operator is safe, then it never causes a run-time exception.
   *
   * <p>For example, the {@code a/b} is not safe. Because the
   * {@code SqlStdOperatorTable.DIVIDE} may throw the exception when dividing by zero.
   *
   * <p>By default, returns {@code false}, which means the operator is not safe.
   *
   * <p>If an operator is safe, then some optimizations can be performed in
   * {@code org.apache.calcite.rex.RexSimplify}.
   *
   * <p>For example:
   * {@code NULL + a} can be optimized to {@code NULL}.
   *
   */
  public Boolean isSafeOperator() {
    return false;
  }

  /**
   * Returns whether this operator is monotonic.
   *
   * <p>Default implementation returns {@link SqlMonotonicity#NOT_MONOTONIC}.
   *
   * @param call  Call to this operator
   * @param scope Scope in which the call occurs
   *
   * @deprecated Use {@link #getMonotonicity(SqlOperatorBinding)}
   */
  @Deprecated // to be removed before 2.0
  public SqlMonotonicity getMonotonicity(
      SqlCall call,
      SqlValidatorScope scope) {
    return getMonotonicity(
        new SqlCallBinding(scope.getValidator(), scope, call));
  }

  /**
   * Returns whether a call to this operator is monotonic.
   *
   * <p>Default implementation returns {@link SqlMonotonicity#NOT_MONOTONIC}.
   *
   * @param call Call to this operator with particular arguments and information
   *             about the monotonicity of the arguments
   */
  public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }

  /**
   * Returns whether a call to this operator is guaranteed to always return
   * the same result given the same operands; true is assumed by default.
   */
  public boolean isDeterministic() {
    return true;
  }

  /**
   * Returns whether a call to this operator is not sensitive to the operands input order.
   * An operator is symmetrical if the call returns the same result when
   * the operands are shuffled.
   *
   * <p>By default, returns true for {@link SqlKind#SYMMETRICAL}.
   */
  public boolean isSymmetrical() {
    return SqlKind.SYMMETRICAL.contains(kind);
  }

  /**
   * Returns whether it is unsafe to cache query plans referencing this
   * operator; false is assumed by default.
   */
  public boolean isDynamicFunction() {
    return false;
  }

  /**
   * Method to check if call requires expansion when it has decimal operands.
   * The default implementation is to return true.
   */
  public boolean requiresDecimalExpansion() {
    return true;
  }

  /**
   * Returns whether the <code>ordinal</code>th argument to this operator must
   * be scalar (as opposed to a query).
   *
   * <p>If true (the default), the validator will attempt to convert the
   * argument into a scalar sub-query, which must have one column and return at
   * most one row.
   *
   * <p>Operators such as <code>SELECT</code> and <code>EXISTS</code> override
   * this method.
   */
  public boolean argumentMustBeScalar(int ordinal) {
    return true;
  }
}
