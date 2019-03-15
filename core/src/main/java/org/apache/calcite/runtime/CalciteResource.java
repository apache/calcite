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
package org.apache.calcite.runtime;

import org.apache.calcite.sql.validate.SqlValidatorException;

import static org.apache.calcite.runtime.Resources.BaseMessage;
import static org.apache.calcite.runtime.Resources.ExInst;
import static org.apache.calcite.runtime.Resources.ExInstWithCause;
import static org.apache.calcite.runtime.Resources.Inst;
import static org.apache.calcite.runtime.Resources.Property;

/**
 * Compiler-checked resources for the Calcite project.
 */
public interface CalciteResource {
  @BaseMessage("line {0,number,#}, column {1,number,#}")
  Inst parserContext(int a0, int a1);

  @BaseMessage("Bang equal ''!='' is not allowed under the current SQL conformance level")
  ExInst<CalciteException> bangEqualNotAllowed();

  @BaseMessage("Percent remainder ''%'' is not allowed under the current SQL conformance level")
  ExInst<CalciteException> percentRemainderNotAllowed();

  @BaseMessage("''LIMIT start, count'' is not allowed under the current SQL conformance level")
  ExInst<CalciteException> limitStartCountNotAllowed();

  @BaseMessage("APPLY operator is not allowed under the current SQL conformance level")
  ExInst<CalciteException> applyNotAllowed();

  @BaseMessage("Illegal {0} literal {1}: {2}")
  ExInst<CalciteException> illegalLiteral(String a0, String a1, String a2);

  @BaseMessage("Length of identifier ''{0}'' must be less than or equal to {1,number,#} characters")
  ExInst<CalciteException> identifierTooLong(String a0, int a1);

  @BaseMessage("not in format ''{0}''")
  Inst badFormat(String a0);

  @BaseMessage("BETWEEN operator has no terminating AND")
  ExInst<SqlValidatorException> betweenWithoutAnd();

  @BaseMessage("Geo-spatial extensions and the GEOMETRY data type are not enabled")
  ExInst<SqlValidatorException> geometryDisabled();

  @BaseMessage("Illegal INTERVAL literal {0}; at {1}")
  @Property(name = "SQLSTATE", value = "42000")
  ExInst<CalciteException> illegalIntervalLiteral(String a0, String a1);

  @BaseMessage("Illegal expression. Was expecting \"(DATETIME - DATETIME) INTERVALQUALIFIER\"")
  ExInst<CalciteException> illegalMinusDate();

  @BaseMessage("Illegal overlaps expression. Was expecting expression on the form \"(DATETIME, EXPRESSION) OVERLAPS (DATETIME, EXPRESSION)\"")
  ExInst<CalciteException> illegalOverlaps();

  @BaseMessage("Non-query expression encountered in illegal context")
  ExInst<CalciteException> illegalNonQueryExpression();

  @BaseMessage("Query expression encountered in illegal context")
  ExInst<CalciteException> illegalQueryExpression();

  @BaseMessage("CURSOR expression encountered in illegal context")
  ExInst<CalciteException> illegalCursorExpression();

  @BaseMessage("ORDER BY unexpected")
  ExInst<CalciteException> illegalOrderBy();

  @BaseMessage("Illegal binary string {0}")
  ExInst<CalciteException> illegalBinaryString(String a0);

  @BaseMessage("''FROM'' without operands preceding it is illegal")
  ExInst<CalciteException> illegalFromEmpty();

  @BaseMessage("ROW expression encountered in illegal context")
  ExInst<CalciteException> illegalRowExpression();

  @BaseMessage("Illegal identifier '':''. Was expecting ''VALUE''")
  ExInst<CalciteException> illegalColon();

  @BaseMessage("TABLESAMPLE percentage must be between 0 and 100, inclusive")
  @Property(name = "SQLSTATE", value = "2202H")
  ExInst<CalciteException> invalidSampleSize();

  @BaseMessage("Unknown character set ''{0}''")
  ExInst<CalciteException> unknownCharacterSet(String a0);

  @BaseMessage("Failed to encode ''{0}'' in character set ''{1}''")
  ExInst<CalciteException> charsetEncoding(String a0, String a1);

  @BaseMessage("UESCAPE ''{0}'' must be exactly one character")
  ExInst<CalciteException> unicodeEscapeCharLength(String a0);

  @BaseMessage("UESCAPE ''{0}'' may not be hex digit, whitespace, plus sign, or double quote")
  ExInst<CalciteException> unicodeEscapeCharIllegal(String a0);

  @BaseMessage("UESCAPE cannot be specified without Unicode literal introducer")
  ExInst<CalciteException> unicodeEscapeUnexpected();

  @BaseMessage("Unicode escape sequence starting at character {0,number,#} is not exactly four hex digits")
  ExInst<SqlValidatorException> unicodeEscapeMalformed(int a0);

  @BaseMessage("No match found for function signature {0}")
  ExInst<SqlValidatorException> validatorUnknownFunction(String a0);

  @BaseMessage("Invalid number of arguments to function ''{0}''. Was expecting {1,number,#} arguments")
  ExInst<SqlValidatorException> invalidArgCount(String a0, int a1);

  @BaseMessage("At line {0,number,#}, column {1,number,#}")
  ExInstWithCause<CalciteContextException> validatorContextPoint(int a0,
      int a1);

  @BaseMessage("From line {0,number,#}, column {1,number,#} to line {2,number,#}, column {3,number,#}")
  ExInstWithCause<CalciteContextException> validatorContext(int a0, int a1,
      int a2,
      int a3);

  @BaseMessage("Cast function cannot convert value of type {0} to type {1}")
  ExInst<SqlValidatorException> cannotCastValue(String a0, String a1);

  @BaseMessage("Unknown datatype name ''{0}''")
  ExInst<SqlValidatorException> unknownDatatypeName(String a0);

  @BaseMessage("Values passed to {0} operator must have compatible types")
  ExInst<SqlValidatorException> incompatibleValueType(String a0);

  @BaseMessage("Values in expression list must have compatible types")
  ExInst<SqlValidatorException> incompatibleTypesInList();

  @BaseMessage("Cannot apply {0} to the two different charsets {1} and {2}")
  ExInst<SqlValidatorException> incompatibleCharset(String a0, String a1,
      String a2);

  @BaseMessage("ORDER BY is only allowed on top-level SELECT")
  ExInst<SqlValidatorException> invalidOrderByPos();

  @BaseMessage("Unknown identifier ''{0}''")
  ExInst<SqlValidatorException> unknownIdentifier(String a0);

  @BaseMessage("Unknown field ''{0}''")
  ExInst<SqlValidatorException> unknownField(String a0);

  @BaseMessage("Unknown target column ''{0}''")
  ExInst<SqlValidatorException> unknownTargetColumn(String a0);

  @BaseMessage("Target column ''{0}'' is assigned more than once")
  ExInst<SqlValidatorException> duplicateTargetColumn(String a0);

  @BaseMessage("Number of INSERT target columns ({0,number}) does not equal number of source items ({1,number})")
  ExInst<SqlValidatorException> unmatchInsertColumn(int a0, int a1);

  @BaseMessage("Column ''{0}'' has no default value and does not allow NULLs")
  ExInst<SqlValidatorException> columnNotNullable(String a0);

  @BaseMessage("Cannot assign to target field ''{0}'' of type {1} from source field ''{2}'' of type {3}")
  ExInst<SqlValidatorException> typeNotAssignable(String a0, String a1,
      String a2, String a3);

  @BaseMessage("Table ''{0}'' not found")
  ExInst<SqlValidatorException> tableNameNotFound(String a0);

  @BaseMessage("Table ''{0}'' not found; did you mean ''{1}''?")
  ExInst<SqlValidatorException> tableNameNotFoundDidYouMean(String a0,
      String a1);

  /** Same message as {@link #tableNameNotFound(String)} but a different kind
   * of exception, so it can be used in {@code RelBuilder}. */
  @BaseMessage("Table ''{0}'' not found")
  ExInst<CalciteException> tableNotFound(String tableName);

  @BaseMessage("Object ''{0}'' not found")
  ExInst<SqlValidatorException> objectNotFound(String a0);

  @BaseMessage("Object ''{0}'' not found within ''{1}''")
  ExInst<SqlValidatorException> objectNotFoundWithin(String a0, String a1);

  @BaseMessage("Object ''{0}'' not found; did you mean ''{1}''?")
  ExInst<SqlValidatorException> objectNotFoundDidYouMean(String a0, String a1);

  @BaseMessage("Object ''{0}'' not found within ''{1}''; did you mean ''{2}''?")
  ExInst<SqlValidatorException> objectNotFoundWithinDidYouMean(String a0,
      String a1, String a2);

  @BaseMessage("Table ''{0}'' is not a sequence")
  ExInst<SqlValidatorException> notASequence(String a0);

  @BaseMessage("Column ''{0}'' not found in any table")
  ExInst<SqlValidatorException> columnNotFound(String a0);

  @BaseMessage("Column ''{0}'' not found in any table; did you mean ''{1}''?")
  ExInst<SqlValidatorException> columnNotFoundDidYouMean(String a0, String a1);

  @BaseMessage("Column ''{0}'' not found in table ''{1}''")
  ExInst<SqlValidatorException> columnNotFoundInTable(String a0, String a1);

  @BaseMessage("Column ''{0}'' not found in table ''{1}''; did you mean ''{2}''?")
  ExInst<SqlValidatorException> columnNotFoundInTableDidYouMean(String a0,
      String a1, String a2);

  @BaseMessage("Column ''{0}'' is ambiguous")
  ExInst<SqlValidatorException> columnAmbiguous(String a0);

  @BaseMessage("Operand {0} must be a query")
  ExInst<SqlValidatorException> needQueryOp(String a0);

  @BaseMessage("Parameters must be of the same type")
  ExInst<SqlValidatorException> needSameTypeParameter();

  @BaseMessage("Cannot apply ''{0}'' to arguments of type {1}. Supported form(s): {2}")
  ExInst<SqlValidatorException> canNotApplyOp2Type(String a0, String a1,
      String a2);

  @BaseMessage("Expected a boolean type")
  ExInst<SqlValidatorException> expectedBoolean();

  @BaseMessage("Expected a character type")
  ExInst<SqlValidatorException> expectedCharacter();

  @BaseMessage("ELSE clause or at least one THEN clause must be non-NULL")
  ExInst<SqlValidatorException> mustNotNullInElse();

  @BaseMessage("Function ''{0}'' is not defined")
  ExInst<SqlValidatorException> functionUndefined(String a0);

  @BaseMessage("Encountered {0} with {1,number} parameter(s); was expecting {2}")
  ExInst<SqlValidatorException> wrongNumberOfParam(String a0, int a1,
      String a2);

  @BaseMessage("Illegal mixing of types in CASE or COALESCE statement")
  ExInst<SqlValidatorException> illegalMixingOfTypes();

  @BaseMessage("Invalid compare. Comparing (collation, coercibility): ({0}, {1} with ({2}, {3}) is illegal")
  ExInst<CalciteException> invalidCompare(String a0, String a1, String a2,
      String a3);

  @BaseMessage("Invalid syntax. Two explicit different collations ({0}, {1}) are illegal")
  ExInst<CalciteException> differentCollations(String a0, String a1);

  @BaseMessage("{0} is not comparable to {1}")
  ExInst<SqlValidatorException> typeNotComparable(String a0, String a1);

  @BaseMessage("Cannot compare values of types ''{0}'', ''{1}''")
  ExInst<SqlValidatorException> typeNotComparableNear(String a0, String a1);

  @BaseMessage("Wrong number of arguments to expression")
  ExInst<SqlValidatorException> wrongNumOfArguments();

  @BaseMessage("Operands {0} not comparable to each other")
  ExInst<SqlValidatorException> operandNotComparable(String a0);

  @BaseMessage("Types {0} not comparable to each other")
  ExInst<SqlValidatorException> typeNotComparableEachOther(String a0);

  @BaseMessage("Numeric literal ''{0}'' out of range")
  ExInst<SqlValidatorException> numberLiteralOutOfRange(String a0);

  @BaseMessage("Date literal ''{0}'' out of range")
  ExInst<SqlValidatorException> dateLiteralOutOfRange(String a0);

  @BaseMessage("String literal continued on same line")
  ExInst<SqlValidatorException> stringFragsOnSameLine();

  @BaseMessage("Table or column alias must be a simple identifier")
  ExInst<SqlValidatorException> aliasMustBeSimpleIdentifier();

  @BaseMessage("List of column aliases must have same degree as table; table has {0,number,#} columns {1}, whereas alias list has {2,number,#} columns")
  ExInst<SqlValidatorException> aliasListDegree(int a0, String a1, int a2);

  @BaseMessage("Duplicate name ''{0}'' in column alias list")
  ExInst<SqlValidatorException> aliasListDuplicate(String a0);

  @BaseMessage("INNER, LEFT, RIGHT or FULL join requires a condition (NATURAL keyword or ON or USING clause)")
  ExInst<SqlValidatorException> joinRequiresCondition();

  @BaseMessage("Cannot specify condition (NATURAL keyword, or ON or USING clause) following CROSS JOIN")
  ExInst<SqlValidatorException> crossJoinDisallowsCondition();

  @BaseMessage("Cannot specify NATURAL keyword with ON or USING clause")
  ExInst<SqlValidatorException> naturalDisallowsOnOrUsing();

  @BaseMessage("Column name ''{0}'' in USING clause is not unique on one side of join")
  ExInst<SqlValidatorException> columnInUsingNotUnique(String a0);

  @BaseMessage("Column ''{0}'' matched using NATURAL keyword or USING clause has incompatible types: cannot compare ''{1}'' to ''{2}''")
  ExInst<SqlValidatorException> naturalOrUsingColumnNotCompatible(String a0,
      String a1, String a2);

  @BaseMessage("OVER clause is necessary for window functions")
  ExInst<SqlValidatorException> absentOverClause();

  @BaseMessage("Window ''{0}'' not found")
  ExInst<SqlValidatorException> windowNotFound(String a0);

  @BaseMessage("Expression ''{0}'' is not being grouped")
  ExInst<SqlValidatorException> notGroupExpr(String a0);

  @BaseMessage("Argument to {0} operator must be a grouped expression")
  ExInst<SqlValidatorException> groupingArgument(String a0);

  @BaseMessage("{0} operator may only occur in an aggregate query")
  ExInst<SqlValidatorException> groupingInAggregate(String a0);

  @BaseMessage("{0} operator may only occur in SELECT, HAVING or ORDER BY clause")
  ExInst<SqlValidatorException> groupingInWrongClause(String a0);

  @BaseMessage("Expression ''{0}'' is not in the select clause")
  ExInst<SqlValidatorException> notSelectDistinctExpr(String a0);

  @BaseMessage("Aggregate expression is illegal in {0} clause")
  ExInst<SqlValidatorException> aggregateIllegalInClause(String a0);

  @BaseMessage("Windowed aggregate expression is illegal in {0} clause")
  ExInst<SqlValidatorException> windowedAggregateIllegalInClause(String a0);

  @BaseMessage("Aggregate expressions cannot be nested")
  ExInst<SqlValidatorException> nestedAggIllegal();

  @BaseMessage("FILTER must not contain aggregate expression")
  ExInst<SqlValidatorException> aggregateInFilterIllegal();

  @BaseMessage("WITHIN GROUP must not contain aggregate expression")
  ExInst<SqlValidatorException> aggregateInWithinGroupIllegal();

  @BaseMessage("Aggregate expression ''{0}'' must contain a within group clause")
  ExInst<SqlValidatorException> aggregateMissingWithinGroupClause(String a0);

  @BaseMessage("Aggregate expression ''{0}'' must not contain a within group clause")
  ExInst<SqlValidatorException> withinGroupClauseIllegalInAggregate(String a0);

  @BaseMessage("Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT")
  ExInst<SqlValidatorException> aggregateIllegalInOrderBy();

  @BaseMessage("{0} clause must be a condition")
  ExInst<SqlValidatorException> condMustBeBoolean(String a0);

  @BaseMessage("HAVING clause must be a condition")
  ExInst<SqlValidatorException> havingMustBeBoolean();

  @BaseMessage("OVER must be applied to aggregate function")
  ExInst<SqlValidatorException> overNonAggregate();

  @BaseMessage("FILTER must be applied to aggregate function")
  ExInst<SqlValidatorException> filterNonAggregate();

  @BaseMessage("Cannot override window attribute")
  ExInst<SqlValidatorException> cannotOverrideWindowAttribute();

  @BaseMessage("Column count mismatch in {0}")
  ExInst<SqlValidatorException> columnCountMismatchInSetop(String a0);

  @BaseMessage("Type mismatch in column {0,number} of {1}")
  ExInst<SqlValidatorException> columnTypeMismatchInSetop(int a0, String a1);

  @BaseMessage("Binary literal string must contain an even number of hexits")
  ExInst<SqlValidatorException> binaryLiteralOdd();

  @BaseMessage("Binary literal string must contain only characters ''0'' - ''9'', ''A'' - ''F''")
  ExInst<SqlValidatorException> binaryLiteralInvalid();

  @BaseMessage("Illegal interval literal format {0} for {1}")
  ExInst<SqlValidatorException> unsupportedIntervalLiteral(String a0,
      String a1);

  @BaseMessage("Interval field value {0,number} exceeds precision of {1} field")
  ExInst<SqlValidatorException> intervalFieldExceedsPrecision(Number a0,
      String a1);

  @BaseMessage("RANGE clause cannot be used with compound ORDER BY clause")
  ExInst<SqlValidatorException> compoundOrderByProhibitsRange();

  @BaseMessage("Data type of ORDER BY prohibits use of RANGE clause")
  ExInst<SqlValidatorException> orderByDataTypeProhibitsRange();

  @BaseMessage("Data Type mismatch between ORDER BY and RANGE clause")
  ExInst<SqlValidatorException> orderByRangeMismatch();

  @BaseMessage("Window ORDER BY expression of type DATE requires range of type INTERVAL")
  ExInst<SqlValidatorException> dateRequiresInterval();

  @BaseMessage("ROWS value must be a non-negative integral constant")
  ExInst<SqlValidatorException> rowMustBeNonNegativeIntegral();

  @BaseMessage("Window specification must contain an ORDER BY clause")
  ExInst<SqlValidatorException> overMissingOrderBy();

  @BaseMessage("PARTITION BY expression should not contain OVER clause")
  ExInst<SqlValidatorException> partitionbyShouldNotContainOver();

  @BaseMessage("ORDER BY expression should not contain OVER clause")
  ExInst<SqlValidatorException> orderbyShouldNotContainOver();

  @BaseMessage("UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary")
  ExInst<SqlValidatorException> badLowerBoundary();

  @BaseMessage("UNBOUNDED PRECEDING cannot be specified for the upper frame boundary")
  ExInst<SqlValidatorException> badUpperBoundary();

  @BaseMessage("Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW")
  ExInst<SqlValidatorException> currentRowPrecedingError();

  @BaseMessage("Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING")
  ExInst<SqlValidatorException> currentRowFollowingError();

  @BaseMessage("Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING")
  ExInst<SqlValidatorException> followingBeforePrecedingError();

  @BaseMessage("Window name must be a simple identifier")
  ExInst<SqlValidatorException> windowNameMustBeSimple();

  @BaseMessage("Duplicate window names not allowed")
  ExInst<SqlValidatorException> duplicateWindowName();

  @BaseMessage("Empty window specification not allowed")
  ExInst<SqlValidatorException> emptyWindowSpec();

  @BaseMessage("Duplicate window specification not allowed in the same window clause")
  ExInst<SqlValidatorException> dupWindowSpec();

  @BaseMessage("ROW/RANGE not allowed with RANK, DENSE_RANK or ROW_NUMBER functions")
  ExInst<SqlValidatorException> rankWithFrame();

  @BaseMessage("RANK or DENSE_RANK functions require ORDER BY clause in window specification")
  ExInst<SqlValidatorException> funcNeedsOrderBy();

  @BaseMessage("PARTITION BY not allowed with existing window reference")
  ExInst<SqlValidatorException> partitionNotAllowed();

  @BaseMessage("ORDER BY not allowed in both base and referenced windows")
  ExInst<SqlValidatorException> orderByOverlap();

  @BaseMessage("Referenced window cannot have framing declarations")
  ExInst<SqlValidatorException> refWindowWithFrame();

  @BaseMessage("Type ''{0}'' is not supported")
  ExInst<SqlValidatorException> typeNotSupported(String a0);

  @BaseMessage("DISTINCT/ALL not allowed with {0} function")
  ExInst<SqlValidatorException> functionQuantifierNotAllowed(String a0);

  @BaseMessage("WITHIN GROUP not allowed with {0} function")
  ExInst<SqlValidatorException> withinGroupNotAllowed(String a0);

  @BaseMessage("Some but not all arguments are named")
  ExInst<SqlValidatorException> someButNotAllArgumentsAreNamed();

  @BaseMessage("Duplicate argument name ''{0}''")
  ExInst<SqlValidatorException> duplicateArgumentName(String name);

  @BaseMessage("DEFAULT is only allowed for optional parameters")
  ExInst<SqlValidatorException> defaultForOptionalParameter();

  @BaseMessage("DEFAULT not allowed here")
  ExInst<SqlValidatorException> defaultNotAllowed();

  @BaseMessage("Not allowed to perform {0} on {1}")
  ExInst<SqlValidatorException> accessNotAllowed(String a0, String a1);

  @BaseMessage("The {0} function does not support the {1} data type.")
  ExInst<SqlValidatorException> minMaxBadType(String a0, String a1);

  @BaseMessage("Only scalar sub-queries allowed in select list.")
  ExInst<SqlValidatorException> onlyScalarSubQueryAllowed();

  @BaseMessage("Ordinal out of range")
  ExInst<SqlValidatorException> orderByOrdinalOutOfRange();

  @BaseMessage("Window has negative size")
  ExInst<SqlValidatorException> windowHasNegativeSize();

  @BaseMessage("UNBOUNDED FOLLOWING window not supported")
  ExInst<SqlValidatorException> unboundedFollowingWindowNotSupported();

  @BaseMessage("Cannot use DISALLOW PARTIAL with window based on RANGE")
  ExInst<SqlValidatorException> cannotUseDisallowPartialWithRange();

  @BaseMessage("Interval leading field precision ''{0,number,#}'' out of range for {1}")
  ExInst<SqlValidatorException> intervalStartPrecisionOutOfRange(int a0,
      String a1);

  @BaseMessage("Interval fractional second precision ''{0,number,#}'' out of range for {1}")
  ExInst<SqlValidatorException> intervalFractionalSecondPrecisionOutOfRange(
      int a0, String a1);

  @BaseMessage("Duplicate relation name ''{0}'' in FROM clause")
  ExInst<SqlValidatorException> fromAliasDuplicate(String a0);

  @BaseMessage("Duplicate column name ''{0}'' in output")
  ExInst<SqlValidatorException> duplicateColumnName(String a0);

  @BaseMessage("Duplicate name ''{0}'' in column list")
  ExInst<SqlValidatorException> duplicateNameInColumnList(String a0);

  @BaseMessage("Internal error: {0}")
  ExInst<CalciteException> internal(String a0);

  @BaseMessage("Argument to function ''{0}'' must be a literal")
  ExInst<SqlValidatorException> argumentMustBeLiteral(String a0);

  @BaseMessage("Argument to function ''{0}'' must be a positive integer literal")
  ExInst<SqlValidatorException> argumentMustBePositiveInteger(String a0);

  @BaseMessage("Validation Error: {0}")
  ExInst<CalciteException> validationError(String a0);

  @BaseMessage("Locale ''{0}'' in an illegal format")
  ExInst<CalciteException> illegalLocaleFormat(String a0);

  @BaseMessage("Argument to function ''{0}'' must not be NULL")
  ExInst<SqlValidatorException> argumentMustNotBeNull(String a0);

  @BaseMessage("Illegal use of ''NULL''")
  ExInst<SqlValidatorException> nullIllegal();

  @BaseMessage("Illegal use of dynamic parameter")
  ExInst<SqlValidatorException> dynamicParamIllegal();

  @BaseMessage("''{0}'' is not a valid boolean value")
  ExInst<CalciteException> invalidBoolean(String a0);

  @BaseMessage("Argument to function ''{0}'' must be a valid precision between ''{1,number,#}'' and ''{2,number,#}''")
  ExInst<SqlValidatorException> argumentMustBeValidPrecision(String a0, int a1,
      int a2);

  @BaseMessage("Wrong arguments for table function ''{0}'' call. Expected ''{1}'', actual ''{2}''")
  ExInst<CalciteException> illegalArgumentForTableFunctionCall(String a0,
      String a1, String a2);

  @BaseMessage("''{0}'' is not a valid datetime format")
  ExInst<CalciteException> invalidDatetimeFormat(String a0);

  @BaseMessage("Cannot INSERT into generated column ''{0}''")
  ExInst<SqlValidatorException> insertIntoAlwaysGenerated(String a0);

  @BaseMessage("Argument to function ''{0}'' must have a scale of 0")
  ExInst<CalciteException> argumentMustHaveScaleZero(String a0);

  @BaseMessage("Statement preparation aborted")
  ExInst<CalciteException> preparationAborted();

  @BaseMessage("SELECT DISTINCT not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  Feature sQLFeature_E051_01();

  @BaseMessage("EXCEPT not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  Feature sQLFeature_E071_03();

  @BaseMessage("UPDATE not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  Feature sQLFeature_E101_03();

  @BaseMessage("Transactions not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  Feature sQLFeature_E151();

  @BaseMessage("INTERSECT not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  Feature sQLFeature_F302();

  @BaseMessage("MERGE not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  Feature sQLFeature_F312();

  @BaseMessage("Basic multiset not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  Feature sQLFeature_S271();

  @BaseMessage("TABLESAMPLE not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  Feature sQLFeature_T613();

  @BaseMessage("Execution of a new autocommit statement while a cursor is still open on same connection is not supported")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  ExInst<CalciteException> sQLConformance_MultipleActiveAutocommitStatements();

  @BaseMessage("Descending sort (ORDER BY DESC) not supported")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  Feature sQLConformance_OrderByDesc();

  @BaseMessage("Sharing of cached statement plans not supported")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  ExInst<CalciteException> sharedStatementPlans();

  @BaseMessage("TABLESAMPLE SUBSTITUTE not supported")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  Feature sQLFeatureExt_T613_Substitution();

  @BaseMessage("Personality does not maintain table''s row count in the catalog")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  ExInst<CalciteException> personalityManagesRowCount();

  @BaseMessage("Personality does not support snapshot reads")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  ExInst<CalciteException> personalitySupportsSnapshots();

  @BaseMessage("Personality does not support labels")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  ExInst<CalciteException> personalitySupportsLabels();

  @BaseMessage("Require at least 1 argument")
  ExInst<SqlValidatorException> requireAtLeastOneArg();

  @BaseMessage("Map requires at least 2 arguments")
  ExInst<SqlValidatorException> mapRequiresTwoOrMoreArgs();

  @BaseMessage("Map requires an even number of arguments")
  ExInst<SqlValidatorException> mapRequiresEvenArgCount();

  @BaseMessage("Incompatible types")
  ExInst<SqlValidatorException> incompatibleTypes();

  @BaseMessage("Number of columns must match number of query columns")
  ExInst<SqlValidatorException> columnCountMismatch();

  @BaseMessage("Column has duplicate column name ''{0}'' and no column list specified")
  ExInst<SqlValidatorException> duplicateColumnAndNoColumnList(String s);

  @BaseMessage("Declaring class ''{0}'' of non-static user-defined function must have a public constructor with zero parameters")
  ExInst<RuntimeException> requireDefaultConstructor(String className);

  @BaseMessage("In user-defined aggregate class ''{0}'', first parameter to ''add'' method must be the accumulator (the return type of the ''init'' method)")
  ExInst<RuntimeException> firstParameterOfAdd(String className);

  @BaseMessage("FilterableTable.scan returned a filter that was not in the original list: {0}")
  ExInst<CalciteException> filterableTableInventedFilter(String s);

  @BaseMessage("FilterableTable.scan must not return null")
  ExInst<CalciteException> filterableTableScanReturnedNull();

  @BaseMessage("Cannot convert table ''{0}'' to stream")
  ExInst<SqlValidatorException> cannotConvertToStream(String tableName);

  @BaseMessage("Cannot convert stream ''{0}'' to relation")
  ExInst<SqlValidatorException> cannotConvertToRelation(String tableName);

  @BaseMessage("Streaming aggregation requires at least one monotonic expression in GROUP BY clause")
  ExInst<SqlValidatorException> streamMustGroupByMonotonic();

  @BaseMessage("Streaming ORDER BY must start with monotonic expression")
  ExInst<SqlValidatorException> streamMustOrderByMonotonic();

  @BaseMessage("Set operator cannot combine streaming and non-streaming inputs")
  ExInst<SqlValidatorException> streamSetOpInconsistentInputs();

  @BaseMessage("Cannot stream VALUES")
  ExInst<SqlValidatorException> cannotStreamValues();

  @BaseMessage("Cannot resolve ''{0}''; it references view ''{1}'', whose definition is cyclic")
  ExInst<SqlValidatorException> cyclicDefinition(String id, String view);

  @BaseMessage("Modifiable view must be based on a single table")
  ExInst<SqlValidatorException> modifiableViewMustBeBasedOnSingleTable();

  @BaseMessage("Modifiable view must be predicated only on equality expressions")
  ExInst<SqlValidatorException> modifiableViewMustHaveOnlyEqualityPredicates();

  @BaseMessage("View is not modifiable. More than one expression maps to column ''{0}'' of base table ''{1}''")
  ExInst<SqlValidatorException> moreThanOneMappedColumn(String columnName, String tableName);

  @BaseMessage("View is not modifiable. No value is supplied for NOT NULL column ''{0}'' of base table ''{1}''")
  ExInst<SqlValidatorException> noValueSuppliedForViewColumn(String columnName, String tableName);

  @BaseMessage("Modifiable view constraint is not satisfied for column ''{0}'' of base table ''{1}''")
  ExInst<SqlValidatorException> viewConstraintNotSatisfied(String columnName, String tableName);

  @BaseMessage("Not a record type. The ''*'' operator requires a record")
  ExInst<SqlValidatorException> starRequiresRecordType();

  @BaseMessage("FILTER expression must be of type BOOLEAN")
  ExInst<CalciteException> filterMustBeBoolean();

  @BaseMessage("Cannot stream results of a query with no streaming inputs: ''{0}''. At least one input should be convertible to a stream")
  ExInst<SqlValidatorException> cannotStreamResultsForNonStreamingInputs(String inputs);

  @BaseMessage("MINUS is not allowed under the current SQL conformance level")
  ExInst<CalciteException> minusNotAllowed();

  @BaseMessage("SELECT must have a FROM clause")
  ExInst<SqlValidatorException> selectMissingFrom();

  @BaseMessage("Group function ''{0}'' can only appear in GROUP BY clause")
  ExInst<SqlValidatorException> groupFunctionMustAppearInGroupByClause(String funcName);

  @BaseMessage("Call to auxiliary group function ''{0}'' must have matching call to group function ''{1}'' in GROUP BY clause")
  ExInst<SqlValidatorException> auxiliaryWithoutMatchingGroupCall(String func1, String func2);

  @BaseMessage("Pattern variable ''{0}'' has already been defined")
  ExInst<SqlValidatorException> patternVarAlreadyDefined(String varName);

  @BaseMessage("Cannot use PREV/NEXT in MEASURE ''{0}''")
  ExInst<SqlValidatorException> patternPrevFunctionInMeasure(String call);

  @BaseMessage("Cannot nest PREV/NEXT under LAST/FIRST ''{0}''")
  ExInst<SqlValidatorException> patternPrevFunctionOrder(String call);

  @BaseMessage("Cannot use aggregation in navigation ''{0}''")
  ExInst<SqlValidatorException> patternAggregationInNavigation(String call);

  @BaseMessage("Invalid number of parameters to COUNT method")
  ExInst<SqlValidatorException> patternCountFunctionArg();

  @BaseMessage("The system time period specification expects Timestamp type but is ''{0}''")
  ExInst<SqlValidatorException> illegalExpressionForTemporal(String type);

  @BaseMessage("Table ''{0}'' is not a temporal table, can not be queried in system time period specification")
  ExInst<SqlValidatorException> notTemporalTable(String tableName);

  @BaseMessage("Cannot use RUNNING/FINAL in DEFINE ''{0}''")
  ExInst<SqlValidatorException> patternRunningFunctionInDefine(String call);

  @BaseMessage("Multiple pattern variables in ''{0}''")
  ExInst<SqlValidatorException> patternFunctionVariableCheck(String call);

  @BaseMessage("Function ''{0}'' can only be used in MATCH_RECOGNIZE")
  ExInst<SqlValidatorException> functionMatchRecognizeOnly(String call);

  @BaseMessage("Null parameters in ''{0}''")
  ExInst<SqlValidatorException> patternFunctionNullCheck(String call);

  @BaseMessage("Unknown pattern ''{0}''")
  ExInst<SqlValidatorException> unknownPattern(String call);

  @BaseMessage("Interval must be non-negative ''{0}''")
  ExInst<SqlValidatorException> intervalMustBeNonNegative(String call);

  @BaseMessage("Must contain an ORDER BY clause when WITHIN is used")
  ExInst<SqlValidatorException> cannotUseWithinWithoutOrderBy();

  @BaseMessage("First column of ORDER BY must be of type TIMESTAMP")
  ExInst<SqlValidatorException> firstColumnOfOrderByMustBeTimestamp();

  @BaseMessage("Extended columns not allowed under the current SQL conformance level")
  ExInst<SqlValidatorException> extendNotAllowed();

  @BaseMessage("Rolled up column ''{0}'' is not allowed in {1}")
  ExInst<SqlValidatorException> rolledUpNotAllowed(String column, String context);

  @BaseMessage("Schema ''{0}'' already exists")
  ExInst<SqlValidatorException> schemaExists(String name);

  @BaseMessage("Invalid schema type ''{0}''; valid values: {1}")
  ExInst<SqlValidatorException> schemaInvalidType(String type, String values);

  @BaseMessage("Table ''{0}'' already exists")
  ExInst<SqlValidatorException> tableExists(String name);

  // If CREATE TABLE does not have "AS query", there must be a column list
  @BaseMessage("Missing column list")
  ExInst<SqlValidatorException> createTableRequiresColumnList();

  // If CREATE TABLE does not have "AS query", a type must be specified for each
  // column
  @BaseMessage("Type required for column ''{0}'' in CREATE TABLE without AS")
  ExInst<SqlValidatorException> createTableRequiresColumnTypes(String columnName);

  @BaseMessage("View ''{0}'' already exists and REPLACE not specified")
  ExInst<SqlValidatorException> viewExists(String name);

  @BaseMessage("Schema ''{0}'' not found")
  ExInst<SqlValidatorException> schemaNotFound(String name);

  @BaseMessage("View ''{0}'' not found")
  ExInst<SqlValidatorException> viewNotFound(String name);

  @BaseMessage("Type ''{0}'' not found")
  ExInst<SqlValidatorException> typeNotFound(String name);

  @BaseMessage("Dialect does not support feature: ''{0}''")
  ExInst<SqlValidatorException> dialectDoesNotSupportFeature(String featureName);

  @BaseMessage("Substring error: negative substring length not allowed")
  ExInst<CalciteException> illegalNegativeSubstringLength();

  @BaseMessage("Trim error: trim character must be exactly 1 character")
  ExInst<CalciteException> trimError();

  @BaseMessage("Invalid types for arithmetic: {0} {1} {2}")
  ExInst<CalciteException> invalidTypesForArithmetic(String clazzName0, String op,
      String clazzName1);

  @BaseMessage("Invalid types for comparison: {0} {1} {2}")
  ExInst<CalciteException> invalidTypesForComparison(String clazzName0, String op,
      String clazzName1);

  @BaseMessage("Cannot convert {0} to {1}")
  ExInst<CalciteException> cannotConvert(String o, String toType);

  @BaseMessage("Invalid character for cast: {0}")
  ExInst<CalciteException> invalidCharacterForCast(String s);

  @BaseMessage("More than one value in list: {0}")
  ExInst<CalciteException> moreThanOneValueInList(String list);

  @BaseMessage("Failed to access field ''{0}'' of object of type {1}")
  ExInstWithCause<CalciteException> failedToAccessField(String fieldName, String typeName);

  @BaseMessage("Illegal jsonpath spec ''{0}'', format of the spec should be: ''<lax|strict> $'{'expr'}'''")
  ExInst<CalciteException> illegalJsonPathSpec(String pathSpec);

  @BaseMessage("Illegal jsonpath mode ''{0}''")
  ExInst<CalciteException> illegalJsonPathMode(String pathMode);

  @BaseMessage("Illegal jsonpath mode ''{0}'' in jsonpath spec: ''{1}''")
  ExInst<CalciteException> illegalJsonPathModeInPathSpec(String pathMode, String pathSpec);

  @BaseMessage("Strict jsonpath mode requires a non empty returned value, but is null")
  ExInst<CalciteException> strictPathModeRequiresNonEmptyValue();

  @BaseMessage("Illegal error behavior ''{0}'' specified in JSON_EXISTS function")
  ExInst<CalciteException> illegalErrorBehaviorInJsonExistsFunc(String errorBehavior);

  @BaseMessage("Empty result of JSON_VALUE function is not allowed")
  ExInst<CalciteException> emptyResultOfJsonValueFuncNotAllowed();

  @BaseMessage("Illegal empty behavior ''{0}'' specified in JSON_VALUE function")
  ExInst<CalciteException> illegalEmptyBehaviorInJsonValueFunc(String emptyBehavior);

  @BaseMessage("Illegal error behavior ''{0}'' specified in JSON_VALUE function")
  ExInst<CalciteException> illegalErrorBehaviorInJsonValueFunc(String errorBehavior);

  @BaseMessage("Strict jsonpath mode requires scalar value, and the actual value is: ''{0}''")
  ExInst<CalciteException> scalarValueRequiredInStrictModeOfJsonValueFunc(String value);

  @BaseMessage("Illegal wrapper behavior ''{0}'' specified in JSON_QUERY function")
  ExInst<CalciteException> illegalWrapperBehaviorInJsonQueryFunc(String wrapperBehavior);

  @BaseMessage("Empty result of JSON_QUERY function is not allowed")
  ExInst<CalciteException> emptyResultOfJsonQueryFuncNotAllowed();

  @BaseMessage("Illegal empty behavior ''{0}'' specified in JSON_VALUE function")
  ExInst<CalciteException> illegalEmptyBehaviorInJsonQueryFunc(String emptyBehavior);

  @BaseMessage("Strict jsonpath mode requires array or object value, and the actual value is: ''{0}''")
  ExInst<CalciteException> arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc(String value);

  @BaseMessage("Illegal error behavior ''{0}'' specified in JSON_VALUE function")
  ExInst<CalciteException> illegalErrorBehaviorInJsonQueryFunc(String errorBehavior);

  @BaseMessage("Null key of JSON object is not allowed")
  ExInst<CalciteException> nullKeyOfJsonObjectNotAllowed();

  @BaseMessage("Timeout of ''{0}'' ms for query execution is reached. Query execution started at ''{1}''")
  ExInst<CalciteException> queryExecutionTimeoutReached(String timeout, String queryStart);

  @BaseMessage("Including both WITHIN GROUP(...) and inside ORDER BY in a single JSON_ARRAYAGG call is not allowed")
  ExInst<CalciteException> ambiguousSortOrderInJsonArrayAggFunc();

  @BaseMessage("While executing SQL [{0}] on JDBC sub-schema")
  ExInst<RuntimeException> exceptionWhilePerformingQueryOnJdbcSubSchema(String sql);

  @BaseMessage("Unknown JSON type in JSON_TYPE function, and the object is: ''{0}''")
  ExInst<CalciteException> unknownObjectOfJsonType(String value);

  @BaseMessage("Unknown JSON depth in JSON_DEPTH function, and the object is: ''{0}''")
  ExInst<CalciteException> unknownObjectOfJsonDepth(String value);

  @BaseMessage("Cannot serialize object to JSON, and the object is: ''{0}''")
  ExInst<CalciteException> exceptionWhileSerializingToJson(String value);

  @BaseMessage("Omit quotes can be only applied on scalar strings, and the object is: ''{0}''")
  ExInst<CalciteException> omitQuotesCanBeOnlyAppliedOnScalarStrings(String value);
}

// End CalciteResource.java
