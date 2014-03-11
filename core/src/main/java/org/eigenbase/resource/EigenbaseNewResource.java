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
package org.eigenbase.resource;

import org.eigenbase.sql.validate.SqlValidatorException;
import org.eigenbase.util.EigenbaseContextException;
import org.eigenbase.util.EigenbaseException;

import static org.eigenbase.resource.Resources.*;

/**
 * Compiler-checked resources for the Eigenbase project.
 */
public interface EigenbaseNewResource {
  @BaseMessage("line {0,number,#}, column {1,number,#}")
  Inst parserContext(int a0, int a1);

  @BaseMessage("Illegal {0} literal {1}: {2}")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalLiteral(String a0, String a1, String a2);

  @BaseMessage("Length of identifier ''{0}'' must be less than or equal to {1,number,#} characters")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> identifierTooLong(String a0, int a1);

  @BaseMessage("not in format ''{0}''")
  Inst badFormat(String a0);

  @BaseMessage("BETWEEN operator has no terminating AND")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> betweenWithoutAnd();

  @BaseMessage("Illegal INTERVAL literal {0}; at {1}")
  @Property(name = "SQLSTATE", value = "42000")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalIntervalLiteral(String a0, String a1);

  @BaseMessage("Illegal expression. Was expecting \"(DATETIME - DATETIME) INTERVALQUALIFIER\"")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalMinusDate();

  @BaseMessage("Illegal overlaps expression. Was expecting expression on the form \"(DATETIME, EXPRESSION) OVERLAPS (DATETIME, EXPRESSION)\"")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalOverlaps();

  @BaseMessage("Non-query expression encountered in illegal context")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalNonQueryExpression();

  @BaseMessage("Query expression encountered in illegal context")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalQueryExpression();

  @BaseMessage("CURSOR expression encountered in illegal context")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalCursorExpression();

  @BaseMessage("ORDER BY unexpected")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalOrderBy();

  @BaseMessage("Illegal binary string {0}")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalBinaryString(String a0);

  @BaseMessage("''FROM'' without operands preceding it is illegal")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalFromEmpty();

  @BaseMessage("ROW expression encountered in illegal context")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalRowExpression();

  @BaseMessage("TABLESAMPLE percentage must be between 0 and 100, inclusive")
  @Property(name = "SQLSTATE", value = "2202H")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> invalidSampleSize();

  @BaseMessage("Unknown character set ''{0}''")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> unknownCharacterSet(String a0);

  @BaseMessage("Failed to encode ''{0}'' in character set ''{1}''")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> charsetEncoding(String a0, String a1);

  @BaseMessage("UESCAPE ''{0}'' must be exactly one character")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> unicodeEscapeCharLength(String a0);

  @BaseMessage("UESCAPE ''{0}'' may not be hex digit, whitespace, plus sign, or double quote")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> unicodeEscapeCharIllegal(String a0);

  @BaseMessage("UESCAPE cannot be specified without Unicode literal introducer")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> unicodeEscapeUnexpected();

  @BaseMessage("Unicode escape sequence starting at character {0,number,#} is not exactly four hex digits")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> unicodeEscapeMalformed(int a0);

  @BaseMessage("No match found for function signature {0}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> validatorUnknownFunction(String a0);

  @BaseMessage("Invalid number of arguments to function ''{0}''. Was expecting {1,number,#} arguments")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> invalidArgCount(String a0, int a1);

  @BaseMessage("At line {0,number,#}, column {1,number,#}")
  @ExceptionClass(value = EigenbaseContextException.class, causeRequired = true)
  ExInst<EigenbaseContextException> validatorContextPoint(int a0, int a1);

  @BaseMessage("From line {0,number,#}, column {1,number,#} to line {2,number,#}, column {3,number,#}")
  @ExceptionClass(value = EigenbaseContextException.class, causeRequired = true)
  ExInst<EigenbaseContextException> validatorContext(int a0, int a1, int a2,
      int a3);

  @BaseMessage("Cast function cannot convert value of type {0} to type {1}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> cannotCastValue(String a0, String a1);

  @BaseMessage("Unknown datatype name ''{0}''")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> unknownDatatypeName(String a0);

  @BaseMessage("Values passed to {0} operator must have compatible types")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> incompatibleValueType(String a0);

  @BaseMessage("Values in expression list must have compatible types")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> incompatibleTypesInList();

  @BaseMessage("Cannot apply {0} to the two different charsets {1} and {2}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> incompatibleCharset(String a0, String a1,
      String a2);

  @BaseMessage("ORDER BY is only allowed on top-level SELECT")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> invalidOrderByPos();

  @BaseMessage("Unknown identifier ''{0}''")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> unknownIdentifier(String a0);

  @BaseMessage("Unknown field ''{0}''")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> unknownField(String a0);

  @BaseMessage("Unknown target column ''{0}''")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> unknownTargetColumn(String a0);

  @BaseMessage("Target column ''{0}'' is assigned more than once")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> duplicateTargetColumn(String a0);

  @BaseMessage("Number of INSERT target columns ({0,number}) does not equal number of source items ({1,number})")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> unmatchInsertColumn(int a0, int a1);

  @BaseMessage("Cannot assign to target field ''{0}'' of type {1} from source field ''{2}'' of type {3}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> typeNotAssignable(String a0, String a1,
      String a2, String a3);

  @BaseMessage("Table ''{0}'' not found")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> tableNameNotFound(String a0);

  @BaseMessage("Column ''{0}'' not found in any table")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> columnNotFound(String a0);

  @BaseMessage("Column ''{0}'' not found in table ''{1}''")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> columnNotFoundInTable(String a0, String a1);

  @BaseMessage("Column ''{0}'' is ambiguous")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> columnAmbiguous(String a0);

  @BaseMessage("Operand {0} must be a query")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> needQueryOp(String a0);

  @BaseMessage("Parameters must be of the same type")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> needSameTypeParameter();

  @BaseMessage("Cannot apply ''{0}'' to arguments of type {1}. Supported form(s): {2}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> canNotApplyOp2Type(String a0, String a1,
      String a2);

  @BaseMessage("Expected a boolean type")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> expectedBoolean();

  @BaseMessage("ELSE clause or at least one THEN clause must be non-NULL")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> mustNotNullInElse();

  @BaseMessage("Function ''{0}'' is not defined")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> functionUndefined(String a0);

  @BaseMessage("Encountered {0} with {1,number} parameter(s); was expecting {2}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> wrongNumberOfParam(String a0, int a1,
      String a2);

  @BaseMessage("Illegal mixing of types in CASE or COALESCE statement")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> illegalMixingOfTypes();

  @BaseMessage("Invalid compare. Comparing (collation, coercibility): ({0}, {1} with ({2}, {3}) is illegal")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> invalidCompare(String a0, String a1, String a2,
      String a3);

  @BaseMessage("Invalid syntax. Two explicit different collations ({0}, {1}) are illegal")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> differentCollations(String a0, String a1);

  @BaseMessage("{0} is not comparable to {1}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> typeNotComparable(String a0, String a1);

  @BaseMessage("Cannot compare values of types ''{0}'', ''{1}''")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> typeNotComparableNear(String a0, String a1);

  @BaseMessage("Wrong number of arguments to expression")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> wrongNumOfArguments();

  @BaseMessage("Operands {0} not comparable to each other")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> operandNotComparable(String a0);

  @BaseMessage("Types {0} not comparable to each other")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> typeNotComparableEachOther(String a0);

  @BaseMessage("Numeric literal ''{0}'' out of range")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> numberLiteralOutOfRange(String a0);

  @BaseMessage("Date literal ''{0}'' out of range")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> dateLiteralOutOfRange(String a0);

  @BaseMessage("String literal continued on same line")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> stringFragsOnSameLine();

  @BaseMessage("Table or column alias must be a simple identifier")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> aliasMustBeSimpleIdentifier();

  @BaseMessage("List of column aliases must have same degree as table; table has {0,number,#} columns {1}, whereas alias list has {2,number,#} columns")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> aliasListDegree(int a0, String a1, int a2);

  @BaseMessage("Duplicate name ''{0}'' in column alias list")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> aliasListDuplicate(String a0);

  @BaseMessage("INNER, LEFT, RIGHT or FULL join requires a condition (NATURAL keyword or ON or USING clause)")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> joinRequiresCondition();

  @BaseMessage("Cannot specify condition (NATURAL keyword, or ON or USING clause) following CROSS JOIN")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> crossJoinDisallowsCondition();

  @BaseMessage("Cannot specify NATURAL keyword with ON or USING clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> naturalDisallowsOnOrUsing();

  @BaseMessage("Column name ''{0}'' in USING clause is not unique on one side of join")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> columnInUsingNotUnique(String a0);

  @BaseMessage("Column ''{0}'' matched using NATURAL keyword or USING clause has incompatible types: cannot compare ''{1}'' to ''{2}''")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> naturalOrUsingColumnNotCompatible(String a0,
      String a1, String a2);

  @BaseMessage("Window ''{0}'' not found")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> windowNotFound(String a0);

  @BaseMessage("Expression ''{0}'' is not being grouped")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> notGroupExpr(String a0);

  @BaseMessage("Expression ''{0}'' is not in the select clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> notSelectDistinctExpr(String a0);

  @BaseMessage("Aggregate expression is illegal in {0} clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> aggregateIllegalInClause(String a0);

  @BaseMessage("Windowed aggregate expression is illegal in {0} clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> windowedAggregateIllegalInClause(String a0);

  @BaseMessage("Aggregate expression is illegal in GROUP BY clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> aggregateIllegalInGroupBy();

  @BaseMessage("Aggregate expressions cannot be nested")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> nestedAggIllegal();

  @BaseMessage("Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> aggregateIllegalInOrderBy();

  @BaseMessage("{0} clause must be a condition")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> condMustBeBoolean(String a0);

  @BaseMessage("HAVING clause must be a condition")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> havingMustBeBoolean();

  @BaseMessage("OVER must be applied to aggregate function")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> overNonAggregate();

  @BaseMessage("Cannot override window attribute")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> cannotOverrideWindowAttribute();

  @BaseMessage("Column count mismatch in {0}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> columnCountMismatchInSetop(String a0);

  @BaseMessage("Type mismatch in column {0,number} of {1}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> columnTypeMismatchInSetop(int a0, String a1);

  @BaseMessage("Binary literal string must contain an even number of hexits")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> binaryLiteralOdd();

  @BaseMessage("Binary literal string must contain only characters ''0'' - ''9'', ''A'' - ''F''")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> binaryLiteralInvalid();

  @BaseMessage("Illegal interval literal format {0} for {1}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> unsupportedIntervalLiteral(String a0,
      String a1);

  @BaseMessage("Interval field value {0,number} exceeds precision of {1} field")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> intervalFieldExceedsPrecision(Number a0,
      String a1);

  @BaseMessage("RANGE clause cannot be used with compound ORDER BY clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> compoundOrderByProhibitsRange();

  @BaseMessage("Data type of ORDER BY prohibits use of RANGE clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> orderByDataTypeProhibitsRange();

  @BaseMessage("Data Type mismatch between ORDER BY and RANGE clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> orderByRangeMismatch();

  @BaseMessage("Window ORDER BY expression of type DATE requires range of type INTERVAL")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> dateRequiresInterval();

  @BaseMessage("Window boundary must be constant")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> rangeOrRowMustBeConstant();

  @BaseMessage("ROWS value must be a non-negative integral constant")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> rowMustBeNonNegativeIntegral();

  @BaseMessage("Window specification must contain an ORDER BY clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> overMissingOrderBy();

  @BaseMessage("UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> badLowerBoundary();

  @BaseMessage("UNBOUNDED PRECEDING cannot be specified for the upper frame boundary")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> badUpperBoundary();

  @BaseMessage("Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> currentRowPrecedingError();

  @BaseMessage("Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> currentRowFollowingError();

  @BaseMessage("Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> followingBeforePrecedingError();

  @BaseMessage("Window name must be a simple identifier")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> windowNameMustBeSimple();

  @BaseMessage("Duplicate window names not allowed")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> duplicateWindowName();

  @BaseMessage("Empty window specification not allowed")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> emptyWindowSpec();

  @BaseMessage("Duplicate window specification not allowed in the same window clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> dupWindowSpec();

  @BaseMessage("ROW/RANGE not allowed with RANK or DENSE_RANK functions")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> rankWithFrame();

  @BaseMessage("RANK or DENSE_RANK functions require ORDER BY clause in window specification")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> funcNeedsOrderBy();

  @BaseMessage("PARTITION BY not allowed with existing window reference")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> partitionNotAllowed();

  @BaseMessage("ORDER BY not allowed in both base and referenced windows")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> orderByOverlap();

  @BaseMessage("Referenced window cannot have framing declarations")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> refWindowWithFrame();

  @BaseMessage("Type ''{0}'' is not supported")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> typeNotSupported(String a0);

  @BaseMessage("DISTINCT/ALL not allowed with {0} function")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> functionQuantifierNotAllowed(String a0);

  @BaseMessage("Not allowed to perform {0} on {1}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> accessNotAllowed(String a0, String a1);

  @BaseMessage("The {0} function does not support the {1} data type.")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> minMaxBadType(String a0, String a1);

  @BaseMessage("Only scalar subqueries allowed in select list.")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> onlyScalarSubqueryAllowed();

  @BaseMessage("Ordinal out of range")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> orderByOrdinalOutOfRange();

  @BaseMessage("Window has negative size")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> windowHasNegativeSize();

  @BaseMessage("UNBOUNDED FOLLOWING window not supported")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> unboundedFollowingWindowNotSupported();

  @BaseMessage("Cannot use DISALLOW PARTIAL with window based on RANGE")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> cannotUseDisallowPartialWithRange();

  @BaseMessage("Interval leading field precision ''{0,number,#}'' out of range for {1}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> intervalStartPrecisionOutOfRange(int a0,
      String a1);

  @BaseMessage("Interval fractional second precision ''{0,number,#}'' out of range for {1}")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> intervalFractionalSecondPrecisionOutOfRange(
      int a0, String a1);

  @BaseMessage("Duplicate relation name ''{0}'' in FROM clause")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> fromAliasDuplicate(String a0);

  @BaseMessage("Duplicate column name ''{0}'' in output")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> duplicateColumnName(String a0);

  @BaseMessage("Duplicate name ''{0}'' in column list")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> duplicateNameInColumnList(String a0);

  @BaseMessage("Internal error: {0}")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> internal(String a0);

  @BaseMessage("Argument to function ''{0}'' must be a literal")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> argumentMustBeLiteral(String a0);

  @BaseMessage("Argument to function ''{0}'' must be a positive integer literal")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> argumentMustBePositiveInteger(String a0);

  @BaseMessage("Validation Error: {0}")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> validationError(String a0);

  @BaseMessage("Locale ''{0}'' in an illegal format")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> illegalLocaleFormat(String a0);

  @BaseMessage("Argument to function ''{0}'' must not be NULL")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> argumentMustNotBeNull(String a0);

  @BaseMessage("Illegal use of ''NULL''")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> nullIllegal();

  @BaseMessage("Illegal use of dynamic parameter")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> dynamicParamIllegal();

  @BaseMessage("''{0}'' is not a valid boolean value")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> invalidBoolean(String a0);

  @BaseMessage("Argument to function ''{0}'' must be a valid precision between ''{1,number,#}'' and ''{2,number,#}''")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> argumentMustBeValidPrecision(String a0, int a1,
      int a2);

  @BaseMessage("''{0}'' is not a valid datetime format")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> invalidDatetimeFormat(String a0);

  @BaseMessage("Cannot explicitly insert value into IDENTITY column ''{0}'' which is ALWAYS GENERATED")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> insertIntoAlwaysGenerated(String a0);

  @BaseMessage("Argument to function ''{0}'' must have a scale of 0")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> argumentMustHaveScaleZero(String a0);

  @BaseMessage("Statement preparation aborted")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> preparationAborted();

  @BaseMessage("SELECT DISTINCT not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  @ExceptionClass(EigenbaseException.class)
  Feature sQLFeature_E051_01();

  @BaseMessage("EXCEPT not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  @ExceptionClass(EigenbaseException.class)
  Feature sQLFeature_E071_03();

  @BaseMessage("UPDATE not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  @ExceptionClass(EigenbaseException.class)
  Feature sQLFeature_E101_03();

  @BaseMessage("Transactions not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  @ExceptionClass(EigenbaseException.class)
  Feature sQLFeature_E151();

  @BaseMessage("INTERSECT not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  @ExceptionClass(EigenbaseException.class)
  Feature sQLFeature_F302();

  @BaseMessage("MERGE not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  @ExceptionClass(EigenbaseException.class)
  Feature sQLFeature_F312();

  @BaseMessage("Basic multiset not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  @ExceptionClass(EigenbaseException.class)
  Feature sQLFeature_S271();

  @BaseMessage("TABLESAMPLE not supported")
  @Property(name = "FeatureDefinition", value = "SQL:2003 Part 2 Annex F")
  @ExceptionClass(EigenbaseException.class)
  Feature sQLFeature_T613();

  @BaseMessage("Execution of a new autocommit statement while a cursor is still open on same connection is not supported")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException>
  sQLConformance_MultipleActiveAutocommitStatements();

  @BaseMessage("Descending sort (ORDER BY DESC) not supported")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  @ExceptionClass(EigenbaseException.class)
  Feature sQLConformance_OrderByDesc();

  @BaseMessage("Sharing of cached statement plans not supported")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> sharedStatementPlans();

  @BaseMessage("TABLESAMPLE SUBSTITUTE not supported")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  @ExceptionClass(EigenbaseException.class)
  Feature sQLFeatureExt_T613_Substitution();

  @BaseMessage("Personality does not maintain table''s row count in the catalog")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> personalityManagesRowCount();

  @BaseMessage("Personality does not support snapshot reads")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> personalitySupportsSnapshots();

  @BaseMessage("Personality does not support labels")
  @Property(name = "FeatureDefinition", value = "Eigenbase-defined")
  @ExceptionClass(EigenbaseException.class)
  ExInst<EigenbaseException> personalitySupportsLabels();

  @BaseMessage("Require at least 1 argument")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> requireAtLeastOneArg();

  @BaseMessage("Map requires at least 2 arguments")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> mapRequiresTwoOrMoreArgs();

  @BaseMessage("Map requires an even number of arguments")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> mapRequiresEvenArgCount();

  @BaseMessage("Incompatible types")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> incompatibleTypes();

  @BaseMessage("Number of columns must match number of query columns")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> columnCountMismatch();

  @BaseMessage("Column has duplicate column name ''{0}'' and no column list specified")
  @ExceptionClass(SqlValidatorException.class)
  ExInst<SqlValidatorException> duplicateColumnAndNoColumnList(String s);
}
