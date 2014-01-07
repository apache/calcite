// This class is generated. Do NOT modify it, or
// add it to source control.

package org.eigenbase.resource;

import java.io.IOException;
import java.util.Locale;
import java.util.ResourceBundle;

import org.eigenbase.resgen.*;

/**
 * This class was generated
 * by class org.eigenbase.resgen.ResourceGen
 * from /Users/jhyde/open1/dynamodb/eigenbase/src/org/eigenbase/resource/EigenbaseResource.xml
 * on Fri Mar 23 14:27:41 PDT 2012.
 * It contains a list of messages, and methods to
 * retrieve and format those messages.
 */

public class EigenbaseResource
    extends org.eigenbase.resgen.ShadowResourceBundle {
  public EigenbaseResource() throws IOException {
  }

  private static final String baseName = "org.eigenbase.resource.EigenbaseResource";

  /**
   * Retrieves the singleton instance of {@link EigenbaseResource}. If
   * the application has called {@link #setThreadLocale}, returns the
   * resource for the thread's locale.
   */
  public static synchronized EigenbaseResource instance() {
    return (EigenbaseResource) instance(baseName,
        getThreadOrDefaultLocale(),
        ResourceBundle.getBundle(baseName, getThreadOrDefaultLocale()));
  }

  /**
   * Retrieves the instance of {@link EigenbaseResource} for the given locale.
   */
  public static synchronized EigenbaseResource instance(Locale locale) {
    return (EigenbaseResource) instance(baseName,
        locale,
        ResourceBundle.getBundle(baseName, locale));
  }

  /**
   * <code>ParserContext</code> is '<code>line {0,number,#}, column {1,number,#}</code>'
   */
  public final _Def0 ParserContext = new _Def0("ParserContext",
      "line {0,number,#}, column {1,number,#}",
      null);

  /**
   * <code>IllegalLiteral</code> is '<code>Illegal {0} literal {1}: {2}</code>'
   */
  public final _Def1 IllegalLiteral = new _Def1("IllegalLiteral",
      "Illegal {0} literal {1}: {2}",
      null);

  /**
   * <code>IdentifierTooLong</code> is '<code>Length of identifier &#39;&#39;{0}&#39;&#39; must be less than or equal to {1,number,#} characters</code>'
   */
  public final _Def2 IdentifierTooLong = new _Def2("IdentifierTooLong",
      "Length of identifier ''{0}'' must be less than or equal to {1,number,#} characters",
      null);

  /**
   * <code>BadFormat</code> is '<code>not in format &#39;&#39;{0}&#39;&#39;</code>'
   */
  public final _Def3 BadFormat = new _Def3("BadFormat",
      "not in format ''{0}''",
      null);

  /**
   * <code>BetweenWithoutAnd</code> is '<code>BETWEEN operator has no terminating AND</code>'
   */
  public final _Def4 BetweenWithoutAnd = new _Def4("BetweenWithoutAnd",
      "BETWEEN operator has no terminating AND",
      null);

  /**
   * <code>IllegalIntervalLiteral</code> is '<code>Illegal INTERVAL literal {0}; at {1}</code>'
   */
  public final _Def5 IllegalIntervalLiteral = new _Def5("IllegalIntervalLiteral",
      "Illegal INTERVAL literal {0}; at {1}",
      new String[]{"SQLSTATE", "42000"});

  /**
   * <code>IllegalMinusDate</code> is '<code>Illegal expression. Was expecting &quot;(DATETIME - DATETIME) INTERVALQUALIFIER&quot;</code>'
   */
  public final _Def6 IllegalMinusDate = new _Def6("IllegalMinusDate",
      "Illegal expression. Was expecting \"(DATETIME - DATETIME) INTERVALQUALIFIER\"",
      null);

  /**
   * <code>IllegalOverlaps</code> is '<code>Illegal overlaps expression. Was expecting expression on the form &quot;(DATETIME, EXPRESSION) OVERLAPS (DATETIME, EXPRESSION)&quot;</code>'
   */
  public final _Def6 IllegalOverlaps = new _Def6("IllegalOverlaps",
      "Illegal overlaps expression. Was expecting expression on the form \"(DATETIME, EXPRESSION) OVERLAPS (DATETIME, EXPRESSION)\"",
      null);

  /**
   * <code>IllegalNonQueryExpression</code> is '<code>Non-query expression encountered in illegal context</code>'
   */
  public final _Def6 IllegalNonQueryExpression = new _Def6(
      "IllegalNonQueryExpression",
      "Non-query expression encountered in illegal context",
      null);

  /**
   * <code>IllegalQueryExpression</code> is '<code>Query expression encountered in illegal context</code>'
   */
  public final _Def6 IllegalQueryExpression = new _Def6("IllegalQueryExpression",
      "Query expression encountered in illegal context",
      null);

  /**
   * <code>IllegalCursorExpression</code> is '<code>CURSOR expression encountered in illegal context</code>'
   */
  public final _Def6 IllegalCursorExpression = new _Def6(
      "IllegalCursorExpression",
      "CURSOR expression encountered in illegal context",
      null);

  /**
   * <code>IllegalOrderBy</code> is '<code>ORDER BY unexpected</code>'
   */
  public final _Def6 IllegalOrderBy = new _Def6("IllegalOrderBy",
      "ORDER BY unexpected",
      null);

  /**
   * <code>IllegalBinaryString</code> is '<code>Illegal binary string {0}</code>'
   */
  public final _Def7 IllegalBinaryString = new _Def7("IllegalBinaryString",
      "Illegal binary string {0}",
      null);

  /**
   * <code>IllegalFromEmpty</code> is '<code>&#39;&#39;FROM&#39;&#39; without operands preceding it is illegal</code>'
   */
  public final _Def6 IllegalFromEmpty = new _Def6("IllegalFromEmpty",
      "''FROM'' without operands preceding it is illegal",
      null);

  /**
   * <code>IllegalRowExpression</code> is '<code>ROW expression encountered in illegal context</code>'
   */
  public final _Def6 IllegalRowExpression = new _Def6("IllegalRowExpression",
      "ROW expression encountered in illegal context",
      null);

  /**
   * <code>InvalidSampleSize</code> is '<code>TABLESAMPLE percentage must be between 0 and 100, inclusive</code>'
   */
  public final _Def6 InvalidSampleSize = new _Def6("InvalidSampleSize",
      "TABLESAMPLE percentage must be between 0 and 100, inclusive",
      new String[]{"SQLSTATE", "2202H"});

  /**
   * <code>UnknownCharacterSet</code> is '<code>Unknown character set &#39;&#39;{0}&#39;&#39;</code>'
   */
  public final _Def7 UnknownCharacterSet = new _Def7("UnknownCharacterSet",
      "Unknown character set ''{0}''",
      null);

  /**
   * <code>CharsetEncoding</code> is '<code>Failed to encode &#39;&#39;{0}&#39;&#39; in character set &#39;&#39;{1}&#39;&#39;</code>'
   */
  public final _Def5 CharsetEncoding = new _Def5("CharsetEncoding",
      "Failed to encode ''{0}'' in character set ''{1}''",
      null);

  /**
   * <code>UnicodeEscapeCharLength</code> is '<code>UESCAPE &#39;&#39;{0}&#39;&#39; must be exactly one character</code>'
   */
  public final _Def7 UnicodeEscapeCharLength = new _Def7(
      "UnicodeEscapeCharLength",
      "UESCAPE ''{0}'' must be exactly one character",
      null);

  /**
   * <code>UnicodeEscapeCharIllegal</code> is '<code>UESCAPE &#39;&#39;{0}&#39;&#39; may not be hex digit, whitespace, plus sign, or double quote</code>'
   */
  public final _Def7 UnicodeEscapeCharIllegal = new _Def7(
      "UnicodeEscapeCharIllegal",
      "UESCAPE ''{0}'' may not be hex digit, whitespace, plus sign, or double quote",
      null);

  /**
   * <code>UnicodeEscapeUnexpected</code> is '<code>UESCAPE cannot be specified without Unicode literal introducer</code>'
   */
  public final _Def6 UnicodeEscapeUnexpected = new _Def6(
      "UnicodeEscapeUnexpected",
      "UESCAPE cannot be specified without Unicode literal introducer",
      null);

  /**
   * <code>UnicodeEscapeMalformed</code> is '<code>Unicode escape sequence starting at character {0,number,#} is not exactly four hex digits</code>'
   */
  public final _Def8 UnicodeEscapeMalformed = new _Def8("UnicodeEscapeMalformed",
      "Unicode escape sequence starting at character {0,number,#} is not exactly four hex digits",
      null);

  /**
   * <code>ValidatorUnknownFunction</code> is '<code>No match found for function signature {0}</code>'
   */
  public final _Def9 ValidatorUnknownFunction = new _Def9(
      "ValidatorUnknownFunction",
      "No match found for function signature {0}",
      null);

  /**
   * <code>InvalidArgCount</code> is '<code>Invalid number of arguments to function &#39;&#39;{0}&#39;&#39;. Was expecting {1,number,#} arguments</code>'
   */
  public final _Def10 InvalidArgCount = new _Def10("InvalidArgCount",
      "Invalid number of arguments to function ''{0}''. Was expecting {1,number,#} arguments",
      null);

  /**
   * <code>ValidatorContextPoint</code> is '<code>At line {0,number,#}, column {1,number,#}</code>'
   */
  public final _Def11 ValidatorContextPoint = new _Def11("ValidatorContextPoint",
      "At line {0,number,#}, column {1,number,#}",
      null);

  /**
   * <code>ValidatorContext</code> is '<code>From line {0,number,#}, column {1,number,#} to line {2,number,#}, column {3,number,#}</code>'
   */
  public final _Def12 ValidatorContext = new _Def12("ValidatorContext",
      "From line {0,number,#}, column {1,number,#} to line {2,number,#}, column {3,number,#}",
      null);

  /**
   * <code>CannotCastValue</code> is '<code>Cast function cannot convert value of type {0} to type {1}</code>'
   */
  public final _Def13 CannotCastValue = new _Def13("CannotCastValue",
      "Cast function cannot convert value of type {0} to type {1}",
      null);

  /**
   * <code>UnknownDatatypeName</code> is '<code>Unknown datatype name &#39;&#39;{0}&#39;&#39;</code>'
   */
  public final _Def9 UnknownDatatypeName = new _Def9("UnknownDatatypeName",
      "Unknown datatype name ''{0}''",
      null);

  /**
   * <code>IncompatibleValueType</code> is '<code>Values passed to {0} operator must have compatible types</code>'
   */
  public final _Def9 IncompatibleValueType = new _Def9("IncompatibleValueType",
      "Values passed to {0} operator must have compatible types",
      null);

  /**
   * <code>IncompatibleTypesInList</code> is '<code>Values in expression list must have compatible types</code>'
   */
  public final _Def4 IncompatibleTypesInList = new _Def4(
      "IncompatibleTypesInList",
      "Values in expression list must have compatible types",
      null);

  /**
   * <code>IncompatibleCharset</code> is '<code>Cannot apply {0} to the two different charsets {1} and {2}</code>'
   */
  public final _Def14 IncompatibleCharset = new _Def14("IncompatibleCharset",
      "Cannot apply {0} to the two different charsets {1} and {2}",
      null);

  /**
   * <code>InvalidOrderByPos</code> is '<code>ORDER BY is only allowed on top-level SELECT</code>'
   */
  public final _Def4 InvalidOrderByPos = new _Def4("InvalidOrderByPos",
      "ORDER BY is only allowed on top-level SELECT",
      null);

  /**
   * <code>UnknownIdentifier</code> is '<code>Unknown identifier &#39;&#39;{0}&#39;&#39;</code>'
   */
  public final _Def9 UnknownIdentifier = new _Def9("UnknownIdentifier",
      "Unknown identifier ''{0}''",
      null);

  /**
   * <code>UnknownField</code> is '<code>Unknown field &#39;&#39;{0}&#39;&#39;</code>'
   */
  public final _Def9 UnknownField = new _Def9("UnknownField",
      "Unknown field ''{0}''",
      null);

  /**
   * <code>UnknownTargetColumn</code> is '<code>Unknown target column &#39;&#39;{0}&#39;&#39;</code>'
   */
  public final _Def9 UnknownTargetColumn = new _Def9("UnknownTargetColumn",
      "Unknown target column ''{0}''",
      null);

  /**
   * <code>DuplicateTargetColumn</code> is '<code>Target column &#39;&#39;{0}&#39;&#39; is assigned more than once</code>'
   */
  public final _Def9 DuplicateTargetColumn = new _Def9("DuplicateTargetColumn",
      "Target column ''{0}'' is assigned more than once",
      null);

  /**
   * <code>UnmatchInsertColumn</code> is '<code>Number of INSERT target columns ({0,number}) does not equal number of source items ({1,number})</code>'
   */
  public final _Def15 UnmatchInsertColumn = new _Def15("UnmatchInsertColumn",
      "Number of INSERT target columns ({0,number}) does not equal number of source items ({1,number})",
      null);

  /**
   * <code>TypeNotAssignable</code> is '<code>Cannot assign to target field &#39;&#39;{0}&#39;&#39; of type {1} from source field &#39;&#39;{2}&#39;&#39; of type {3}</code>'
   */
  public final _Def16 TypeNotAssignable = new _Def16("TypeNotAssignable",
      "Cannot assign to target field ''{0}'' of type {1} from source field ''{2}'' of type {3}",
      null);

  /**
   * <code>TableNameNotFound</code> is '<code>Table &#39;&#39;{0}&#39;&#39; not found</code>'
   */
  public final _Def9 TableNameNotFound = new _Def9("TableNameNotFound",
      "Table ''{0}'' not found",
      null);

  /**
   * <code>ColumnNotFound</code> is '<code>Column &#39;&#39;{0}&#39;&#39; not found in any table</code>'
   */
  public final _Def9 ColumnNotFound = new _Def9("ColumnNotFound",
      "Column ''{0}'' not found in any table",
      null);

  /**
   * <code>ColumnNotFoundInTable</code> is '<code>Column &#39;&#39;{0}&#39;&#39; not found in table &#39;&#39;{1}&#39;&#39;</code>'
   */
  public final _Def13 ColumnNotFoundInTable = new _Def13("ColumnNotFoundInTable",
      "Column ''{0}'' not found in table ''{1}''",
      null);

  /**
   * <code>ColumnAmbiguous</code> is '<code>Column &#39;&#39;{0}&#39;&#39; is ambiguous</code>'
   */
  public final _Def9 ColumnAmbiguous = new _Def9("ColumnAmbiguous",
      "Column ''{0}'' is ambiguous",
      null);

  /**
   * <code>NeedQueryOp</code> is '<code>Operand {0} must be a query</code>'
   */
  public final _Def9 NeedQueryOp = new _Def9("NeedQueryOp",
      "Operand {0} must be a query",
      null);

  /**
   * <code>NeedSameTypeParameter</code> is '<code>Parameters must be of the same type</code>'
   */
  public final _Def4 NeedSameTypeParameter = new _Def4("NeedSameTypeParameter",
      "Parameters must be of the same type",
      null);

  /**
   * <code>CanNotApplyOp2Type</code> is '<code>Cannot apply &#39;&#39;{0}&#39;&#39; to arguments of type {1}. Supported form(s): {2}</code>'
   */
  public final _Def14 CanNotApplyOp2Type = new _Def14("CanNotApplyOp2Type",
      "Cannot apply ''{0}'' to arguments of type {1}. Supported form(s): {2}",
      null);

  /**
   * <code>ExpectedBoolean</code> is '<code>Expected a boolean type</code>'
   */
  public final _Def4 ExpectedBoolean = new _Def4("ExpectedBoolean",
      "Expected a boolean type",
      null);

  /**
   * <code>MustNotNullInElse</code> is '<code>ELSE clause or at least one THEN clause must be non-NULL</code>'
   */
  public final _Def4 MustNotNullInElse = new _Def4("MustNotNullInElse",
      "ELSE clause or at least one THEN clause must be non-NULL",
      null);

  /**
   * <code>FunctionUndefined</code> is '<code>Function &#39;&#39;{0}&#39;&#39; is not defined</code>'
   */
  public final _Def9 FunctionUndefined = new _Def9("FunctionUndefined",
      "Function ''{0}'' is not defined",
      null);

  /**
   * <code>WrongNumberOfParam</code> is '<code>Encountered {0} with {1,number} parameter(s); was expecting {2}</code>'
   */
  public final _Def17 WrongNumberOfParam = new _Def17("WrongNumberOfParam",
      "Encountered {0} with {1,number} parameter(s); was expecting {2}",
      null);

  /**
   * <code>IllegalMixingOfTypes</code> is '<code>Illegal mixing of types in CASE or COALESCE statement</code>'
   */
  public final _Def4 IllegalMixingOfTypes = new _Def4("IllegalMixingOfTypes",
      "Illegal mixing of types in CASE or COALESCE statement",
      null);

  /**
   * <code>InvalidCompare</code> is '<code>Invalid compare. Comparing  (collation, coercibility): ({0}, {1} with ({2}, {3}) is illegal</code>'
   */
  public final _Def18 InvalidCompare = new _Def18("InvalidCompare",
      "Invalid compare. Comparing  (collation, coercibility): ({0}, {1} with ({2}, {3}) is illegal",
      null);

  /**
   * <code>DifferentCollations</code> is '<code>Invalid syntax. Two explicit different collations ({0}, {1}) are illegal</code>'
   */
  public final _Def5 DifferentCollations = new _Def5("DifferentCollations",
      "Invalid syntax. Two explicit different collations ({0}, {1}) are illegal",
      null);

  /**
   * <code>TypeNotComparable</code> is '<code>{0} is not comparable to {1}</code>'
   */
  public final _Def13 TypeNotComparable = new _Def13("TypeNotComparable",
      "{0} is not comparable to {1}",
      null);

  /**
   * <code>TypeNotComparableNear</code> is '<code>Cannot compare values of types &#39;&#39;{0}&#39;&#39;, &#39;&#39;{1}&#39;&#39;</code>'
   */
  public final _Def13 TypeNotComparableNear = new _Def13("TypeNotComparableNear",
      "Cannot compare values of types ''{0}'', ''{1}''",
      null);

  /**
   * <code>WrongNumOfArguments</code> is '<code>Wrong number of arguments to expression</code>'
   */
  public final _Def4 WrongNumOfArguments = new _Def4("WrongNumOfArguments",
      "Wrong number of arguments to expression",
      null);

  /**
   * <code>OperandNotComparable</code> is '<code>Operands {0} not comparable to each other</code>'
   */
  public final _Def9 OperandNotComparable = new _Def9("OperandNotComparable",
      "Operands {0} not comparable to each other",
      null);

  /**
   * <code>TypeNotComparableEachOther</code> is '<code>Types {0} not comparable to each other</code>'
   */
  public final _Def9 TypeNotComparableEachOther = new _Def9(
      "TypeNotComparableEachOther",
      "Types {0} not comparable to each other",
      null);

  /**
   * <code>NumberLiteralOutOfRange</code> is '<code>Numeric literal &#39;&#39;{0}&#39;&#39; out of range</code>'
   */
  public final _Def9 NumberLiteralOutOfRange = new _Def9(
      "NumberLiteralOutOfRange",
      "Numeric literal ''{0}'' out of range",
      null);

  /**
   * <code>DateLiteralOutOfRange</code> is '<code>Date literal &#39;&#39;{0}&#39;&#39; out of range</code>'
   */
  public final _Def9 DateLiteralOutOfRange = new _Def9("DateLiteralOutOfRange",
      "Date literal ''{0}'' out of range",
      null);

  /**
   * <code>StringFragsOnSameLine</code> is '<code>String literal continued on same line</code>'
   */
  public final _Def4 StringFragsOnSameLine = new _Def4("StringFragsOnSameLine",
      "String literal continued on same line",
      null);

  /**
   * <code>AliasMustBeSimpleIdentifier</code> is '<code>Table or column alias must be a simple identifier</code>'
   */
  public final _Def4 AliasMustBeSimpleIdentifier = new _Def4(
      "AliasMustBeSimpleIdentifier",
      "Table or column alias must be a simple identifier",
      null);

  /**
   * <code>AliasListDegree</code> is '<code>List of column aliases must have same degree as table; table has {0,number,#} columns {1}, whereas alias list has {2,number,#} columns</code>'
   */
  public final _Def19 AliasListDegree = new _Def19("AliasListDegree",
      "List of column aliases must have same degree as table; table has {0,number,#} columns {1}, whereas alias list has {2,number,#} columns",
      null);

  /**
   * <code>AliasListDuplicate</code> is '<code>Duplicate name &#39;&#39;{0}&#39;&#39; in column alias list</code>'
   */
  public final _Def9 AliasListDuplicate = new _Def9("AliasListDuplicate",
      "Duplicate name ''{0}'' in column alias list",
      null);

  /**
   * <code>JoinRequiresCondition</code> is '<code>INNER, LEFT, RIGHT or FULL join requires a condition (NATURAL keyword or ON or USING clause)</code>'
   */
  public final _Def4 JoinRequiresCondition = new _Def4("JoinRequiresCondition",
      "INNER, LEFT, RIGHT or FULL join requires a condition (NATURAL keyword or ON or USING clause)",
      null);

  /**
   * <code>CrossJoinDisallowsCondition</code> is '<code>Cannot specify condition (NATURAL keyword, or ON or USING clause) following CROSS JOIN</code>'
   */
  public final _Def4 CrossJoinDisallowsCondition = new _Def4(
      "CrossJoinDisallowsCondition",
      "Cannot specify condition (NATURAL keyword, or ON or USING clause) following CROSS JOIN",
      null);

  /**
   * <code>NaturalDisallowsOnOrUsing</code> is '<code>Cannot specify NATURAL keyword with ON or USING clause</code>'
   */
  public final _Def4 NaturalDisallowsOnOrUsing = new _Def4(
      "NaturalDisallowsOnOrUsing",
      "Cannot specify NATURAL keyword with ON or USING clause",
      null);

  /**
   * <code>ColumnInUsingNotUnique</code> is '<code>Column name &#39;&#39;{0}&#39;&#39; in USING clause is not unique on one side of join</code>'
   */
  public final _Def9 ColumnInUsingNotUnique = new _Def9("ColumnInUsingNotUnique",
      "Column name ''{0}'' in USING clause is not unique on one side of join",
      null);

  /**
   * <code>NaturalOrUsingColumnNotCompatible</code> is '<code>Column &#39;&#39;{0}&#39;&#39; matched using NATURAL keyword or USING clause has incompatible types: cannot compare &#39;&#39;{1}&#39;&#39; to &#39;&#39;{2}&#39;&#39;</code>'
   */
  public final _Def14 NaturalOrUsingColumnNotCompatible = new _Def14(
      "NaturalOrUsingColumnNotCompatible",
      "Column ''{0}'' matched using NATURAL keyword or USING clause has incompatible types: cannot compare ''{1}'' to ''{2}''",
      null);

  /**
   * <code>WindowNotFound</code> is '<code>Window &#39;&#39;{0}&#39;&#39; not found</code>'
   */
  public final _Def9 WindowNotFound = new _Def9("WindowNotFound",
      "Window ''{0}'' not found",
      null);

  /**
   * <code>NotGroupExpr</code> is '<code>Expression &#39;&#39;{0}&#39;&#39; is not being grouped</code>'
   */
  public final _Def9 NotGroupExpr = new _Def9("NotGroupExpr",
      "Expression ''{0}'' is not being grouped",
      null);

  /**
   * <code>NotSelectDistinctExpr</code> is '<code>Expression &#39;&#39;{0}&#39;&#39; is not in the select clause</code>'
   */
  public final _Def9 NotSelectDistinctExpr = new _Def9("NotSelectDistinctExpr",
      "Expression ''{0}'' is not in the select clause",
      null);

  /**
   * <code>AggregateIllegalInClause</code> is '<code>Aggregate expression is illegal in {0} clause</code>'
   */
  public final _Def9 AggregateIllegalInClause = new _Def9(
      "AggregateIllegalInClause",
      "Aggregate expression is illegal in {0} clause",
      null);

  /**
   * <code>WindowedAggregateIllegalInClause</code> is '<code>Windowed aggregate expression is illegal in {0} clause</code>'
   */
  public final _Def9 WindowedAggregateIllegalInClause = new _Def9(
      "WindowedAggregateIllegalInClause",
      "Windowed aggregate expression is illegal in {0} clause",
      null);

  /**
   * <code>AggregateIllegalInGroupBy</code> is '<code>Aggregate expression is illegal in GROUP BY clause</code>'
   */
  public final _Def4 AggregateIllegalInGroupBy = new _Def4(
      "AggregateIllegalInGroupBy",
      "Aggregate expression is illegal in GROUP BY clause",
      null);

  /**
   * <code>NestedAggIllegal</code> is '<code>Aggregate expressions cannot be nested</code>'
   */
  public final _Def4 NestedAggIllegal = new _Def4("NestedAggIllegal",
      "Aggregate expressions cannot be nested",
      null);

  /**
   * <code>AggregateIllegalInOrderBy</code> is '<code>Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT</code>'
   */
  public final _Def4 AggregateIllegalInOrderBy = new _Def4(
      "AggregateIllegalInOrderBy",
      "Aggregate expression is illegal in ORDER BY clause of non-aggregating SELECT",
      null);

  /**
   * <code>CondMustBeBoolean</code> is '<code>{0} clause must be a condition</code>'
   */
  public final _Def9 CondMustBeBoolean = new _Def9("CondMustBeBoolean",
      "{0} clause must be a condition",
      null);

  /**
   * <code>HavingMustBeBoolean</code> is '<code>HAVING clause must be a condition</code>'
   */
  public final _Def4 HavingMustBeBoolean = new _Def4("HavingMustBeBoolean",
      "HAVING clause must be a condition",
      null);

  /**
   * <code>OverNonAggregate</code> is '<code>OVER must be applied to aggregate function</code>'
   */
  public final _Def4 OverNonAggregate = new _Def4("OverNonAggregate",
      "OVER must be applied to aggregate function",
      null);

  /**
   * <code>CannotOverrideWindowAttribute</code> is '<code>Cannot override window attribute</code>'
   */
  public final _Def4 CannotOverrideWindowAttribute = new _Def4(
      "CannotOverrideWindowAttribute",
      "Cannot override window attribute",
      null);

  /**
   * <code>ColumnCountMismatchInSetop</code> is '<code>Column count mismatch in {0}</code>'
   */
  public final _Def9 ColumnCountMismatchInSetop = new _Def9(
      "ColumnCountMismatchInSetop",
      "Column count mismatch in {0}",
      null);

  /**
   * <code>ColumnTypeMismatchInSetop</code> is '<code>Type mismatch in column {0,number} of {1}</code>'
   */
  public final _Def20 ColumnTypeMismatchInSetop = new _Def20(
      "ColumnTypeMismatchInSetop",
      "Type mismatch in column {0,number} of {1}",
      null);

  /**
   * <code>BinaryLiteralOdd</code> is '<code>Binary literal string must contain an even number of hexits</code>'
   */
  public final _Def4 BinaryLiteralOdd = new _Def4("BinaryLiteralOdd",
      "Binary literal string must contain an even number of hexits",
      null);

  /**
   * <code>BinaryLiteralInvalid</code> is '<code>Binary literal string must contain only characters &#39;&#39;0&#39;&#39; - &#39;&#39;9&#39;&#39;, &#39;&#39;A&#39;&#39; - &#39;&#39;F&#39;&#39;</code>'
   */
  public final _Def4 BinaryLiteralInvalid = new _Def4("BinaryLiteralInvalid",
      "Binary literal string must contain only characters ''0'' - ''9'', ''A'' - ''F''",
      null);

  /**
   * <code>UnsupportedIntervalLiteral</code> is '<code>Illegal interval literal format {0} for {1}</code>'
   */
  public final _Def13 UnsupportedIntervalLiteral = new _Def13(
      "UnsupportedIntervalLiteral",
      "Illegal interval literal format {0} for {1}",
      null);

  /**
   * <code>IntervalFieldExceedsPrecision</code> is '<code>Interval field value {0,number} exceeds precision of {1} field</code>'
   */
  public final _Def20 IntervalFieldExceedsPrecision = new _Def20(
      "IntervalFieldExceedsPrecision",
      "Interval field value {0,number} exceeds precision of {1} field",
      null);

  /**
   * <code>CompoundOrderByProhibitsRange</code> is '<code>RANGE clause cannot be used with compound ORDER BY clause</code>'
   */
  public final _Def4 CompoundOrderByProhibitsRange = new _Def4(
      "CompoundOrderByProhibitsRange",
      "RANGE clause cannot be used with compound ORDER BY clause",
      null);

  /**
   * <code>OrderByDataTypeProhibitsRange</code> is '<code>Data type of ORDER BY prohibits use of RANGE clause</code>'
   */
  public final _Def4 OrderByDataTypeProhibitsRange = new _Def4(
      "OrderByDataTypeProhibitsRange",
      "Data type of ORDER BY prohibits use of RANGE clause",
      null);

  /**
   * <code>OrderByRangeMismatch</code> is '<code>Data Type mismatch between ORDER BY and RANGE clause</code>'
   */
  public final _Def4 OrderByRangeMismatch = new _Def4("OrderByRangeMismatch",
      "Data Type mismatch between ORDER BY and RANGE clause",
      null);

  /**
   * <code>DateRequiresInterval</code> is '<code>Window ORDER BY expression of type DATE requires range of type INTERVAL</code>'
   */
  public final _Def4 DateRequiresInterval = new _Def4("DateRequiresInterval",
      "Window ORDER BY expression of type DATE requires range of type INTERVAL",
      null);

  /**
   * <code>RangeOrRowMustBeConstant</code> is '<code>Window boundary must be constant</code>'
   */
  public final _Def4 RangeOrRowMustBeConstant = new _Def4(
      "RangeOrRowMustBeConstant",
      "Window boundary must be constant",
      null);

  /**
   * <code>RowMustBeNonNegativeIntegral</code> is '<code>ROWS value must be a non-negative integral constant</code>'
   */
  public final _Def4 RowMustBeNonNegativeIntegral = new _Def4(
      "RowMustBeNonNegativeIntegral",
      "ROWS value must be a non-negative integral constant",
      null);

  /**
   * <code>OverMissingOrderBy</code> is '<code>Window specification must contain an ORDER BY clause</code>'
   */
  public final _Def4 OverMissingOrderBy = new _Def4("OverMissingOrderBy",
      "Window specification must contain an ORDER BY clause",
      null);

  /**
   * <code>BadLowerBoundary</code> is '<code>UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary</code>'
   */
  public final _Def4 BadLowerBoundary = new _Def4("BadLowerBoundary",
      "UNBOUNDED FOLLOWING cannot be specified for the lower frame boundary",
      null);

  /**
   * <code>BadUpperBoundary</code> is '<code>UNBOUNDED PRECEDING cannot be specified for the upper frame boundary</code>'
   */
  public final _Def4 BadUpperBoundary = new _Def4("BadUpperBoundary",
      "UNBOUNDED PRECEDING cannot be specified for the upper frame boundary",
      null);

  /**
   * <code>CurrentRowPrecedingError</code> is '<code>Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW</code>'
   */
  public final _Def4 CurrentRowPrecedingError = new _Def4(
      "CurrentRowPrecedingError",
      "Upper frame boundary cannot be PRECEDING when lower boundary is CURRENT ROW",
      null);

  /**
   * <code>CurrentRowFollowingError</code> is '<code>Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING</code>'
   */
  public final _Def4 CurrentRowFollowingError = new _Def4(
      "CurrentRowFollowingError",
      "Upper frame boundary cannot be CURRENT ROW when lower boundary is FOLLOWING",
      null);

  /**
   * <code>FollowingBeforePrecedingError</code> is '<code>Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING</code>'
   */
  public final _Def4 FollowingBeforePrecedingError = new _Def4(
      "FollowingBeforePrecedingError",
      "Upper frame boundary cannot be PRECEDING when lower boundary is FOLLOWING",
      null);

  /**
   * <code>WindowNameMustBeSimple</code> is '<code>Window name must be a simple identifier</code>'
   */
  public final _Def4 WindowNameMustBeSimple = new _Def4("WindowNameMustBeSimple",
      "Window name must be a simple identifier",
      null);

  /**
   * <code>DuplicateWindowName</code> is '<code>Duplicate window names not allowed</code>'
   */
  public final _Def4 DuplicateWindowName = new _Def4("DuplicateWindowName",
      "Duplicate window names not allowed",
      null);

  /**
   * <code>EmptyWindowSpec</code> is '<code>Empty window specification not allowed</code>'
   */
  public final _Def4 EmptyWindowSpec = new _Def4("EmptyWindowSpec",
      "Empty window specification not allowed",
      null);

  /**
   * <code>DupWindowSpec</code> is '<code>Duplicate window specification not allowed in the same window clause</code>'
   */
  public final _Def4 DupWindowSpec = new _Def4("DupWindowSpec",
      "Duplicate window specification not allowed in the same window clause",
      null);

  /**
   * <code>RankWithFrame</code> is '<code>ROW/RANGE not allowed with RANK or DENSE_RANK functions</code>'
   */
  public final _Def4 RankWithFrame = new _Def4("RankWithFrame",
      "ROW/RANGE not allowed with RANK or DENSE_RANK functions",
      null);

  /**
   * <code>FuncNeedsOrderBy</code> is '<code>RANK or DENSE_RANK functions require ORDER BY clause in window specification</code>'
   */
  public final _Def4 FuncNeedsOrderBy = new _Def4("FuncNeedsOrderBy",
      "RANK or DENSE_RANK functions require ORDER BY clause in window specification",
      null);

  /**
   * <code>PartitionNotAllowed</code> is '<code>PARTITION BY not allowed with existing window reference</code>'
   */
  public final _Def4 PartitionNotAllowed = new _Def4("PartitionNotAllowed",
      "PARTITION BY not allowed with existing window reference",
      null);

  /**
   * <code>OrderByOverlap</code> is '<code>ORDER BY not allowed in both base and referenced windows</code>'
   */
  public final _Def4 OrderByOverlap = new _Def4("OrderByOverlap",
      "ORDER BY not allowed in both base and referenced windows",
      null);

  /**
   * <code>RefWindowWithFrame</code> is '<code>Referenced window cannot have framing declarations</code>'
   */
  public final _Def4 RefWindowWithFrame = new _Def4("RefWindowWithFrame",
      "Referenced window cannot have framing declarations",
      null);

  /**
   * <code>TypeNotSupported</code> is '<code>Type &#39;&#39;{0}&#39;&#39; is not supported</code>'
   */
  public final _Def9 TypeNotSupported = new _Def9("TypeNotSupported",
      "Type ''{0}'' is not supported",
      null);

  /**
   * <code>FunctionQuantifierNotAllowed</code> is '<code>DISTINCT/ALL not allowed with {0} function</code>'
   */
  public final _Def9 FunctionQuantifierNotAllowed = new _Def9(
      "FunctionQuantifierNotAllowed",
      "DISTINCT/ALL not allowed with {0} function",
      null);

  /**
   * <code>AccessNotAllowed</code> is '<code>Not allowed to perform {0} on {1}</code>'
   */
  public final _Def13 AccessNotAllowed = new _Def13("AccessNotAllowed",
      "Not allowed to perform {0} on {1}",
      null);

  /**
   * <code>MinMaxBadType</code> is '<code>The {0} function does not support the {1} data type.</code>'
   */
  public final _Def13 MinMaxBadType = new _Def13("MinMaxBadType",
      "The {0} function does not support the {1} data type.",
      null);

  /**
   * <code>OnlyScalarSubqueryAllowed</code> is '<code>Only scalar subqueries allowed in select list.</code>'
   */
  public final _Def4 OnlyScalarSubqueryAllowed = new _Def4(
      "OnlyScalarSubqueryAllowed",
      "Only scalar subqueries allowed in select list.",
      null);

  /**
   * <code>OrderByOrdinalOutOfRange</code> is '<code>Ordinal out of range</code>'
   */
  public final _Def4 OrderByOrdinalOutOfRange = new _Def4(
      "OrderByOrdinalOutOfRange",
      "Ordinal out of range",
      null);

  /**
   * <code>WindowHasNegativeSize</code> is '<code>Window has negative size</code>'
   */
  public final _Def4 WindowHasNegativeSize = new _Def4("WindowHasNegativeSize",
      "Window has negative size",
      null);

  /**
   * <code>UnboundedFollowingWindowNotSupported</code> is '<code>UNBOUNDED FOLLOWING window not supported</code>'
   */
  public final _Def4 UnboundedFollowingWindowNotSupported = new _Def4(
      "UnboundedFollowingWindowNotSupported",
      "UNBOUNDED FOLLOWING window not supported",
      null);

  /**
   * <code>CannotUseDisallowPartialWithRange</code> is '<code>Cannot use DISALLOW PARTIAL with window based on RANGE</code>'
   */
  public final _Def4 CannotUseDisallowPartialWithRange = new _Def4(
      "CannotUseDisallowPartialWithRange",
      "Cannot use DISALLOW PARTIAL with window based on RANGE",
      null);

  /**
   * <code>IntervalStartPrecisionOutOfRange</code> is '<code>Interval leading field precision &#39;&#39;{0}&#39;&#39; out of range for {1}</code>'
   */
  public final _Def13 IntervalStartPrecisionOutOfRange = new _Def13(
      "IntervalStartPrecisionOutOfRange",
      "Interval leading field precision ''{0}'' out of range for {1}",
      null);

  /**
   * <code>IntervalFractionalSecondPrecisionOutOfRange</code> is '<code>Interval fractional second precision &#39;&#39;{0}&#39;&#39; out of range for {1}</code>'
   */
  public final _Def13 IntervalFractionalSecondPrecisionOutOfRange = new _Def13(
      "IntervalFractionalSecondPrecisionOutOfRange",
      "Interval fractional second precision ''{0}'' out of range for {1}",
      null);

  /**
   * <code>FromAliasDuplicate</code> is '<code>Duplicate relation name &#39;&#39;{0}&#39;&#39; in FROM clause</code>'
   */
  public final _Def9 FromAliasDuplicate = new _Def9("FromAliasDuplicate",
      "Duplicate relation name ''{0}'' in FROM clause",
      null);

  /**
   * <code>DuplicateColumnName</code> is '<code>Duplicate column name &#39;&#39;{0}&#39;&#39; in output</code>'
   */
  public final _Def9 DuplicateColumnName = new _Def9("DuplicateColumnName",
      "Duplicate column name ''{0}'' in output",
      null);

  /**
   * <code>Internal</code> is '<code>Internal error: {0}</code>'
   */
  public final _Def7 Internal = new _Def7("Internal",
      "Internal error: {0}",
      null);

  /**
   * <code>ArgumentMustBeLiteral</code> is '<code>Argument to function &#39;&#39;{0}&#39;&#39; must be a literal</code>'
   */
  public final _Def9 ArgumentMustBeLiteral = new _Def9("ArgumentMustBeLiteral",
      "Argument to function ''{0}'' must be a literal",
      null);

  /**
   * <code>ArgumentMustBePositiveInteger</code> is '<code>Argument to function &#39;&#39;{0}&#39;&#39; must be a positive integer literal</code>'
   */
  public final _Def9 ArgumentMustBePositiveInteger = new _Def9(
      "ArgumentMustBePositiveInteger",
      "Argument to function ''{0}'' must be a positive integer literal",
      null);

  /**
   * <code>ValidationError</code> is '<code>Validation Error: {0}</code>'
   */
  public final _Def7 ValidationError = new _Def7("ValidationError",
      "Validation Error: {0}",
      null);

  /**
   * <code>ParserError</code> is '<code>Parser Error: {0}</code>'
   */
  public final _Def7 ParserError = new _Def7("ParserError",
      "Parser Error: {0}",
      null);

  /**
   * <code>ArgumentMustNotBeNull</code> is '<code>Argument to function &#39;&#39;{0}&#39;&#39; must not be NULL</code>'
   */
  public final _Def9 ArgumentMustNotBeNull = new _Def9("ArgumentMustNotBeNull",
      "Argument to function ''{0}'' must not be NULL",
      null);

  /**
   * <code>NullIllegal</code> is '<code>Illegal use of &#39;&#39;NULL&#39;&#39;</code>'
   */
  public final _Def4 NullIllegal = new _Def4("NullIllegal",
      "Illegal use of ''NULL''",
      null);

  /**
   * <code>DynamicParamIllegal</code> is '<code>Illegal use of dynamic parameter</code>'
   */
  public final _Def4 DynamicParamIllegal = new _Def4("DynamicParamIllegal",
      "Illegal use of dynamic parameter",
      null);

  /**
   * <code>InvalidBoolean</code> is '<code>&#39;&#39;{0}&#39;&#39; is not a valid boolean value</code>'
   */
  public final _Def7 InvalidBoolean = new _Def7("InvalidBoolean",
      "''{0}'' is not a valid boolean value",
      null);

  /**
   * <code>ArgumentMustBeValidPrecision</code> is '<code>Argument to function &#39;&#39;{0}&#39;&#39; must be a valid precision between &#39;&#39;{1}&#39;&#39; and &#39;&#39;{2}&#39;&#39;</code>'
   */
  public final _Def14 ArgumentMustBeValidPrecision = new _Def14(
      "ArgumentMustBeValidPrecision",
      "Argument to function ''{0}'' must be a valid precision between ''{1}'' and ''{2}''",
      null);

  /**
   * <code>InvalidDatetimeFormat</code> is '<code>&#39;&#39;{0}&#39;&#39; is not a valid datetime format</code>'
   */
  public final _Def7 InvalidDatetimeFormat = new _Def7("InvalidDatetimeFormat",
      "''{0}'' is not a valid datetime format",
      null);

  /**
   * <code>InsertIntoAlwaysGenerated</code> is '<code>Cannot explicitly insert value into IDENTITY column &#39;&#39;{0}&#39;&#39; which is ALWAYS GENERATED</code>'
   */
  public final _Def7 InsertIntoAlwaysGenerated = new _Def7(
      "InsertIntoAlwaysGenerated",
      "Cannot explicitly insert value into IDENTITY column ''{0}'' which is ALWAYS GENERATED",
      null);

  /**
   * <code>ArgumentMustHaveScaleZero</code> is '<code>Argument to function &#39;&#39;{0}&#39;&#39; must have a scale of 0</code>'
   */
  public final _Def7 ArgumentMustHaveScaleZero = new _Def7(
      "ArgumentMustHaveScaleZero",
      "Argument to function ''{0}'' must have a scale of 0",
      null);

  /**
   * <code>PreparationAborted</code> is '<code>Statement preparation aborted</code>'
   */
  public final _Def6 PreparationAborted = new _Def6("PreparationAborted",
      "Statement preparation aborted",
      null);

  /**
   * <code>SQLFeature_E051_01</code> is '<code>SELECT DISTINCT not supported</code>'
   */
  public final _Def6 SQLFeature_E051_01 = new _Def6("SQLFeature_E051_01",
      "SELECT DISTINCT not supported",
      new String[]{"FeatureDefinition", "SQL:2003 Part 2 Annex F"});

  /**
   * <code>SQLFeature_E071_03</code> is '<code>EXCEPT not supported</code>'
   */
  public final _Def6 SQLFeature_E071_03 = new _Def6("SQLFeature_E071_03",
      "EXCEPT not supported",
      new String[]{"FeatureDefinition", "SQL:2003 Part 2 Annex F"});

  /**
   * <code>SQLFeature_E101_03</code> is '<code>UPDATE not supported</code>'
   */
  public final _Def6 SQLFeature_E101_03 = new _Def6("SQLFeature_E101_03",
      "UPDATE not supported",
      new String[]{"FeatureDefinition", "SQL:2003 Part 2 Annex F"});

  /**
   * <code>SQLFeature_E151</code> is '<code>Transactions not supported</code>'
   */
  public final _Def6 SQLFeature_E151 = new _Def6("SQLFeature_E151",
      "Transactions not supported",
      new String[]{"FeatureDefinition", "SQL:2003 Part 2 Annex F"});

  /**
   * <code>SQLFeature_F302</code> is '<code>INTERSECT not supported</code>'
   */
  public final _Def6 SQLFeature_F302 = new _Def6("SQLFeature_F302",
      "INTERSECT not supported",
      new String[]{"FeatureDefinition", "SQL:2003 Part 2 Annex F"});

  /**
   * <code>SQLFeature_F312</code> is '<code>MERGE not supported</code>'
   */
  public final _Def6 SQLFeature_F312 = new _Def6("SQLFeature_F312",
      "MERGE not supported",
      new String[]{"FeatureDefinition", "SQL:2003 Part 2 Annex F"});

  /**
   * <code>SQLFeature_S271</code> is '<code>Basic multiset not supported</code>'
   */
  public final _Def6 SQLFeature_S271 = new _Def6("SQLFeature_S271",
      "Basic multiset not supported",
      new String[]{"FeatureDefinition", "SQL:2003 Part 2 Annex F"});

  /**
   * <code>SQLFeature_T613</code> is '<code>TABLESAMPLE not supported</code>'
   */
  public final _Def6 SQLFeature_T613 = new _Def6("SQLFeature_T613",
      "TABLESAMPLE not supported",
      new String[]{"FeatureDefinition", "SQL:2003 Part 2 Annex F"});

  /**
   * <code>SQLConformance_MultipleActiveAutocommitStatements</code> is '<code>Execution of a new autocommit statement while a cursor is still open on same connection is not supported</code>'
   */
  public final _Def6 SQLConformance_MultipleActiveAutocommitStatements = new _Def6(
      "SQLConformance_MultipleActiveAutocommitStatements",
      "Execution of a new autocommit statement while a cursor is still open on same connection is not supported",
      new String[]{"FeatureDefinition", "Eigenbase-defined"});

  /**
   * <code>SQLConformance_OrderByDesc</code> is '<code>Descending sort (ORDER BY DESC) not supported</code>'
   */
  public final _Def6 SQLConformance_OrderByDesc = new _Def6(
      "SQLConformance_OrderByDesc",
      "Descending sort (ORDER BY DESC) not supported",
      new String[]{"FeatureDefinition", "Eigenbase-defined"});

  /**
   * <code>SharedStatementPlans</code> is '<code>Sharing of cached statement plans not supported</code>'
   */
  public final _Def6 SharedStatementPlans = new _Def6("SharedStatementPlans",
      "Sharing of cached statement plans not supported",
      new String[]{"FeatureDefinition", "Eigenbase-defined"});

  /**
   * <code>SQLFeatureExt_T613_Substitution</code> is '<code>TABLESAMPLE SUBSTITUTE not supported</code>'
   */
  public final _Def6 SQLFeatureExt_T613_Substitution = new _Def6(
      "SQLFeatureExt_T613_Substitution",
      "TABLESAMPLE SUBSTITUTE not supported",
      new String[]{"FeatureDefinition", "Eigenbase-defined"});

  /**
   * <code>PersonalityManagesRowCount</code> is '<code>Personality does not maintain table&#39;&#39;s row count in the catalog</code>'
   */
  public final _Def6 PersonalityManagesRowCount = new _Def6(
      "PersonalityManagesRowCount",
      "Personality does not maintain table''s row count in the catalog",
      new String[]{"FeatureDefinition", "Eigenbase-defined"});

  /**
   * <code>PersonalitySupportsSnapshots</code> is '<code>Personality does not support snapshot reads</code>'
   */
  public final _Def6 PersonalitySupportsSnapshots = new _Def6(
      "PersonalitySupportsSnapshots",
      "Personality does not support snapshot reads",
      new String[]{"FeatureDefinition", "Eigenbase-defined"});

  /**
   * <code>PersonalitySupportsLabels</code> is '<code>Personality does not support labels</code>'
   */
  public final _Def6 PersonalitySupportsLabels = new _Def6(
      "PersonalitySupportsLabels",
      "Personality does not support labels",
      new String[]{"FeatureDefinition", "Eigenbase-defined"});


  /**
   * Definition for resources which
   * take arguments 'Number p0, Number p1'.
   */
  public final class _Def0 extends org.eigenbase.resgen.ResourceDefinition {
    _Def0(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(Number p0, Number p1) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1}).toString();
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.util.EigenbaseException} exception and
   * take arguments 'String p0, String p1, String p2'.
   */
  public final class _Def1 extends org.eigenbase.resgen.ResourceDefinition {
    _Def1(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0, String p1, String p2) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString();
    }

    public org.eigenbase.util.EigenbaseException ex(String p0, String p1,
        String p2) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString(), null);
    }

    public org.eigenbase.util.EigenbaseException ex(String p0, String p1,
        String p2, Throwable err) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.util.EigenbaseException} exception and
   * take arguments 'String p0, Number p1'.
   */
  public final class _Def2 extends org.eigenbase.resgen.ResourceDefinition {
    _Def2(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0, Number p1) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1}).toString();
    }

    public org.eigenbase.util.EigenbaseException ex(String p0, Number p1) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), null);
    }

    public org.eigenbase.util.EigenbaseException ex(String p0, Number p1,
        Throwable err) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * take arguments 'String p0'.
   */
  public final class _Def3 extends org.eigenbase.resgen.ResourceDefinition {
    _Def3(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0) {
      return instantiate(EigenbaseResource.this, new Object[]{p0}).toString();
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.sql.validate.SqlValidatorException} exception and
   * take arguments ''.
   */
  public final class _Def4 extends org.eigenbase.resgen.ResourceDefinition {
    _Def4(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str() {
      return instantiate(EigenbaseResource.this, emptyObjectArray).toString();
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex() {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          emptyObjectArray).toString(), null);
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(Throwable err) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          emptyObjectArray).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.util.EigenbaseException} exception and
   * take arguments 'String p0, String p1'.
   */
  public final class _Def5 extends org.eigenbase.resgen.ResourceDefinition {
    _Def5(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0, String p1) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1}).toString();
    }

    public org.eigenbase.util.EigenbaseException ex(String p0, String p1) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), null);
    }

    public org.eigenbase.util.EigenbaseException ex(String p0, String p1,
        Throwable err) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.util.EigenbaseException} exception and
   * take arguments ''.
   */
  public final class _Def6 extends org.eigenbase.resgen.ResourceDefinition {
    _Def6(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str() {
      return instantiate(EigenbaseResource.this, emptyObjectArray).toString();
    }

    public org.eigenbase.util.EigenbaseException ex() {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          emptyObjectArray).toString(), null);
    }

    public org.eigenbase.util.EigenbaseException ex(Throwable err) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          emptyObjectArray).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.util.EigenbaseException} exception and
   * take arguments 'String p0'.
   */
  public final class _Def7 extends org.eigenbase.resgen.ResourceDefinition {
    _Def7(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0) {
      return instantiate(EigenbaseResource.this, new Object[]{p0}).toString();
    }

    public org.eigenbase.util.EigenbaseException ex(String p0) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0}).toString(), null);
    }

    public org.eigenbase.util.EigenbaseException ex(String p0, Throwable err) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.util.EigenbaseException} exception and
   * take arguments 'Number p0'.
   */
  public final class _Def8 extends org.eigenbase.resgen.ResourceDefinition {
    _Def8(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(Number p0) {
      return instantiate(EigenbaseResource.this, new Object[]{p0}).toString();
    }

    public org.eigenbase.util.EigenbaseException ex(Number p0) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0}).toString(), null);
    }

    public org.eigenbase.util.EigenbaseException ex(Number p0, Throwable err) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.sql.validate.SqlValidatorException} exception and
   * take arguments 'String p0'.
   */
  public final class _Def9 extends org.eigenbase.resgen.ResourceDefinition {
    _Def9(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0) {
      return instantiate(EigenbaseResource.this, new Object[]{p0}).toString();
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0}).toString(), null);
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        Throwable err) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.sql.validate.SqlValidatorException} exception and
   * take arguments 'String p0, Number p1'.
   */
  public final class _Def10 extends org.eigenbase.resgen.ResourceDefinition {
    _Def10(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0, Number p1) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1}).toString();
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        Number p1) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), null);
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        Number p1, Throwable err) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.util.EigenbaseContextException} exception and
   * take arguments 'Number p0, Number p1'.
   */
  public final class _Def11 extends org.eigenbase.resgen.ResourceDefinition {
    _Def11(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(Number p0, Number p1) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1}).toString();
    }

    public org.eigenbase.util.EigenbaseContextException ex(Number p0,
        Number p1) {
      return new org.eigenbase.util.EigenbaseContextException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), null);
    }

    public org.eigenbase.util.EigenbaseContextException ex(Number p0, Number p1,
        Throwable err) {
      return new org.eigenbase.util.EigenbaseContextException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.util.EigenbaseContextException} exception and
   * take arguments 'Number p0, Number p1, Number p2, Number p3'.
   */
  public final class _Def12 extends org.eigenbase.resgen.ResourceDefinition {
    _Def12(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(Number p0, Number p1, Number p2, Number p3) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1, p2, p3}).toString();
    }

    public org.eigenbase.util.EigenbaseContextException ex(Number p0, Number p1,
        Number p2, Number p3) {
      return new org.eigenbase.util.EigenbaseContextException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2, p3}).toString(), null);
    }

    public org.eigenbase.util.EigenbaseContextException ex(Number p0, Number p1,
        Number p2, Number p3, Throwable err) {
      return new org.eigenbase.util.EigenbaseContextException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2, p3}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.sql.validate.SqlValidatorException} exception and
   * take arguments 'String p0, String p1'.
   */
  public final class _Def13 extends org.eigenbase.resgen.ResourceDefinition {
    _Def13(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0, String p1) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1}).toString();
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        String p1) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), null);
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        String p1, Throwable err) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.sql.validate.SqlValidatorException} exception and
   * take arguments 'String p0, String p1, String p2'.
   */
  public final class _Def14 extends org.eigenbase.resgen.ResourceDefinition {
    _Def14(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0, String p1, String p2) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString();
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        String p1, String p2) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString(), null);
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        String p1, String p2, Throwable err) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.sql.validate.SqlValidatorException} exception and
   * take arguments 'Number p0, Number p1'.
   */
  public final class _Def15 extends org.eigenbase.resgen.ResourceDefinition {
    _Def15(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(Number p0, Number p1) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1}).toString();
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(Number p0,
        Number p1) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), null);
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(Number p0,
        Number p1, Throwable err) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.sql.validate.SqlValidatorException} exception and
   * take arguments 'String p0, String p1, String p2, String p3'.
   */
  public final class _Def16 extends org.eigenbase.resgen.ResourceDefinition {
    _Def16(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0, String p1, String p2, String p3) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1, p2, p3}).toString();
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        String p1, String p2, String p3) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2, p3}).toString(), null);
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        String p1, String p2, String p3, Throwable err) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2, p3}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.sql.validate.SqlValidatorException} exception and
   * take arguments 'String p0, Number p1, String p2'.
   */
  public final class _Def17 extends org.eigenbase.resgen.ResourceDefinition {
    _Def17(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0, Number p1, String p2) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString();
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        Number p1, String p2) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString(), null);
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(String p0,
        Number p1, String p2, Throwable err) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.util.EigenbaseException} exception and
   * take arguments 'String p0, String p1, String p2, String p3'.
   */
  public final class _Def18 extends org.eigenbase.resgen.ResourceDefinition {
    _Def18(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(String p0, String p1, String p2, String p3) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1, p2, p3}).toString();
    }

    public org.eigenbase.util.EigenbaseException ex(String p0, String p1,
        String p2, String p3) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2, p3}).toString(), null);
    }

    public org.eigenbase.util.EigenbaseException ex(String p0, String p1,
        String p2, String p3, Throwable err) {
      return new org.eigenbase.util.EigenbaseException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2, p3}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.sql.validate.SqlValidatorException} exception and
   * take arguments 'Number p0, String p1, Number p2'.
   */
  public final class _Def19 extends org.eigenbase.resgen.ResourceDefinition {
    _Def19(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(Number p0, String p1, Number p2) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString();
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(Number p0,
        String p1, Number p2) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString(), null);
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(Number p0,
        String p1, Number p2, Throwable err) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1, p2}).toString(), err);
    }
  }

  /**
   * Definition for resources which
   * return a {@link org.eigenbase.sql.validate.SqlValidatorException} exception and
   * take arguments 'Number p0, String p1'.
   */
  public final class _Def20 extends org.eigenbase.resgen.ResourceDefinition {
    _Def20(String key, String baseMessage, String[] props) {
      super(key, baseMessage, props);
    }

    public String str(Number p0, String p1) {
      return instantiate(EigenbaseResource.this,
          new Object[]{p0, p1}).toString();
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(Number p0,
        String p1) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), null);
    }

    public org.eigenbase.sql.validate.SqlValidatorException ex(Number p0,
        String p1, Throwable err) {
      return new org.eigenbase.sql.validate.SqlValidatorException(instantiate(
          EigenbaseResource.this,
          new Object[]{p0, p1}).toString(), err);
    }
  }

}
