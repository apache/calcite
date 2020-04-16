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

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BarfingInvocationHandler;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.Glossary;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Predicates;
import com.google.common.base.Utf8;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Contains utility functions related to SQL parsing, all static.
 */
public abstract class SqlUtil {
  //~ Methods ----------------------------------------------------------------

  static SqlNode andExpressions(
      SqlNode node1,
      SqlNode node2) {
    if (node1 == null) {
      return node2;
    }
    ArrayList<SqlNode> list = new ArrayList<>();
    if (node1.getKind() == SqlKind.AND) {
      list.addAll(((SqlCall) node1).getOperandList());
    } else {
      list.add(node1);
    }
    if (node2.getKind() == SqlKind.AND) {
      list.addAll(((SqlCall) node2).getOperandList());
    } else {
      list.add(node2);
    }
    return SqlStdOperatorTable.AND.createCall(
        SqlParserPos.ZERO,
        list);
  }

  static ArrayList<SqlNode> flatten(SqlNode node) {
    ArrayList<SqlNode> list = new ArrayList<>();
    flatten(node, list);
    return list;
  }

  /**
   * Returns the <code>n</code>th (0-based) input to a join expression.
   */
  public static SqlNode getFromNode(
      SqlSelect query,
      int ordinal) {
    ArrayList<SqlNode> list = flatten(query.getFrom());
    return list.get(ordinal);
  }

  private static void flatten(
      SqlNode node,
      ArrayList<SqlNode> list) {
    switch (node.getKind()) {
    case JOIN:
      SqlJoin join = (SqlJoin) node;
      flatten(
          join.getLeft(),
          list);
      flatten(
          join.getRight(),
          list);
      return;
    case AS:
      SqlCall call = (SqlCall) node;
      flatten(call.operand(0), list);
      return;
    default:
      list.add(node);
      return;
    }
  }

  /**
   * Converts an SqlNode array to a SqlNodeList
   */
  public static SqlNodeList toNodeList(SqlNode[] operands) {
    SqlNodeList ret = new SqlNodeList(SqlParserPos.ZERO);
    for (SqlNode node : operands) {
      ret.add(node);
    }
    return ret;
  }

  /**
   * Returns whether a node represents the NULL value.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>For {@link SqlLiteral} Unknown, returns false.
   * <li>For <code>CAST(NULL AS <i>type</i>)</code>, returns true if <code>
   * allowCast</code> is true, false otherwise.
   * <li>For <code>CAST(CAST(NULL AS <i>type</i>) AS <i>type</i>))</code>,
   * returns false.
   * </ul>
   */
  public static boolean isNullLiteral(
      SqlNode node,
      boolean allowCast) {
    if (node instanceof SqlLiteral) {
      SqlLiteral literal = (SqlLiteral) node;
      if (literal.getTypeName() == SqlTypeName.NULL) {
        assert null == literal.getValue();
        return true;
      } else {
        // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as
        // NULL.
        return false;
      }
    }
    if (allowCast) {
      if (node.getKind() == SqlKind.CAST) {
        SqlCall call = (SqlCall) node;
        if (isNullLiteral(call.operand(0), false)) {
          // node is "CAST(NULL as type)"
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns whether a node represents the NULL value or a series of nested
   * <code>CAST(NULL AS type)</code> calls. For example:
   * <code>isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1)))</code>
   * returns {@code true}.
   */
  public static boolean isNull(SqlNode node) {
    return isNullLiteral(node, false)
        || node.getKind() == SqlKind.CAST
            && isNull(((SqlCall) node).operand(0));
  }

  /**
   * Returns whether a node is a literal.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>For <code>CAST(literal AS <i>type</i>)</code>, returns true if <code>
   * allowCast</code> is true, false otherwise.
   * <li>For <code>CAST(CAST(literal AS <i>type</i>) AS <i>type</i>))</code>,
   * returns false.
   * </ul>
   *
   * @param node The node, never null.
   * @param allowCast whether to regard CAST(literal) as a literal
   * @return Whether the node is a literal
   */
  public static boolean isLiteral(SqlNode node, boolean allowCast) {
    assert node != null;
    if (node instanceof SqlLiteral) {
      return true;
    }
    if (allowCast) {
      if (node.getKind() == SqlKind.CAST) {
        SqlCall call = (SqlCall) node;
        if (isLiteral(call.operand(0), false)) {
          // node is "CAST(literal as type)"
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns whether a node is a literal.
   *
   * <p>Many constructs which require literals also accept <code>CAST(NULL AS
   * <i>type</i>)</code>. This method does not accept casts, so you should
   * call {@link #isNullLiteral} first.
   *
   * @param node The node, never null.
   * @return Whether the node is a literal
   */
  public static boolean isLiteral(SqlNode node) {
    return isLiteral(node, false);
  }

  /**
   * Returns whether a node is a literal chain which is used to represent a
   * continued string literal.
   *
   * @param node The node, never null.
   * @return Whether the node is a literal chain
   */
  public static boolean isLiteralChain(SqlNode node) {
    assert node != null;
    if (node instanceof SqlCall) {
      SqlCall call = (SqlCall) node;
      return call.getKind() == SqlKind.LITERAL_CHAIN;
    } else {
      return false;
    }
  }

  /**
   * Unparses a call to an operator which has function syntax.
   *
   * @param operator    The operator
   * @param writer      Writer
   * @param call    List of 0 or more operands
   */
  public static void unparseFunctionSyntax(
      SqlOperator operator,
      SqlWriter writer,
      SqlCall call) {
    if (operator instanceof SqlFunction) {
      SqlFunction function = (SqlFunction) operator;

      if (function.getFunctionType().isSpecific()) {
        writer.keyword("SPECIFIC");
      }
      SqlIdentifier id = function.getSqlIdentifier();
      if (id == null) {
        writer.keyword(operator.getName());
      } else {
        unparseSqlIdentifierSyntax(writer, id, true);
      }
    } else {
      writer.print(operator.getName());
    }
    if (call.operandCount() == 0) {
      switch (call.getOperator().getSyntax()) {
      case FUNCTION_ID:
        // For example, the "LOCALTIME" function appears as "LOCALTIME"
        // when it has 0 args, not "LOCALTIME()".
        return;
      case FUNCTION_STAR: // E.g. "COUNT(*)"
      case FUNCTION: // E.g. "RANK()"
        // fall through - dealt with below
      }
    }
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    final SqlLiteral quantifier = call.getFunctionQuantifier();
    if (quantifier != null) {
      quantifier.unparse(writer, 0, 0);
    }
    if (call.operandCount() == 0) {
      switch (call.getOperator().getSyntax()) {
      case FUNCTION_STAR:
        writer.sep("*");
      }
    }
    for (SqlNode operand : call.getOperandList()) {
      writer.sep(",");
      operand.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }

  /**
   * Unparse a SqlIdentifier syntax.
   *
   * @param writer       Writer
   * @param identifier   SqlIdentifier
   * @param asFunctionID Whether this identifier comes from a SqlFunction
   */
  public static void unparseSqlIdentifierSyntax(
      SqlWriter writer,
      SqlIdentifier identifier,
      boolean asFunctionID) {
    final boolean isUnquotedSimple = identifier.isSimple()
        && !identifier.getParserPosition().isQuoted();
    final SqlOperator operator = isUnquotedSimple
        ? SqlValidatorUtil.lookupSqlFunctionByID(SqlStdOperatorTable.instance(), identifier, null)
        : null;
    boolean unparsedAsFunc = false;
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.IDENTIFIER);
    if (isUnquotedSimple && operator != null) {
      // Unparse conditions:
      // 1. If the identifier is quoted or is component, unparse as normal.
      // 2. If the identifier comes from a sql function, lookup in the
      // standard sql operator table to see if the function is a builtin,
      // unparse without quoting for builtins.

      // 3. If the identifier does not come from a function(resolved as a SqlIdentifier),
      // look up in the standard sql operator table to see if it is a function
      // with empty argument list, e.g. LOCALTIME, we should not quote
      // such identifier cause quoted `LOCALTIME` always represents a sql identifier.
      if (asFunctionID
          || operator.getSyntax() == SqlSyntax.FUNCTION_ID) {
        writer.keyword(identifier.getSimple());
        unparsedAsFunc = true;
      }
    }

    if (!unparsedAsFunc) {
      for (int i = 0; i < identifier.names.size(); i++) {
        writer.sep(".");
        final String name = identifier.names.get(i);
        final SqlParserPos pos = identifier.getComponentParserPosition(i);
        if (name.equals("")) {
          writer.print("*");
        } else {
          writer.identifier(name, pos.isQuoted());
        }
      }
    }
    if (null != identifier.getCollation()) {
      identifier.getCollation().unparse(writer);
    }
    writer.endList(frame);
  }

  public static void unparseBinarySyntax(
      SqlOperator operator,
      SqlCall call,
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 2;
    final SqlWriter.Frame frame =
        writer.startList(
            (operator instanceof SqlSetOperator)
                ? SqlWriter.FrameTypeEnum.SETOP
                : SqlWriter.FrameTypeEnum.SIMPLE);
    call.operand(0).unparse(writer, leftPrec, operator.getLeftPrec());
    final boolean needsSpace = operator.needsSpace();
    writer.setNeedWhitespace(needsSpace);
    writer.sep(operator.getName());
    writer.setNeedWhitespace(needsSpace);
    call.operand(1).unparse(writer, operator.getRightPrec(), rightPrec);
    writer.endList(frame);
  }

  /**
   * Concatenates string literals.
   *
   * <p>This method takes an array of arguments, since pairwise concatenation
   * means too much string copying.
   *
   * @param lits an array of {@link SqlLiteral}, not empty, all of the same
   *             class
   * @return a new {@link SqlLiteral}, of that same class, whose value is the
   * string concatenation of the values of the literals
   * @throws ClassCastException             if the lits are not homogeneous.
   * @throws ArrayIndexOutOfBoundsException if lits is an empty array.
   */
  public static SqlLiteral concatenateLiterals(List<SqlLiteral> lits) {
    if (lits.size() == 1) {
      return lits.get(0); // nothing to do
    }
    return ((SqlAbstractStringLiteral) lits.get(0)).concat1(lits);
  }

  /**
   * Looks up a (possibly overloaded) routine based on name and argument
   * types.
   *
   * @param opTab         operator table to search
   * @param funcName      name of function being invoked
   * @param argTypes      argument types
   * @param argNames      argument names, or null if call by position
   * @param category      whether a function or a procedure. (If a procedure is
   *                      being invoked, the overload rules are simpler.)
   * @param nameMatcher   Whether to look up the function case-sensitively
   * @param coerce        Whether to allow type coercion when do filter routines
   *                      by parameter types
   * @return matching routine, or null if none found
   *
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 10.4
   */
  public static SqlOperator lookupRoutine(SqlOperatorTable opTab,
      SqlIdentifier funcName, List<RelDataType> argTypes,
      List<String> argNames, SqlFunctionCategory category,
      SqlSyntax syntax, SqlKind sqlKind, SqlNameMatcher nameMatcher,
      boolean coerce) {
    Iterator<SqlOperator> list =
        lookupSubjectRoutines(
            opTab,
            funcName,
            argTypes,
            argNames,
            syntax,
            sqlKind,
            category,
            nameMatcher,
            coerce);
    if (list.hasNext()) {
      // return first on schema path
      return list.next();
    }
    return null;
  }

  private static Iterator<SqlOperator> filterOperatorRoutinesByKind(
      Iterator<SqlOperator> routines, final SqlKind sqlKind) {
    return Iterators.filter(routines,
        operator -> Objects.requireNonNull(operator).getKind() == sqlKind);
  }

  /**
   * Looks up all subject routines matching the given name and argument types.
   *
   * @param opTab       operator table to search
   * @param funcName    name of function being invoked
   * @param argTypes    argument types
   * @param argNames    argument names, or null if call by position
   * @param sqlSyntax   the SqlSyntax of the SqlOperator being looked up
   * @param sqlKind     the SqlKind of the SqlOperator being looked up
   * @param category    Category of routine to look up
   * @param nameMatcher Whether to look up the function case-sensitively
   * @param coerce      Whether to allow type coercion when do filter routine
   *                    by parameter types
   * @return list of matching routines
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 10.4
   */
  public static Iterator<SqlOperator> lookupSubjectRoutines(
      SqlOperatorTable opTab,
      SqlIdentifier funcName,
      List<RelDataType> argTypes,
      List<String> argNames,
      SqlSyntax sqlSyntax,
      SqlKind sqlKind,
      SqlFunctionCategory category,
      SqlNameMatcher nameMatcher,
      boolean coerce) {
    // start with all routines matching by name
    Iterator<SqlOperator> routines =
        lookupSubjectRoutinesByName(opTab, funcName, sqlSyntax, category,
            nameMatcher);

    // first pass:  eliminate routines which don't accept the given
    // number of arguments
    routines = filterRoutinesByParameterCount(routines, argTypes);

    // NOTE: according to SQL99, procedures are NOT overloaded on type,
    // only on number of arguments.
    if (category == SqlFunctionCategory.USER_DEFINED_PROCEDURE) {
      return routines;
    }

    // second pass:  eliminate routines which don't accept the given
    // argument types and parameter names if specified
    routines =
        filterRoutinesByParameterTypeAndName(sqlSyntax, routines, argTypes, argNames, coerce);

    // see if we can stop now; this is necessary for the case
    // of builtin functions where we don't have param type info,
    // or UDF whose operands can make type coercion.
    final List<SqlOperator> list = Lists.newArrayList(routines);
    routines = list.iterator();
    if (list.size() < 2 || coerce) {
      return routines;
    }

    // third pass:  for each parameter from left to right, eliminate
    // all routines except those with the best precedence match for
    // the given arguments
    routines = filterRoutinesByTypePrecedence(sqlSyntax, routines, argTypes, argNames);

    // fourth pass: eliminate routines which do not have the same
    // SqlKind as requested
    return filterOperatorRoutinesByKind(routines, sqlKind);
  }

  /**
   * Determines whether there is a routine matching the given name and number
   * of arguments.
   *
   * @param opTab         operator table to search
   * @param funcName      name of function being invoked
   * @param argTypes      argument types
   * @param category      category of routine to look up
   * @param nameMatcher   Whether to look up the function case-sensitively
   * @return true if match found
   */
  public static boolean matchRoutinesByParameterCount(
      SqlOperatorTable opTab,
      SqlIdentifier funcName,
      List<RelDataType> argTypes,
      SqlFunctionCategory category,
      SqlNameMatcher nameMatcher) {
    // start with all routines matching by name
    Iterator<SqlOperator> routines =
        lookupSubjectRoutinesByName(opTab, funcName, SqlSyntax.FUNCTION,
            category, nameMatcher);

    // first pass:  eliminate routines which don't accept the given
    // number of arguments
    routines = filterRoutinesByParameterCount(routines, argTypes);

    return routines.hasNext();
  }

  private static Iterator<SqlOperator> lookupSubjectRoutinesByName(
      SqlOperatorTable opTab,
      SqlIdentifier funcName,
      final SqlSyntax syntax,
      SqlFunctionCategory category,
      SqlNameMatcher nameMatcher) {
    final List<SqlOperator> sqlOperators = new ArrayList<>();
    opTab.lookupOperatorOverloads(funcName, category, syntax, sqlOperators,
        nameMatcher);
    switch (syntax) {
    case FUNCTION:
      return Iterators.filter(sqlOperators.iterator(),
          Predicates.instanceOf(SqlFunction.class));
    default:
      return Iterators.filter(sqlOperators.iterator(),
          operator -> Objects.requireNonNull(operator).getSyntax() == syntax);
    }
  }

  private static Iterator<SqlOperator> filterRoutinesByParameterCount(
      Iterator<SqlOperator> routines,
      final List<RelDataType> argTypes) {
    return Iterators.filter(routines,
        operator -> Objects.requireNonNull(operator)
            .getOperandCountRange().isValidCount(argTypes.size()));
  }

  /**
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 10.4 Syntax Rule 6.b.iii.2.B
   */
  private static Iterator<SqlOperator> filterRoutinesByParameterTypeAndName(
      SqlSyntax syntax,
      final Iterator<SqlOperator> routines,
      final List<RelDataType> argTypes,
      final List<String> argNames,
      final boolean coerce) {
    if (syntax != SqlSyntax.FUNCTION) {
      return routines;
    }

    //noinspection unchecked
    return (Iterator) Iterators.filter(
        Iterators.filter(routines, SqlFunction.class),
        function -> {
          List<RelDataType> paramTypes = function.getParamTypes();
          if (paramTypes == null) {
            // no parameter information for builtins; keep for now,
            // the type coerce will not work here.
            return true;
          }
          final List<RelDataType> permutedArgTypes;
          if (argNames != null) {
            permutedArgTypes = permuteArgTypes(function, argNames, argTypes);
            if (permutedArgTypes == null) {
              return false;
            }
          } else {
            permutedArgTypes = Lists.newArrayList(argTypes);
            while (permutedArgTypes.size() < argTypes.size()) {
              paramTypes.add(null);
            }
          }
          for (Pair<RelDataType, RelDataType> p
              : Pair.zip(paramTypes, permutedArgTypes)) {
            final RelDataType argType = p.right;
            final RelDataType paramType = p.left;
            if (argType != null
                && !SqlTypeUtil.canCastFrom(paramType, argType, coerce)) {
              return false;
            }
          }
          return true;
        });
  }

  /**
   * Permutes argument types to correspond to the order of parameter names.
   */
  private static List<RelDataType> permuteArgTypes(SqlFunction function,
      List<String> argNames, List<RelDataType> argTypes) {
    // Arguments passed by name. Make sure that the function has
    // parameters of all of these names.
    Map<Integer, Integer> map = new HashMap<>();
    for (Ord<String> argName : Ord.zip(argNames)) {
      int i = function.getParamNames().indexOf(argName.e);
      if (i < 0) {
        return null;
      }
      map.put(i, argName.i);
    }
    return Functions.generate(function.getParamTypes().size(), index -> {
      if (map.containsKey(index)) {
        return argTypes.get(map.get(index));
      } else {
        return null;
      }
    });
  }

  /**
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 9.4
   */
  private static Iterator<SqlOperator> filterRoutinesByTypePrecedence(
      SqlSyntax sqlSyntax,
      Iterator<SqlOperator> routines,
      List<RelDataType> argTypes,
      List<String> argNames) {
    if (sqlSyntax != SqlSyntax.FUNCTION) {
      return routines;
    }

    List<SqlFunction> sqlFunctions =
        Lists.newArrayList(Iterators.filter(routines, SqlFunction.class));

    for (final Ord<RelDataType> argType : Ord.zip(argTypes)) {
      final RelDataTypePrecedenceList precList =
          argType.e.getPrecedenceList();
      final RelDataType bestMatch = bestMatch(sqlFunctions, argType.i, argNames, precList);
      if (bestMatch != null) {
        sqlFunctions = sqlFunctions.stream()
            .filter(function -> {
              final List<RelDataType> paramTypes = function.getParamTypes();
              if (paramTypes == null) {
                return false;
              }
              int index = argNames != null
                  ? function.getParamNames().indexOf(argNames.get(argType.i))
                  : argType.i;
              final RelDataType paramType = paramTypes.get(index);
              return precList.compareTypePrecedence(paramType, bestMatch) >= 0;
            })
            .collect(Collectors.toList());
      }
    }
    //noinspection unchecked
    return (Iterator) sqlFunctions.iterator();
  }

  private static RelDataType bestMatch(List<SqlFunction> sqlFunctions, int i,
      List<String> argNames, RelDataTypePrecedenceList precList) {
    RelDataType bestMatch = null;
    for (SqlFunction function : sqlFunctions) {
      List<RelDataType> paramTypes = function.getParamTypes();
      if (paramTypes == null) {
        continue;
      }
      final RelDataType paramType = argNames != null
          ? paramTypes.get(function.getParamNames().indexOf(argNames.get(i)))
          : paramTypes.get(i);
      if (bestMatch == null) {
        bestMatch = paramType;
      } else {
        int c =
            precList.compareTypePrecedence(
                bestMatch,
                paramType);
        if (c < 0) {
          bestMatch = paramType;
        }
      }
    }
    return bestMatch;
  }

  /**
   * Returns the <code>i</code>th select-list item of a query.
   */
  public static SqlNode getSelectListItem(SqlNode query, int i) {
    switch (query.getKind()) {
    case SELECT:
      SqlSelect select = (SqlSelect) query;
      final SqlNode from = stripAs(select.getFrom());
      if (from != null && from.getKind() == SqlKind.VALUES) {
        // They wrote "VALUES (x, y)", but the validator has
        // converted this into "SELECT * FROM VALUES (x, y)".
        return getSelectListItem(from, i);
      }
      final SqlNodeList fields = select.getSelectList();

      // Range check the index to avoid index out of range.  This
      // could be expanded to actually check to see if the select
      // list is a "*"
      if (i >= fields.size()) {
        i = 0;
      }
      return fields.get(i);

    case VALUES:
      SqlCall call = (SqlCall) query;
      assert call.operandCount() > 0
          : "VALUES must have at least one operand";
      final SqlCall row = call.operand(0);
      assert row.operandCount() > i : "VALUES has too few columns";
      return row.operand(i);

    default:
      // Unexpected type of query.
      throw Util.needToImplement(query);
    }
  }

  public static String deriveAliasFromOrdinal(int ordinal) {
    // Use a '$' so that queries can't easily reference the
    // generated name.
    return "EXPR$" + ordinal;
  }

  /**
   * Constructs an operator signature from a type list.
   *
   * @param op       operator
   * @param typeList list of types to use for operands. Types may be
   *                 represented as {@link String}, {@link SqlTypeFamily}, or
   *                 any object with a valid {@link Object#toString()} method.
   * @return constructed signature
   */
  public static String getOperatorSignature(SqlOperator op, List<?> typeList) {
    return getAliasedSignature(op, op.getName(), typeList);
  }

  /**
   * Constructs an operator signature from a type list, substituting an alias
   * for the operator name.
   *
   * @param op       operator
   * @param opName   name to use for operator
   * @param typeList list of {@link SqlTypeName} or {@link String} to use for
   *                 operands
   * @return constructed signature
   */
  public static String getAliasedSignature(
      SqlOperator op,
      String opName,
      List<?> typeList) {
    StringBuilder ret = new StringBuilder();
    String template = op.getSignatureTemplate(typeList.size());
    if (null == template) {
      ret.append("'");
      ret.append(opName);
      ret.append("(");
      for (int i = 0; i < typeList.size(); i++) {
        if (i > 0) {
          ret.append(", ");
        }
        final String t = typeList.get(i).toString().toUpperCase(Locale.ROOT);
        ret.append("<").append(t).append(">");
      }
      ret.append(")'");
    } else {
      Object[] values = new Object[typeList.size() + 1];
      values[0] = opName;
      ret.append("'");
      for (int i = 0; i < typeList.size(); i++) {
        final String t = typeList.get(i).toString().toUpperCase(Locale.ROOT);
        values[i + 1] = "<" + t + ">";
      }
      ret.append(new MessageFormat(template, Locale.ROOT).format(values));
      ret.append("'");
      assert (typeList.size() + 1) == values.length;
    }

    return ret.toString();
  }

  /**
   * Wraps an exception with context.
   */
  public static CalciteException newContextException(
      final SqlParserPos pos,
      Resources.ExInst<?> e,
      String inputText) {
    CalciteContextException ex = newContextException(pos, e);
    ex.setOriginalStatement(inputText);
    return ex;
  }

  /**
   * Wraps an exception with context.
   */
  public static CalciteContextException newContextException(
      final SqlParserPos pos,
      Resources.ExInst<?> e) {
    int line = pos.getLineNum();
    int col = pos.getColumnNum();
    int endLine = pos.getEndLineNum();
    int endCol = pos.getEndColumnNum();
    return newContextException(line, col, endLine, endCol, e);
  }

  /**
   * Wraps an exception with context.
   */
  public static CalciteContextException newContextException(
      int line,
      int col,
      int endLine,
      int endCol,
      Resources.ExInst<?> e) {
    CalciteContextException contextExcn =
        (line == endLine && col == endCol
            ? RESOURCE.validatorContextPoint(line, col)
            : RESOURCE.validatorContext(line, col, endLine, endCol)).ex(e.ex());
    contextExcn.setPosition(line, col, endLine, endCol);
    return contextExcn;
  }

  /**
   * Returns whether a {@link SqlNode node} is a {@link SqlCall call} to a
   * given {@link SqlOperator operator}.
   */
  public static boolean isCallTo(SqlNode node, SqlOperator operator) {
    return (node instanceof SqlCall)
        && (((SqlCall) node).getOperator() == operator);
  }

  /**
   * Creates the type of an {@link org.apache.calcite.util.NlsString}.
   *
   * <p>The type inherits the The NlsString's {@link Charset} and
   * {@link SqlCollation}, if they are set, otherwise it gets the system
   * defaults.
   *
   * @param typeFactory Type factory
   * @param str         String
   * @return Type, including collation and charset
   */
  public static RelDataType createNlsStringType(
      RelDataTypeFactory typeFactory,
      NlsString str) {
    Charset charset = str.getCharset();
    if (null == charset) {
      charset = typeFactory.getDefaultCharset();
    }
    SqlCollation collation = str.getCollation();
    if (null == collation) {
      collation = SqlCollation.COERCIBLE;
    }
    RelDataType type =
        typeFactory.createSqlType(
            SqlTypeName.CHAR,
            str.getValue().length());
    type =
        typeFactory.createTypeWithCharsetAndCollation(
            type,
            charset,
            collation);
    return type;
  }

  /**
   * Translates a character set name from a SQL-level name into a Java-level
   * name.
   *
   * @param name SQL-level name
   * @return Java-level name, or null if SQL-level name is unknown
   */
  public static String translateCharacterSetName(String name) {
    switch (name) {
    case "BIG5":
      return "Big5";
    case "LATIN1":
      return "ISO-8859-1";
    case "GB2312":
    case "GBK":
      return name;
    case "UTF8":
      return "UTF-8";
    case "UTF16":
      return ConversionUtil.NATIVE_UTF16_CHARSET_NAME;
    case "UTF-16BE":
    case "UTF-16LE":
    case "ISO-8859-1":
    case "UTF-8":
      return name;
    default:
      return null;
    }
  }

  /**
   * Returns the Java-level {@link Charset} based on given SQL-level name.
   *
   * @param charsetName Sql charset name, must not be null.
   * @return charset, or default charset if charsetName is null.
   * @throws UnsupportedCharsetException If no support for the named charset
   *     is available in this instance of the Java virtual machine
   */
  public static Charset getCharset(String charsetName) {
    assert charsetName != null;
    charsetName = charsetName.toUpperCase(Locale.ROOT);
    String javaCharsetName = translateCharacterSetName(charsetName);
    if (javaCharsetName == null) {
      throw new UnsupportedCharsetException(charsetName);
    }
    return Charset.forName(javaCharsetName);
  }

  /**
   * Validate if value can be decoded by given charset.
   *
   * @param value nls string in byte array
   * @param charset charset
   * @throws RuntimeException If the given value cannot be represented in the
   *     given charset
   */
  public static void validateCharset(ByteString value, Charset charset) {
    if (charset == StandardCharsets.UTF_8) {
      final byte[] bytes = value.getBytes();
      if (!Utf8.isWellFormed(bytes)) {
        //CHECKSTYLE: IGNORE 1
        final String string = new String(bytes, charset);
        throw RESOURCE.charsetEncoding(string, charset.name()).ex();
      }
    }
  }

  /** If a node is "AS", returns the underlying expression; otherwise returns
   * the node. */
  public static SqlNode stripAs(SqlNode node) {
    if (node != null && node.getKind() == SqlKind.AS) {
      return ((SqlCall) node).operand(0);
    }
    return node;
  }

  /** Modifies a list of nodes, removing AS from each if present.
   *
   * @see #stripAs */
  public static SqlNodeList stripListAs(SqlNodeList nodeList) {
    for (int i = 0; i < nodeList.size(); i++) {
      SqlNode n = nodeList.get(i);
      SqlNode n2 = stripAs(n);
      if (n != n2) {
        nodeList.set(i, n2);
      }
    }
    return nodeList;
  }

  /** Returns a list of ancestors of {@code predicate} within a given
   * {@code SqlNode} tree.
   *
   * <p>The first element of the list is {@code root}, and the last is
   * the node that matched {@code predicate}. Throws if no node matches.
   */
  public static ImmutableList<SqlNode> getAncestry(SqlNode root,
      Predicate<SqlNode> predicate, Predicate<SqlNode> postPredicate) {
    try {
      new Genealogist(predicate, postPredicate).visitChild(root);
      throw new AssertionError("not found: " + predicate + " in " + root);
    } catch (Util.FoundOne e) {
      //noinspection unchecked
      return (ImmutableList<SqlNode>) e.getNode();
    }
  }

  /**
   * Returns an immutable list of {@link RelHint} from sql hints, with a given
   * inherit path from the root node.
   *
   * <p>The inherit path would be empty list.
   *
   * @param hintStrategies The hint strategies to validate the sql hints
   * @param sqlHints       The sql hints nodes
   * @return the {@code RelHint} list
   */
  public static List<RelHint> getRelHint(HintStrategyTable hintStrategies, SqlNodeList sqlHints) {
    if (sqlHints == null || sqlHints.size() == 0) {
      return ImmutableList.of();
    }
    final ImmutableList.Builder<RelHint> relHints = ImmutableList.builder();
    for (SqlNode node : sqlHints) {
      assert node instanceof SqlHint;
      final SqlHint sqlHint = (SqlHint) node;
      final String hintName = sqlHint.getName();

      final RelHint.Builder builder = RelHint.builder(hintName);
      switch (sqlHint.getOptionFormat()) {
      case EMPTY:
        // do nothing.
        break;
      case LITERAL_LIST:
      case ID_LIST:
        builder.hintOptions(sqlHint.getOptionList());
        break;
      case KV_LIST:
        builder.hintOptions(sqlHint.getOptionKVPairs());
        break;
      default:
        throw new AssertionError("Unexpected hint option format");
      }
      final RelHint relHint = builder.build();
      if (hintStrategies.validateHint(relHint)) {
        // Skips the hint if the validation fails.
        relHints.add(relHint);
      }
    }
    return relHints.build();
  }

  /**
   * Attach the {@code hints} to {@code rel} with specified hint strategies.
   *
   * @param hintStrategies The strategies to filter the hints
   * @param hints          The original hints to be attached
   * @return A copy of {@code rel} if there are any hints can be attached given
   * the hint strategies, or the original node if such hints don't exist
   */
  public static RelNode attachRelHint(
      HintStrategyTable hintStrategies,
      List<RelHint> hints,
      Hintable rel) {
    final List<RelHint> relHints = hintStrategies.apply(hints, (RelNode) rel);
    if (relHints.size() > 0) {
      return rel.attachHints(relHints);
    }
    return (RelNode) rel;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Handles particular {@link DatabaseMetaData} methods; invocations of other
   * methods will fall through to the base class,
   * {@link org.apache.calcite.util.BarfingInvocationHandler}, which will throw
   * an error.
   */
  public static class DatabaseMetaDataInvocationHandler
      extends BarfingInvocationHandler {
    private final String databaseProductName;
    private final String identifierQuoteString;

    public DatabaseMetaDataInvocationHandler(
        String databaseProductName,
        String identifierQuoteString) {
      this.databaseProductName = databaseProductName;
      this.identifierQuoteString = identifierQuoteString;
    }

    public String getDatabaseProductName() throws SQLException {
      return databaseProductName;
    }

    public String getIdentifierQuoteString() throws SQLException {
      return identifierQuoteString;
    }
  }

  /** Walks over a {@link org.apache.calcite.sql.SqlNode} tree and returns the
   * ancestry stack when it finds a given node. */
  private static class Genealogist extends SqlBasicVisitor<Void> {
    private final List<SqlNode> ancestors = new ArrayList<>();
    private final Predicate<SqlNode> predicate;
    private final Predicate<SqlNode> postPredicate;

    Genealogist(Predicate<SqlNode> predicate,
        Predicate<SqlNode> postPredicate) {
      this.predicate = predicate;
      this.postPredicate = postPredicate;
    }

    private Void check(SqlNode node) {
      preCheck(node);
      postCheck(node);
      return null;
    }

    private Void preCheck(SqlNode node) {
      if (predicate.test(node)) {
        throw new Util.FoundOne(ImmutableList.copyOf(ancestors));
      }
      return null;
    }

    private Void postCheck(SqlNode node) {
      if (postPredicate.test(node)) {
        throw new Util.FoundOne(ImmutableList.copyOf(ancestors));
      }
      return null;
    }

    private void visitChild(SqlNode node) {
      if (node == null) {
        return;
      }
      ancestors.add(node);
      node.accept(this);
      ancestors.remove(ancestors.size() - 1);
    }

    @Override public Void visit(SqlIdentifier id) {
      return check(id);
    }

    @Override public Void visit(SqlCall call) {
      preCheck(call);
      for (SqlNode node : call.getOperandList()) {
        visitChild(node);
      }
      return postCheck(call);
    }

    @Override public Void visit(SqlIntervalQualifier intervalQualifier) {
      return check(intervalQualifier);
    }

    @Override public Void visit(SqlLiteral literal) {
      return check(literal);
    }

    @Override public Void visit(SqlNodeList nodeList) {
      preCheck(nodeList);
      for (SqlNode node : nodeList) {
        visitChild(node);
      }
      return postCheck(nodeList);
    }

    @Override public Void visit(SqlDynamicParam param) {
      return check(param);
    }

    @Override public Void visit(SqlDataTypeSpec type) {
      return check(type);
    }
  }
}
