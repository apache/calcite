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
package org.eigenbase.sql;

import java.nio.charset.*;

import java.sql.*;

import java.text.*;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * Contains utility functions related to SQL parsing, all static.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 26, 2003
 */
public abstract class SqlUtil
{
    //~ Methods ----------------------------------------------------------------

    static SqlNode andExpressions(
        SqlNode node1,
        SqlNode node2)
    {
        if (node1 == null) {
            return node2;
        }
        ArrayList<SqlNode> list = new ArrayList<SqlNode>();
        if (node1.getKind() == SqlKind.AND) {
            list.addAll(Arrays.asList(((SqlCall) node1).operands));
        } else {
            list.add(node1);
        }
        if (node2.getKind() == SqlKind.AND) {
            list.addAll(Arrays.asList(((SqlCall) node2).operands));
        } else {
            list.add(node2);
        }
        return SqlStdOperatorTable.andOperator.createCall(
            SqlParserPos.ZERO,
            list);
    }

    static ArrayList<SqlNode> flatten(SqlNode node)
    {
        ArrayList<SqlNode> list = new ArrayList<SqlNode>();
        flatten(node, list);
        return list;
    }

    /**
     * Returns the <code>n</code>th (0-based) input to a join expression.
     */
    public static SqlNode getFromNode(
        SqlSelect query,
        int ordinal)
    {
        ArrayList<SqlNode> list = flatten(query.getFrom());
        return list.get(ordinal);
    }

    private static void flatten(
        SqlNode node,
        ArrayList<SqlNode> list)
    {
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
            flatten(call.operands[0], list);
            return;
        default:
            list.add(node);
            return;
        }
    }

    /**
     * Converts an SqlNode array to a SqlNodeList
     */
    public static SqlNodeList toNodeList(SqlNode [] operands)
    {
        SqlNodeList ret = new SqlNodeList(SqlParserPos.ZERO);
        for (int i = 0; i < operands.length; i++) {
            SqlNode node = operands[i];
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
        boolean allowCast)
    {
        if (node instanceof SqlLiteral) {
            SqlLiteral literal = (SqlLiteral) node;
            if (literal.getTypeName() == SqlTypeName.NULL) {
                assert (null == literal.getValue());
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
                if (isNullLiteral(call.operands[0], false)) {
                    // node is "CAST(NULL as type)"
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns whether a node represents the NULL value or a series of nested
     * CAST(NULL as <TYPE>) calls<br>
     * For Example:<br>
     * isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1))) returns true
     */
    public static boolean isNull(SqlNode node)
    {
        return isNullLiteral(node, false)
            || ((node.getKind() == SqlKind.CAST)
                && isNull(((SqlCall) node).operands[0]));
    }

    /**
     * Returns whether a node is a literal.
     *
     * <p>Many constructs which require literals also accept <code>CAST(NULL AS
     * <i>type</i>)</code>. This method does not accept casts, so you should
     * call {@link #isNullLiteral} first.
     *
     * @param node The node, never null.
     *
     * @return Whether the node is a literal
     *
     * @pre node != null
     */
    public static boolean isLiteral(SqlNode node)
    {
        Util.pre(node != null, "node != null");
        return node instanceof SqlLiteral;
    }

    /**
     * Returns whether a node is a literal chain which is used to represent a
     * continued string literal.
     *
     * @param node The node, never null.
     *
     * @return Whether the node is a literal chain
     *
     * @pre node != null
     */
    public static boolean isLiteralChain(SqlNode node)
    {
        Util.pre(node != null, "node != null");
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
     * @param operator The operator
     * @param writer Writer
     * @param operands List of 0 or more operands
     * @param emptyParens Whether to print parentheses if there are 0 operands
     * @param quantifier Quantifier
     */
    public static void unparseFunctionSyntax(
        SqlOperator operator,
        SqlWriter writer,
        SqlNode [] operands,
        boolean emptyParens,
        SqlLiteral quantifier)
    {
        if (operator instanceof SqlFunction) {
            SqlFunction function = (SqlFunction) operator;

            if (function.getFunctionType()
                == SqlFunctionCategory.UserDefinedSpecificFunction)
            {
                writer.keyword("SPECIFIC");
            }
            SqlIdentifier id = function.getSqlIdentifier();
            if (id == null) {
                writer.keyword(operator.getName());
            } else {
                id.unparse(writer, 0, 0);
            }
        } else {
            writer.print(operator.getName());
        }
        if ((operands.length == 0) && !emptyParens) {
            // For example, the "LOCALTIME" function appears as "LOCALTIME"
            // when it has 0 args, not "LOCALTIME()".
            return;
        }
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.FunCall, "(", ")");
        if (null != quantifier) {
            quantifier.unparse(writer, 0, 0);
        }
        for (int i = 0; i < operands.length; i++) {
            SqlNode operand = operands[i];
            writer.sep(",");
            operand.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }

    public static void unparseBinarySyntax(
        SqlOperator operator,
        SqlNode [] operands,
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        SqlBinaryOperator binop = (SqlBinaryOperator) operator;
        assert operands.length == 2;
        final SqlWriter.Frame frame =
            writer.startList(
                (binop instanceof SqlSetOperator)
                ? SqlWriter.FrameTypeEnum.Setop
                : SqlWriter.FrameTypeEnum.Simple);
        operands[0].unparse(
            writer,
            leftPrec,
            binop.getLeftPrec());
        final boolean needsSpace = binop.needsSpace();
        writer.setNeedWhitespace(needsSpace);
        writer.sep(binop.getName());
        writer.setNeedWhitespace(needsSpace);
        operands[1].unparse(
            writer,
            binop.getRightPrec(),
            rightPrec);
        writer.endList(frame);
    }

    /**
     * Concatenates string literals.
     *
     * <p>This method takes an array of arguments, since pairwise concatenation
     * means too much string copying.
     *
     * @param lits an array of {@link SqlLiteral}, not empty, all of the same
     * class
     *
     * @return a new {@link SqlLiteral}, of that same class, whose value is the
     * string concatenation of the values of the literals
     *
     * @throws ClassCastException if the lits are not homogeneous.
     * @throws ArrayIndexOutOfBoundsException if lits is an empty array.
     */
    public static SqlLiteral concatenateLiterals(SqlLiteral [] lits)
    {
        if (lits.length == 1) {
            return lits[0]; // nothing to do
        }
        return ((SqlAbstractStringLiteral) lits[0]).concat1(lits);
    }

    /**
     * Looks up a (possibly overloaded) routine based on name and argument
     * types.
     *
     * @param opTab operator table to search
     * @param funcName name of function being invoked
     * @param argTypes argument types
     * @param category whether a function or a procedure. (If a procedure is
     * being invoked, the overload rules are simpler.)
     *
     * @return matching routine, or null if none found
     *
     * @sql.99 Part 2 Section 10.4
     */
    public static SqlFunction lookupRoutine(
        SqlOperatorTable opTab,
        SqlIdentifier funcName,
        RelDataType [] argTypes,
        SqlFunctionCategory category)
    {
        List<SqlFunction> list =
            lookupSubjectRoutines(
                opTab,
                funcName,
                argTypes,
                category);
        if (list.isEmpty()) {
            return null;
        } else {
            // return first on schema path
            return list.get(0);
        }
    }

    /**
     * Looks up all subject routines matching the given name and argument types.
     *
     * @param opTab operator table to search
     * @param funcName name of function being invoked
     * @param argTypes argument types
     * @param category category of routine to look up
     *
     * @return list of matching routines
     *
     * @sql.99 Part 2 Section 10.4
     */
    public static List<SqlFunction> lookupSubjectRoutines(
        SqlOperatorTable opTab,
        SqlIdentifier funcName,
        RelDataType [] argTypes,
        SqlFunctionCategory category)
    {
        // start with all routines matching by name
        List<SqlFunction> routines =
            lookupSubjectRoutinesByName(opTab, funcName, category);

        // first pass:  eliminate routines which don't accept the given
        // number of arguments
        filterRoutinesByParameterCount(routines, argTypes);

        // NOTE: according to SQL99, procedures are NOT overloaded on type,
        // only on number of arguments.
        if (category == SqlFunctionCategory.UserDefinedProcedure) {
            return routines;
        }

        // second pass:  eliminate routines which don't accept the given
        // argument types
        filterRoutinesByParameterType(routines, argTypes);

        // see if we can stop now; this is necessary for the case
        // of builtin functions where we don't have param type info
        if (routines.size() < 2) {
            return routines;
        }

        // third pass:  for each parameter from left to right, eliminate
        // all routines except those with the best precedence match for
        // the given arguments
        filterRoutinesByTypePrecedence(routines, argTypes);

        return routines;
    }

    /**
     * Determine if there is a routine matching the given name and number of
     * arguments.
     *
     * @param opTab operator table to search
     * @param funcName name of function being invoked
     * @param argTypes argument types
     * @param category category of routine to look up
     *
     * @return true if match found
     */
    public static boolean matchRoutinesByParameterCount(
        SqlOperatorTable opTab,
        SqlIdentifier funcName,
        RelDataType [] argTypes,
        SqlFunctionCategory category)
    {
        // start with all routines matching by name
        List<SqlFunction> routines =
            lookupSubjectRoutinesByName(opTab, funcName, category);

        // first pass:  eliminate routines which don't accept the given
        // number of arguments
        filterRoutinesByParameterCount(routines, argTypes);

        return (routines.size() > 0);
    }

    private static List<SqlFunction> lookupSubjectRoutinesByName(
        SqlOperatorTable opTab,
        SqlIdentifier funcName,
        SqlFunctionCategory category)
    {
        List<SqlOperator> operators =
            opTab.lookupOperatorOverloads(
                funcName,
                category,
                SqlSyntax.Function);
        List<SqlFunction> routines = new ArrayList<SqlFunction>();
        for (SqlOperator operator : operators) {
            if (operator instanceof SqlFunction) {
                routines.add((SqlFunction) operator);
            }
        }
        return routines;
    }

    private static void filterRoutinesByParameterCount(
        List<SqlFunction> routines,
        RelDataType [] argTypes)
    {
        Iterator<SqlFunction> iter = routines.iterator();
        while (iter.hasNext()) {
            SqlFunction function = (SqlFunction) iter.next();
            SqlOperandCountRange od = function.getOperandCountRange();
            if (!od.isValidCount(argTypes.length)) {
                iter.remove();
            }
        }
    }

    /**
     * @sql.99 Part 2 Section 10.4 Syntax Rule 6.b.iii.2.B
     */
    private static void filterRoutinesByParameterType(
        List<SqlFunction> routines,
        RelDataType [] argTypes)
    {
        Iterator<SqlFunction> iter = routines.iterator();
        while (iter.hasNext()) {
            SqlFunction function = iter.next();
            RelDataType [] paramTypes = function.getParamTypes();
            if (paramTypes == null) {
                // no parameter information for builtins; keep for now
                continue;
            }
            assert (paramTypes.length == argTypes.length);
            boolean keep = true;
            for (int i = 0; i < paramTypes.length; ++i) {
                RelDataTypePrecedenceList precList =
                    argTypes[i].getPrecedenceList();
                if (!precList.containsType(paramTypes[i])) {
                    keep = false;
                    break;
                }
            }
            if (!keep) {
                iter.remove();
            }
        }
    }

    /**
     * @sql.99 Part 2 Section 9.4
     */
    private static void filterRoutinesByTypePrecedence(
        List<SqlFunction> routines,
        RelDataType [] argTypes)
    {
        for (int i = 0; i < argTypes.length; ++i) {
            RelDataTypePrecedenceList precList =
                argTypes[i].getPrecedenceList();
            RelDataType bestMatch = null;
            for (SqlFunction function : routines) {
                RelDataType [] paramTypes = function.getParamTypes();
                if (paramTypes == null) {
                    continue;
                }
                if (bestMatch == null) {
                    bestMatch = paramTypes[i];
                } else {
                    int c =
                        precList.compareTypePrecedence(
                            bestMatch,
                            paramTypes[i]);
                    if (c < 0) {
                        bestMatch = paramTypes[i];
                    }
                }
            }
            if (bestMatch != null) {
                Iterator<SqlFunction> iter = routines.iterator();
                while (iter.hasNext()) {
                    SqlFunction function = (SqlFunction) iter.next();
                    RelDataType [] paramTypes = function.getParamTypes();
                    int c;
                    if (paramTypes == null) {
                        c = -1;
                    } else {
                        c = precList.compareTypePrecedence(
                            paramTypes[i],
                            bestMatch);
                    }
                    if (c < 0) {
                        iter.remove();
                    }
                }
            }
        }
    }

    /**
     * Returns the <code>i</code>th select-list item of a query.
     */
    public static SqlNode getSelectListItem(SqlNode query, int i)
    {
        switch (query.getKind()) {
        case SELECT:
            SqlSelect select = (SqlSelect) query;
            SqlNode from = select.getFrom();
            if (from.getKind() == SqlKind.AS) {
                SqlCall as = (SqlCall) from;
                from = as.operands[0];
            }
            if (from.getKind() == SqlKind.VALUES) {
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
            Util.permAssert(
                call.operands.length > 0,
                "VALUES must have at least one operand");
            final SqlCall row = (SqlCall) call.operands[0];
            Util.permAssert(
                row.operands.length > i, "VALUES has too few columns");
            return row.operands[i];

        default:
            // Unexpected type of query.
            throw Util.needToImplement(query);
        }
    }

    /**
     * If an identifier is a legitimate call to a function which has no
     * arguments and requires no parentheses (for example "CURRENT_USER"),
     * returns a call to that function, otherwise returns null.
     */
    public static SqlCall makeCall(
        SqlOperatorTable opTab,
        SqlIdentifier id)
    {
        if (id.names.length == 1) {
            List<SqlOperator> list =
                opTab.lookupOperatorOverloads(
                    id,
                    null,
                    SqlSyntax.Function);
            for (SqlOperator operator : list) {
                if (operator.getSyntax() == SqlSyntax.FunctionId) {
                    // Even though this looks like an identifier, it is a
                    // actually a call to a function. Construct a fake
                    // call to this function, so we can use the regular
                    // operator validation.
                    return new SqlCall(
                        operator,
                        SqlNode.emptyArray,
                        id.getParserPosition(),
                        true,
                        null);
                }
            }
        }
        return null;
    }

    public static String deriveAliasFromOrdinal(int ordinal)
    {
        // Use a '$' so that queries can't easily reference the
        // generated name.
        return "EXPR$" + ordinal;
    }

    /**
     * Constructs an operator signature from a type list.
     *
     * @param op operator
     * @param typeList list of types to use for operands. Types may be
     * represented as {@link String}, {@link SqlTypeFamily}, or any object with
     * a valid {@link Object#toString()} method.
     *
     * @return constructed signature
     */
    public static String getOperatorSignature(
        SqlOperator op,
        List<? extends Object> typeList)
    {
        return getAliasedSignature(
            op,
            op.getName(),
            typeList);
    }

    /**
     * Constructs an operator signature from a type list, substituting an alias
     * for the operator name.
     *
     * @param op operator
     * @param opName name to use for operator
     * @param typeList list of {@link SqlTypeName} or {@link String} to use for
     * operands
     *
     * @return constructed signature
     */
    public static String getAliasedSignature(
        SqlOperator op,
        String opName,
        List<? extends Object> typeList)
    {
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
                ret.append("<").append(
                    typeList.get(i).toString().toUpperCase()).append(">");
            }
            ret.append(")'");
        } else {
            Object [] values = new Object[typeList.size() + 1];
            values[0] = opName;
            ret.append("'");
            for (int i = 0; i < typeList.size(); i++) {
                values[i + 1] =
                    "<"
                    + typeList.get(i).toString().toUpperCase() + ">";
            }
            ret.append(MessageFormat.format(template, values));
            ret.append("'");
            assert (typeList.size() + 1) == values.length;
        }

        return ret.toString();
    }

    /**
     * Wraps an exception with context.
     */
    public static EigenbaseException newContextException(
        final SqlParserPos pos,
        Throwable e,
        String inputText)
    {
        EigenbaseContextException ex =
            (EigenbaseContextException) newContextException(pos, e);
        ex.setOriginalStatement(inputText);
        return ex;
    }

    /**
     * Wraps an exception with context.
     */
    public static EigenbaseException newContextException(
        final SqlParserPos pos,
        Throwable e)
    {
        int line = pos.getLineNum();
        int col = pos.getColumnNum();
        int endLine = pos.getEndLineNum();
        int endCol = pos.getEndColumnNum();
        return newContextException(line, col, endLine, endCol, e);
    }

    /**
     * Wraps an exception with context.
     */
    public static EigenbaseException newContextException(
        int line,
        int col,
        int endLine,
        int endCol,
        Throwable e)
    {
        EigenbaseContextException contextExcn =
            ((line == endLine) && (col == endCol))
            ? EigenbaseResource.instance().ValidatorContextPoint.ex(
                line,
                col,
                e)
            : EigenbaseResource.instance().ValidatorContext.ex(
                line,
                col,
                endLine,
                endCol,
                e);
        contextExcn.setPosition(line, col, endLine, endCol);
        return contextExcn;
    }

    /**
     * Returns whether a {@link SqlNode node} is a {@link SqlCall call} to a
     * given {@link SqlOperator operator}.
     */
    public static boolean isCallTo(SqlNode node, SqlOperator operator)
    {
        return (node instanceof SqlCall)
            && (((SqlCall) node).getOperator() == operator);
    }

    /**
     * Creates the type of an {@link NlsString}.
     *
     * <p>The type inherits the The NlsString's {@link Charset} and {@link
     * SqlCollation}, if they are set, otherwise it gets the system defaults.
     *
     * @param typeFactory Type factory
     * @param str String
     *
     * @return Type, including collation and charset
     */
    public static RelDataType createNlsStringType(
        RelDataTypeFactory typeFactory,
        NlsString str)
    {
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
     *
     * @return Java-level name, or null if SQL-level name is unknown
     */
    public static String translateCharacterSetName(String name)
    {
        if (name.equals("LATIN1")) {
            return "ISO-8859-1";
        } else if (name.equals("UTF16")) {
            return ConversionUtil.NATIVE_UTF16_CHARSET_NAME;
        } else if (name.equals(ConversionUtil.NATIVE_UTF16_CHARSET_NAME)) {
            // no translation needed
            return name;
        } else if (name.equals("ISO-8859-1")) {
            // no translation needed
            return name;
        }
        return null;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Handles particular {@link DatabaseMetaData} methods; invocations of other
     * methods will fall through to the base class, {@link
     * BarfingInvocationHandler}, which will throw an error.
     */
    public static class DatabaseMetaDataInvocationHandler
        extends BarfingInvocationHandler
    {
        private final String databaseProductName;
        private final String identifierQuoteString;

        public DatabaseMetaDataInvocationHandler(
            String databaseProductName,
            String identifierQuoteString)
        {
            this.databaseProductName = databaseProductName;
            this.identifierQuoteString = identifierQuoteString;
        }

        public String getDatabaseProductName()
            throws SQLException
        {
            return databaseProductName;
        }

        public String getIdentifierQuoteString()
            throws SQLException
        {
            return identifierQuoteString;
        }
    }
}

// End SqlUtil.java
