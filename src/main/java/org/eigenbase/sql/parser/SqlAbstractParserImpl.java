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
package org.eigenbase.sql.parser;

import java.io.*;

import java.lang.reflect.*;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.*;


/**
 * Abstract base for parsers generated from CommonParser.jj.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class SqlAbstractParserImpl
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Set<String> sql92ReservedWordSet;

    static {
        Set<String> set = new HashSet<String>();
        set.add("ABSOLUTE");
        set.add("ACTION");
        set.add("ADD");
        set.add("ALL");
        set.add("ALLOCATE");
        set.add("ALTER");
        set.add("AND");
        set.add("ANY");
        set.add("ARE");
        set.add("AS");
        set.add("ASC");
        set.add("ASSERTION");
        set.add("AT");
        set.add("AUTHORIZATION");
        set.add("AVG");
        set.add("BEGIN");
        set.add("BETWEEN");
        set.add("BIT");
        set.add("BIT_LENGTH");
        set.add("BOTH");
        set.add("BY");
        set.add("CASCADE");
        set.add("CASCADED");
        set.add("CASE");
        set.add("CAST");
        set.add("CATALOG");
        set.add("CHAR");
        set.add("CHARACTER");
        set.add("CHARACTER_LENGTH");
        set.add("CHAR_LENGTH");
        set.add("CHECK");
        set.add("CLOSE");
        set.add("COALESCE");
        set.add("COLLATE");
        set.add("COLLATION");
        set.add("COLUMN");
        set.add("COMMIT");
        set.add("CONNECT");
        set.add("CONNECTION");
        set.add("CONSTRAINT");
        set.add("CONSTRAINTS");
        set.add("CONTINUE");
        set.add("CONVERT");
        set.add("CORRESPONDING");
        set.add("COUNT");
        set.add("CREATE");
        set.add("CROSS");
        set.add("CURRENT");
        set.add("CURRENT_DATE");
        set.add("CURRENT_TIME");
        set.add("CURRENT_TIMESTAMP");
        set.add("CURRENT_USER");
        set.add("CURSOR");
        set.add("DATE");
        set.add("DAY");
        set.add("DEALLOCATE");
        set.add("DEC");
        set.add("DECIMAL");
        set.add("DECLARE");
        set.add("DEFAULT");
        set.add("DEFERRABLE");
        set.add("DEFERRED");
        set.add("DELETE");
        set.add("DESC");
        set.add("DESCRIBE");
        set.add("DESCRIPTOR");
        set.add("DIAGNOSTICS");
        set.add("DISCONNECT");
        set.add("DISTINCT");
        set.add("DOMAIN");
        set.add("DOUBLE");
        set.add("DROP");
        set.add("ELSE");
        set.add("END");
        set.add("END-EXEC");
        set.add("ESCAPE");
        set.add("EXCEPT");
        set.add("EXCEPTION");
        set.add("EXEC");
        set.add("EXECUTE");
        set.add("EXISTS");
        set.add("EXTERNAL");
        set.add("EXTRACT");
        set.add("FALSE");
        set.add("FETCH");
        set.add("FIRST");
        set.add("FLOAT");
        set.add("FOR");
        set.add("FOREIGN");
        set.add("FOUND");
        set.add("FROM");
        set.add("FULL");
        set.add("GET");
        set.add("GLOBAL");
        set.add("GO");
        set.add("GOTO");
        set.add("GRANT");
        set.add("GROUP");
        set.add("HAVING");
        set.add("HOUR");
        set.add("IDENTITY");
        set.add("IMMEDIATE");
        set.add("IN");
        set.add("INDICATOR");
        set.add("INITIALLY");
        set.add("INNER");
        set.add("INADD");
        set.add("INSENSITIVE");
        set.add("INSERT");
        set.add("INT");
        set.add("INTEGER");
        set.add("INTERSECT");
        set.add("INTERVAL");
        set.add("INTO");
        set.add("IS");
        set.add("ISOLATION");
        set.add("JOIN");
        set.add("KEY");
        set.add("LANGUAGE");
        set.add("LAST");
        set.add("LEADING");
        set.add("LEFT");
        set.add("LEVEL");
        set.add("LIKE");
        set.add("LOCAL");
        set.add("LOWER");
        set.add("MATCH");
        set.add("MAX");
        set.add("MIN");
        set.add("MINUTE");
        set.add("MODULE");
        set.add("MONTH");
        set.add("NAMES");
        set.add("NATIONAL");
        set.add("NATURAL");
        set.add("NCHAR");
        set.add("NEXT");
        set.add("NO");
        set.add("NOT");
        set.add("NULL");
        set.add("NULLIF");
        set.add("NUMERIC");
        set.add("OCTET_LENGTH");
        set.add("OF");
        set.add("ON");
        set.add("ONLY");
        set.add("OPEN");
        set.add("OPTION");
        set.add("OR");
        set.add("ORDER");
        set.add("OUTER");
        set.add("OUTADD");
        set.add("OVERLAPS");
        set.add("PAD");
        set.add("PARTIAL");
        set.add("POSITION");
        set.add("PRECISION");
        set.add("PREPARE");
        set.add("PRESERVE");
        set.add("PRIMARY");
        set.add("PRIOR");
        set.add("PRIVILEGES");
        set.add("PROCEDURE");
        set.add("PUBLIC");
        set.add("READ");
        set.add("REAL");
        set.add("REFERENCES");
        set.add("RELATIVE");
        set.add("RESTRICT");
        set.add("REVOKE");
        set.add("RIGHT");
        set.add("ROLLBACK");
        set.add("ROWS");
        set.add("SCHEMA");
        set.add("SCROLL");
        set.add("SECOND");
        set.add("SECTION");
        set.add("SELECT");
        set.add("SESSION");
        set.add("SESSION_USER");
        set.add("SET");
        set.add("SIZE");
        set.add("SMALLINT");
        set.add("SOME");
        set.add("SPACE");
        set.add("SQL");
        set.add("SQLCODE");
        set.add("SQLERROR");
        set.add("SQLSTATE");
        set.add("SUBSTRING");
        set.add("SUM");
        set.add("SYSTEM_USER");
        set.add("TABLE");
        set.add("TEMPORARY");
        set.add("THEN");
        set.add("TIME");
        set.add("TIMESTAMP");
        set.add("TIMEZONE_HOUR");
        set.add("TIMEZONE_MINUTE");
        set.add("TO");
        set.add("TRAILING");
        set.add("TRANSACTION");
        set.add("TRANSLATE");
        set.add("TRANSLATION");
        set.add("TRIM");
        set.add("TRUE");
        set.add("UNION");
        set.add("UNIQUE");
        set.add("UNKNOWN");
        set.add("UPDATE");
        set.add("UPPER");
        set.add("USAGE");
        set.add("USER");
        set.add("USING");
        set.add("VALUE");
        set.add("VALUES");
        set.add("VARCHAR");
        set.add("VARYING");
        set.add("VIEW");
        set.add("WHEN");
        set.add("WHENEVER");
        set.add("WHERE");
        set.add("WITH");
        set.add("WORK");
        set.add("WRITE");
        set.add("YEAR");
        set.add("ZONE");
        sql92ReservedWordSet = Collections.unmodifiableSet(set);
    }

    //~ Enums ------------------------------------------------------------------

    /**
     * Type-safe enum for context of acceptable expressions.
     */
    protected enum ExprContext
    {
        /**
         * Accept any kind of expression in this context.
         */
        ACCEPT_ALL,

        /**
         * Accept any kind of expression in this context, with the exception of
         * CURSOR constructors.
         */
        ACCEPT_NONCURSOR,

        /**
         * Accept only query expressions in this context.
         */
        ACCEPT_QUERY,

        /**
         * Accept query or join expressions in this context.
         */
        ACCEPT_QUERY_OR_JOIN,

        /**
         * Accept only non-query expressions in this context.
         */
        ACCEPT_NONQUERY,

        /**
         * Accept only parenthesized queries or non-query expressions in this
         * context.
         */
        ACCEPT_SUBQUERY,

        /**
         * Accept only CURSOR constructors, parenthesized queries, or non-query
         * expressions in this context.
         */
        ACCEPT_CURSOR
    }

    //~ Instance fields --------------------------------------------------------

    /**
     * Operator table containing the standard SQL operators and functions.
     */
    protected final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();

    protected int nDynamicParams;

    //~ Methods ----------------------------------------------------------------

    /**
     * @return immutable set of all reserved words defined by SQL-92
     *
     * @sql.92 Section 5.2
     */
    public static Set<String> getSql92ReservedWords()
    {
        return sql92ReservedWordSet;
    }

    /**
     * Creates a call.
     *
     * @param funName Name of function
     * @param pos Position in source code
     * @param funcType Type of function
     * @param functionQualifier Qualifier
     * @param operands Operands to call
     *
     * @return Call
     */
    protected SqlCall createCall(
        SqlIdentifier funName,
        SqlParserPos pos,
        SqlFunctionCategory funcType,
        SqlLiteral functionQualifier,
        SqlNode [] operands)
    {
        SqlOperator fun = null;

        // First, try a half-hearted resolution as a builtin function.
        // If we find one, use it; this will guarantee that we
        // preserve the correct syntax (i.e. don't quote builtin function
        /// name when regenerating SQL).
        if (funName.isSimple()) {
            List<SqlOperator> list =
                opTab.lookupOperatorOverloads(
                    funName,
                    null,
                    SqlSyntax.Function);
            if (list.size() == 1) {
                fun = list.get(0);
            }
        }

        // Otherwise, just create a placeholder function.  Later, during
        // validation, it will be resolved into a real function reference.
        if (fun == null) {
            fun = new SqlFunction(funName, null, null, null, null, funcType);
        }

        return fun.createCall(functionQualifier, pos, operands);
    }

    /**
     * Returns metadata about this parser: keywords, etc.
     */
    public abstract Metadata getMetadata();

    /**
     * Removes or transforms misleading information from a parse exception or
     * error, and converts to {@link SqlParseException}.
     *
     * @param ex dirty excn
     *
     * @return clean excn
     */
    public abstract SqlParseException normalizeException(Throwable ex);

    /**
     * Reinitializes parser with new input.
     *
     * @param reader provides new input
     */
    public abstract void ReInit(Reader reader);

    /**
     * Sets the tab stop size.
     *
     * @param tabSize Tab stop size
     */
    public abstract void setTabSize(int tabSize);

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * Metadata about the parser. For example:
     *
     * <ul>
     * <li>"KEY" is a keyword: it is meaningful in certain contexts, such as
     * "CREATE FOREIGN KEY", but can be used as an identifier, as in <code>
     * "CREATE TABLE t (key INTEGER)"</code>.
     * <li>"SELECT" is a reserved word. It can not be used as an identifier.
     * <li>"CURRENT_USER" is the name of a context variable. It cannot be used
     * as an identifier.
     * <li>"ABS" is the name of a reserved function. It cannot be used as an
     * identifier.
     * <li>"DOMAIN" is a reserved word as specified by the SQL:92 standard.
     * </ul>
     */
    public interface Metadata
    {
        /**
         * Returns true if token is a keyword but not a reserved word. For
         * example, "KEY".
         */
        boolean isNonReservedKeyword(String token);

        /**
         * Returns whether token is the name of a context variable such as
         * "CURRENT_USER".
         */
        boolean isContextVariableName(String token);

        /**
         * Returns whether token is a reserved function name such as
         * "CURRENT_USER".
         */
        boolean isReservedFunctionName(String token);

        /**
         * Returns whether token is a keyword. (That is, a non-reserved keyword,
         * a context variable, or a reserved function name.)
         */
        boolean isKeyword(String token);

        /**
         * Returns whether token is a reserved word.
         */
        boolean isReservedWord(String token);

        /**
         * Returns whether token is a reserved word as specified by the SQL:92
         * standard.
         */
        boolean isSql92ReservedWord(String token);

        /**
         * Returns comma-separated list of JDBC keywords.
         */
        String getJdbcKeywords();

        /**
         * Returns a list of all tokens in alphabetical order.
         */
        List<String> getTokens();
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Default implementation of the {@link Metadata} interface.
     */
    public static class MetadataImpl
        implements Metadata
    {
        private final Set<String> reservedFunctionNames = new HashSet<String>();
        private final Set<String> contextVariableNames = new HashSet<String>();
        private final Set<String> nonReservedKeyWordSet = new HashSet<String>();

        /**
         * Set of all tokens.
         */
        private final SortedSet<String> tokenSet = new TreeSet<String>();

        /**
         * Immutable list of all tokens, in alphabetical order.
         */
        private final List<String> tokenList;
        private final Set<String> reservedWords = new HashSet<String>();
        private final String sql92ReservedWords;

        /**
         * Creates a MetadataImpl.
         *
         * @param sqlParser Parser
         */
        public MetadataImpl(SqlAbstractParserImpl sqlParser)
        {
            initList(sqlParser, reservedFunctionNames, "ReservedFunctionName");
            initList(sqlParser, contextVariableNames, "ContextVariable");
            initList(sqlParser, nonReservedKeyWordSet, "NonReservedKeyWord");
            tokenList =
                Collections.unmodifiableList(new ArrayList<String>(tokenSet));
            sql92ReservedWords = constructSql92ReservedWordList();
            Set<String> reservedWordSet = new TreeSet<String>();
            reservedWordSet.addAll(tokenSet);
            reservedWordSet.removeAll(nonReservedKeyWordSet);
            reservedWords.addAll(reservedWordSet);
        }

        /**
         * Initializes lists of keywords.
         */
        private void initList(
            SqlAbstractParserImpl parserImpl,
            Set<String> keywords,
            String name)
        {
            parserImpl.ReInit(new StringReader("1"));
            try {
                Object o = virtualCall(parserImpl, name);
                Util.discard(o);
                throw Util.newInternal("expected call to fail");
            } catch (SqlParseException parseException) {
                // First time through, build the list of all tokens.
                final String [] tokenImages = parseException.getTokenImages();
                if (tokenSet.isEmpty()) {
                    for (int i = 0; i < tokenImages.length; i++) {
                        String token = tokenImages[i];
                        String tokenVal = SqlParserUtil.getTokenVal(token);
                        if (tokenVal != null) {
                            tokenSet.add(tokenVal);
                        }
                    }
                }

                // Add the tokens which would have been expected in this
                // syntactic context to the list we're building.
                final int [][] expectedTokenSequences =
                    parseException.getExpectedTokenSequences();
                for (int i = 0; i
                    < expectedTokenSequences.length; i++)
                {
                    final int [] expectedTokenSequence =
                        expectedTokenSequences[i];
                    assert expectedTokenSequence.length == 1;
                    final int tokenId = expectedTokenSequence[0];
                    String token = tokenImages[tokenId];
                    String tokenVal = SqlParserUtil.getTokenVal(token);
                    if (tokenVal != null) {
                        keywords.add(tokenVal);
                    }
                }
            } catch (Throwable e) {
                throw Util.newInternal(
                    e,
                    "Unexpected error while building token lists");
            }
        }

        /**
         * Uses reflection to invoke a method on this parser. The method must be
         * public and have no parameters.
         *
         * @param parserImpl Parser
         * @param name Name of method. For example "ReservedFunctionName".
         *
         * @return Result of calling method
         */
        private Object virtualCall(
            SqlAbstractParserImpl parserImpl,
            String name)
            throws Throwable
        {
            Class<? extends Object> clazz = parserImpl.getClass();
            try {
                final Method method = clazz.getMethod(name, (Class []) null);
                return method.invoke(parserImpl, (Object []) null);
            } catch (NoSuchMethodException e) {
                throw Util.newInternal(e);
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                throw parserImpl.normalizeException(cause);
            }
        }

        /**
         * Builds a comma-separated list of JDBC reserved words.
         */
        private String constructSql92ReservedWordList()
        {
            StringBuilder sb = new StringBuilder();
            TreeSet<String> jdbcReservedSet = new TreeSet<String>();
            jdbcReservedSet.addAll(tokenSet);
            jdbcReservedSet.removeAll(sql92ReservedWordSet);
            jdbcReservedSet.removeAll(nonReservedKeyWordSet);
            int j = 0;
            for (
                Iterator<String> jdbcReservedIter = jdbcReservedSet.iterator();
                jdbcReservedIter.hasNext();)
            {
                String jdbcReserved = jdbcReservedIter.next();
                if (j++ > 0) {
                    sb.append(",");
                }
                sb.append(jdbcReserved);
            }
            return sb.toString();
        }

        public List<String> getTokens()
        {
            return tokenList;
        }

        public boolean isSql92ReservedWord(String token)
        {
            return sql92ReservedWordSet.contains(token);
        }

        public String getJdbcKeywords()
        {
            return sql92ReservedWords;
        }

        public boolean isKeyword(String token)
        {
            return isNonReservedKeyword(token)
                || isReservedFunctionName(token)
                || isContextVariableName(token)
                || isReservedWord(token);
        }

        public boolean isNonReservedKeyword(String token)
        {
            return nonReservedKeyWordSet.contains(token);
        }

        public boolean isReservedFunctionName(String token)
        {
            return reservedFunctionNames.contains(token);
        }

        public boolean isContextVariableName(String token)
        {
            return contextVariableNames.contains(token);
        }

        public boolean isReservedWord(String token)
        {
            return reservedWords.contains(token);
        }
    }
}

// End SqlAbstractParserImpl.java
