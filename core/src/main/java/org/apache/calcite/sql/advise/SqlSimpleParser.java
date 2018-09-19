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
package org.apache.calcite.sql.advise;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;

/**
 * A simple parser that takes an incomplete and turn it into a syntactically
 * correct statement. It is used in the SQL editor user-interface.
 */
public class SqlSimpleParser {
  //~ Enums ------------------------------------------------------------------

  enum TokenType {
    // keywords
    SELECT, FROM, JOIN, ON, USING, WHERE, GROUP, HAVING, ORDER, BY,

    UNION, INTERSECT, EXCEPT, MINUS,

    /**
     * left parenthesis
     */
    LPAREN {
      public String sql() {
        return "(";
      }
    },

    /**
     * right parenthesis
     */
    RPAREN {
      public String sql() {
        return ")";
      }
    },

    /**
     * identifier, or indeed any miscellaneous sequence of characters
     */
    ID,

    /**
     * double-quoted identifier, e.g. "FOO""BAR"
     */
    DQID,

    /**
     * single-quoted string literal, e.g. 'foobar'
     */
    SQID, COMMENT,
    COMMA {
      public String sql() {
        return ",";
      }
    },

    /**
     * A token created by reducing an entire sub-query.
     */
    QUERY;

    public String sql() {
      return name();
    }
  }

  //~ Instance fields --------------------------------------------------------

  private final String hintToken;
  private final SqlParser.Config parserConfig;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlSimpleParser
   *
   * @param hintToken Hint token
   * @deprecated
   */
  @Deprecated
  public SqlSimpleParser(String hintToken) {
    this(hintToken, SqlParser.Config.DEFAULT);
  }

  /**
   * Creates a SqlSimpleParser
   *
   * @param hintToken Hint token
   * @param parserConfig parser configuration
   */
  public SqlSimpleParser(String hintToken,
      SqlParser.Config parserConfig) {
    this.hintToken = hintToken;
    this.parserConfig = parserConfig;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Turns a partially completed or syntactically incorrect sql statement into
   * a simplified, valid one that can be passed into getCompletionHints().
   *
   * @param sql    A partial or syntactically incorrect sql statement
   * @param cursor to indicate column position in the query at which
   *               completion hints need to be retrieved.
   * @return a completed, valid (and possibly simplified SQL statement
   */
  public String simplifySql(String sql, int cursor) {
    // introduce the hint token into the sql at the cursor pos
    if (cursor >= sql.length()) {
      sql += " " + hintToken + " ";
    } else {
      String left = sql.substring(0, cursor);
      String right = sql.substring(cursor);
      sql = left + " " + hintToken + " " + right;
    }
    return simplifySql(sql);
  }

  /**
   * Turns a partially completed or syntactically incorrect sql statement into
   * a simplified, valid one that can be validated
   *
   * @param sql A partial or syntactically incorrect sql statement
   * @return a completed, valid (and possibly simplified) SQL statement
   */
  public String simplifySql(String sql) {
    Tokenizer tokenizer = new Tokenizer(sql, hintToken, parserConfig.quoting());
    List<Token> list = new ArrayList<>();
    while (true) {
      Token token = tokenizer.nextToken();
      if (token == null) {
        break;
      }
      if (token.type == TokenType.COMMENT) {
        // ignore comments
        continue;
      }
      list.add(token);
    }

    // Gather consecutive sub-sequences of tokens into sub-queries.
    List<Token> outList = new ArrayList<>();
    consumeQuery(list.listIterator(), outList);

    // Simplify.
    Query.simplifyList(outList, hintToken);

    // Convert to string.
    StringBuilder buf = new StringBuilder();
    int k = -1;
    for (Token token : outList) {
      if (++k > 0) {
        buf.append(' ');
      }
      token.unparse(buf);
    }
    return buf.toString();
  }

  private void consumeQuery(ListIterator<Token> iter, List<Token> outList) {
    while (iter.hasNext()) {
      consumeSelect(iter, outList);
      if (iter.hasNext()) {
        Token token = iter.next();
        switch (token.type) {
        case UNION:
        case INTERSECT:
        case EXCEPT:
        case MINUS:
          outList.add(token);
          if (iter.hasNext()) {
            token = iter.next();
            if ((token.type == TokenType.ID)
                && token.s.equalsIgnoreCase("ALL")) {
              outList.add(token);
            } else {
              iter.previous();
            }
          }
          // Combine SELECT ... UNION SELECT..., so keep trying consumeSelect
          break;
        default:
          // Unknown token detected => end of query detected
          iter.previous();
          return;
        }
      }
    }
  }

  private void consumeSelect(ListIterator<Token> iter, List<Token> outList) {
    boolean isQuery = false;
    int start = outList.size();
    List<Token> subQueryList = new ArrayList<>();
  loop:
    while (iter.hasNext()) {
      Token token = iter.next();
      subQueryList.add(token);
      switch (token.type) {
      case LPAREN:
        consumeQuery(iter, subQueryList);
        break;
      case RPAREN:
        if (isQuery) {
          subQueryList.remove(subQueryList.size() - 1);
        }
        break loop;
      case SELECT:
        isQuery = true;
        break;
      case UNION:
      case INTERSECT:
      case EXCEPT:
      case MINUS:
        subQueryList.remove(subQueryList.size() - 1);
        iter.previous();
        break loop;
      default:
      }
    }

    // Fell off end of list. Pretend we saw the required right-paren.
    if (isQuery) {
      outList.subList(start, outList.size()).clear();
      outList.add(new Query(subQueryList));
      if ((outList.size() >= 2)
          && (outList.get(outList.size() - 2).type == TokenType.LPAREN)) {
        outList.add(new Token(TokenType.RPAREN));
      }
    } else {
      // not a query - just a parenthesized expr
      outList.addAll(subQueryList);
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  public static class Tokenizer {
    private static final Map<String, TokenType> map = new HashMap<>();

    static {
      for (TokenType type : TokenType.values()) {
        map.put(type.name(), type);
      }
    }

    final String sql;
    private final String hintToken;
    private final char openQuote;
    private int pos;
    int start = 0;

    @Deprecated
    public Tokenizer(String sql, String hintToken) {
      this(sql, hintToken, Quoting.DOUBLE_QUOTE);
    }

    public Tokenizer(String sql, String hintToken, Quoting quoting) {
      this.sql = sql;
      this.hintToken = hintToken;
      this.openQuote = quoting.string.charAt(0);
      this.pos = 0;
    }

    private Token parseQuotedIdentifier() {
      // Parse double-quoted identifier.
      start = pos;
      ++pos;
      char closeQuote = openQuote == '[' ? ']' : openQuote;
      while (pos < sql.length()) {
        char c = sql.charAt(pos);
        ++pos;
        if (c == closeQuote) {
          if (pos < sql.length() && sql.charAt(pos) == closeQuote) {
            // Double close means escaped closing quote is a part of identifer
            ++pos;
            continue;
          }
          break;
        }
      }
      String match = sql.substring(start, pos);
      if (match.startsWith(openQuote + " " + hintToken + " ")) {
        return new Token(TokenType.ID, hintToken);
      }
      return new Token(TokenType.DQID, match);
    }

    public Token nextToken() {
      while (pos < sql.length()) {
        char c = sql.charAt(pos);
        final String match;
        switch (c) {
        case ',':
          ++pos;
          return new Token(TokenType.COMMA);

        case '(':
          ++pos;
          return new Token(TokenType.LPAREN);

        case ')':
          ++pos;
          return new Token(TokenType.RPAREN);

        case '\'':

          // Parse single-quoted identifier.
          start = pos;
          ++pos;
          while (pos < sql.length()) {
            c = sql.charAt(pos);
            ++pos;
            if (c == '\'') {
              if (pos < sql.length()) {
                char c1 = sql.charAt(pos);
                if (c1 == '\'') {
                  // encountered consecutive
                  // single-quotes; still in identifier
                  ++pos;
                } else {
                  break;
                }
              } else {
                break;
              }
            }
          }
          match = sql.substring(start, pos);
          return new Token(TokenType.SQID, match);

        case '/':

          // possible start of '/*' or '//' comment
          if (pos + 1 < sql.length()) {
            char c1 = sql.charAt(pos + 1);
            if (c1 == '*') {
              int end = sql.indexOf("*/", pos + 2);
              if (end < 0) {
                end = sql.length();
              } else {
                end += "*/".length();
              }
              pos = end;
              return new Token(TokenType.COMMENT);
            }
            if (c1 == '/') {
              pos = indexOfLineEnd(sql, pos + 2);
              return new Token(TokenType.COMMENT);
            }
            // fall through
          }

        case '-':
          // possible start of '--' comment
          if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) == '-') {
            pos = indexOfLineEnd(sql, pos + 2);
            return new Token(TokenType.COMMENT);
          }
          // fall through

        default:
          if (c == openQuote) {
            return parseQuotedIdentifier();
          }
          if (Character.isWhitespace(c)) {
            ++pos;
            break;
          } else {
            // Probably a letter or digit. Start an identifier.
            // Other characters, e.g. *, ! are also included
            // in identifiers.
            int start = pos;
            ++pos;
            loop:
            while (pos < sql.length()) {
              c = sql.charAt(pos);
              switch (c) {
              case '(':
              case ')':
              case '/':
              case ',':
                break loop;
              default:
                if (Character.isWhitespace(c)) {
                  break loop;
                } else {
                  ++pos;
                }
              }
            }
            String name = sql.substring(start, pos);
            TokenType tokenType = map.get(name.toUpperCase(Locale.ROOT));
            if (tokenType == null) {
              return new IdToken(TokenType.ID, name);
            } else {
              // keyword, e.g. SELECT, FROM, WHERE
              return new Token(tokenType);
            }
          }
        }
      }
      return null;
    }

    private int indexOfLineEnd(String sql, int i) {
      int length = sql.length();
      while (i < length) {
        char c = sql.charAt(i);
        switch (c) {
        case '\r':
        case '\n':
          return i;
        default:
          ++i;
        }
      }
      return i;
    }
  }

  public static class Token {
    private final TokenType type;
    private final String s;

    Token(TokenType tokenType) {
      this(tokenType, null);
    }

    Token(TokenType type, String s) {
      this.type = type;
      this.s = s;
    }

    public String toString() {
      return (s == null) ? type.toString() : (type + "(" + s + ")");
    }

    public void unparse(StringBuilder buf) {
      if (s == null) {
        buf.append(type.sql());
      } else {
        buf.append(s);
      }
    }
  }

  public static class IdToken extends Token {
    public IdToken(TokenType type, String s) {
      super(type, s);
      assert (type == TokenType.DQID) || (type == TokenType.ID);
    }
  }

  static class Query extends Token {
    private final List<Token> tokenList;

    public Query(List<Token> tokenList) {
      super(TokenType.QUERY);
      this.tokenList = new ArrayList<>(tokenList);
    }

    public void unparse(StringBuilder buf) {
      int k = -1;
      for (Token token : tokenList) {
        if (++k > 0) {
          buf.append(' ');
        }
        token.unparse(buf);
      }
    }

    public static void simplifyList(List<Token> list, String hintToken) {
      // Simplify
      //   SELECT * FROM t UNION ALL SELECT * FROM u WHERE ^
      // to
      //   SELECT * FROM u WHERE ^
      for (Token token : list) {
        if (token instanceof Query) {
          Query query = (Query) token;
          if (query.contains(hintToken)) {
            list.clear();
            list.add(query.simplify(hintToken));
            break;
          }
        }
      }
    }

    public Query simplify(String hintToken) {
      TokenType clause = TokenType.SELECT;
      TokenType foundInClause = null;
      Query foundInSubQuery = null;
      TokenType majorClause = null;
      if (hintToken != null) {
        for (Token token : tokenList) {
          switch (token.type) {
          case ID:
            if (token.s.equals(hintToken)) {
              foundInClause = clause;
            }
            break;
          case SELECT:
          case FROM:
          case WHERE:
          case GROUP:
          case HAVING:
          case ORDER:
            majorClause = token.type;
            // fall through
          case JOIN:
          case USING:
          case ON:
            clause = token.type;
            break;
          case COMMA:
            if (majorClause == TokenType.FROM) {
              // comma inside from clause
              clause = TokenType.FROM;
            }
            break;
          case QUERY:
            if (((Query) token).contains(hintToken)) {
              foundInClause = clause;
              foundInSubQuery = (Query) token;
            }
            break;
          }
        }
      } else {
        foundInClause = TokenType.QUERY;
      }
      if (foundInClause != null) {
        switch (foundInClause) {
        case SELECT:
          purgeSelectListExcept(hintToken);
          purgeWhere();
          purgeOrderBy();
          break;
        case FROM:
        case JOIN:

          // See comments against ON/USING.
          purgeSelect();
          purgeFromExcept(hintToken);
          purgeWhere();
          purgeGroupByHaving();
          purgeOrderBy();
          break;
        case ON:
        case USING:

          // We need to treat expressions in FROM and JOIN
          // differently than ON and USING. Consider
          //     FROM t1 JOIN t2 ON b1 JOIN t3 USING (c2)
          // t1, t2, t3 occur in the FROM clause, and do not depend
          // on anything; b1 and c2 occur in ON scope, and depend
          // on the FROM clause
          purgeSelect();
          purgeWhere();
          purgeOrderBy();
          break;
        case WHERE:
          purgeSelect();
          purgeGroupByHaving();
          purgeOrderBy();
          break;
        case GROUP:
        case HAVING:
          purgeSelect();
          purgeWhere();
          purgeOrderBy();
          break;
        case ORDER:
          purgeWhere();
          break;
        case QUERY:

          // Indicates that the expression to be simplified is
          // outside this sub-query. Preserve a simplified SELECT
          // clause.
          // It might be a good idea to purge select expressions, however
          // purgeSelectExprsKeepAliases might end up with <<0 as "*">> which is not valid.
          // purgeSelectExprsKeepAliases();
          purgeWhere();
          purgeGroupByHaving();
          break;
        }
      }

      // Simplify sub-queries.
      for (Token token : tokenList) {
        switch (token.type) {
        case QUERY: {
          Query query = (Query) token;
          query.simplify(
              (query == foundInSubQuery) ? hintToken : null);
          break;
        }
        }
      }
      return this;
    }

    private void purgeSelectListExcept(String hintToken) {
      List<Token> sublist = findClause(TokenType.SELECT);
      int parenCount = 0;
      int itemStart = 1;
      int itemEnd = -1;
      boolean found = false;
      for (int i = 0; i < sublist.size(); i++) {
        Token token = sublist.get(i);
        switch (token.type) {
        case LPAREN:
          ++parenCount;
          break;
        case RPAREN:
          --parenCount;
          break;
        case COMMA:
          if (parenCount == 0) {
            if (found) {
              itemEnd = i;
              break;
            }
            itemStart = i + 1;
          }
          break;
        case ID:
          if (token.s.equals(hintToken)) {
            found = true;
          }
        }
      }
      if (found) {
        if (itemEnd < 0) {
          itemEnd = sublist.size();
        }

        List<Token> selectItem =
            new ArrayList<>(
                sublist.subList(itemStart, itemEnd));
        Token select = sublist.get(0);
        sublist.clear();
        sublist.add(select);
        sublist.addAll(selectItem);
      }
    }

    private void purgeSelect() {
      List<Token> sublist = findClause(TokenType.SELECT);
      Token select = sublist.get(0);
      sublist.clear();
      sublist.add(select);
      sublist.add(new Token(TokenType.ID, "*"));
    }

    private void purgeSelectExprsKeepAliases() {
      List<Token> sublist = findClause(TokenType.SELECT);
      List<Token> newSelectClause = new ArrayList<>();
      newSelectClause.add(sublist.get(0));
      int itemStart = 1;
      for (int i = 1; i < sublist.size(); i++) {
        Token token = sublist.get(i);
        if (((i + 1) == sublist.size())
            || (sublist.get(i + 1).type == TokenType.COMMA)) {
          if (token.type == TokenType.ID) {
            // This might produce <<0 as "a.x+b.y">>, or <<0 as "*">>, or even <<0 as "a.*">>
            newSelectClause.add(new Token(TokenType.ID, "0"));
            newSelectClause.add(new Token(TokenType.ID, "AS"));
            newSelectClause.add(token);
          } else {
            newSelectClause.addAll(
                sublist.subList(itemStart, i + 1));
          }
          itemStart = i + 2;
          if ((i + 1) < sublist.size()) {
            newSelectClause.add(new Token(TokenType.COMMA));
          }
        }
      }
      sublist.clear();
      sublist.addAll(newSelectClause);
    }

    private void purgeFromExcept(String hintToken) {
      List<Token> sublist = findClause(TokenType.FROM);
      int itemStart = -1;
      int itemEnd = -1;
      int joinCount = 0;
      boolean found = false;
      for (int i = 0; i < sublist.size(); i++) {
        Token token = sublist.get(i);
        switch (token.type) {
        case QUERY:
          if (((Query)token).contains(hintToken)) {
            found = true;
          }
          break;
        case JOIN:
          ++joinCount;
          // fall through
        case FROM:
        case ON:
        case COMMA:
          if (found) {
            itemEnd = i;
            break;
          }
          itemStart = i + 1;
          break;
        case ID:
          if (token.s.equals(hintToken)) {
            found = true;
          }
        }
      }

      // Don't simplify a FROM clause containing a JOIN: we lose help
      // with syntax.
      if (found && (joinCount == 0)) {
        if (itemEnd == -1) {
          itemEnd = sublist.size();
        }
        List<Token> fromItem =
            new ArrayList<>(
                sublist.subList(itemStart, itemEnd));
        Token from = sublist.get(0);
        sublist.clear();
        sublist.add(from);
        sublist.addAll(fromItem);
      }
      if (sublist.get(sublist.size() - 1).type == TokenType.ON) {
        sublist.add(new Token(TokenType.ID, "TRUE"));
      }
    }

    private void purgeWhere() {
      List<Token> sublist = findClause(TokenType.WHERE);
      if (sublist != null) {
        sublist.clear();
      }
    }

    private void purgeGroupByHaving() {
      List<Token> sublist = findClause(TokenType.GROUP);
      if (sublist != null) {
        sublist.clear();
      }
      sublist = findClause(TokenType.HAVING);
      if (sublist != null) {
        sublist.clear();
      }
    }

    private void purgeOrderBy() {
      List<Token> sublist = findClause(TokenType.ORDER);
      if (sublist != null) {
        sublist.clear();
      }
    }

    private List<Token> findClause(TokenType keyword) {
      int start = -1;
      int k = -1;
      EnumSet<TokenType> clauses =
          EnumSet.of(
              TokenType.SELECT,
              TokenType.FROM,
              TokenType.WHERE,
              TokenType.GROUP,
              TokenType.HAVING,
              TokenType.ORDER);
      for (Token token : tokenList) {
        ++k;
        if (token.type == keyword) {
          start = k;
        } else if ((start >= 0)
            && clauses.contains(token.type)) {
          return tokenList.subList(start, k);
        }
      }
      if (start >= 0) {
        return tokenList.subList(start, k + 1);
      }
      return null;
    }

    private boolean contains(String hintToken) {
      for (Token token : tokenList) {
        switch (token.type) {
        case ID:
          if (token.s.equals(hintToken)) {
            return true;
          }
          break;
        case QUERY:
          if (((Query) token).contains(hintToken)) {
            return true;
          }
          break;
        }
      }
      return false;
    }
  }
}

// End SqlSimpleParser.java
