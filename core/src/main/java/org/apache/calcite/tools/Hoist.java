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
package org.apache.calcite.tools;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * Utility that extracts constants from a SQL query.
 *
 * <p>Simple use:
 *
 * <blockquote><code>
 * final String sql =<br>
 *     "select 'x' from emp where deptno &lt; 10";<br>
 * final Hoist.Hoisted hoisted =<br>
 *     Hoist.create(Hoist.config()).hoist();<br>
 * print(hoisted); // "select ?0 from emp where deptno &lt; ?1"
 * </code></blockquote>
 *
 * <p>Calling {@link Hoisted#toString()} generates a string that is similar to
 * SQL where a user has manually converted all constants to bind variables, and
 * which could then be executed using {@link PreparedStatement#execute()}.
 * That is not a goal of this utility, but see
 * <a href="https://issues.apache.org/jira/browse/CALCITE-963">[CALCITE-963]
 * Hoist literals</a>.
 *
 * <p>For more advanced formatting, use {@link Hoisted#substitute(Function)}.
 *
 * <p>Adjust {@link Config} to use a different parser or parsing options.
 */
@Value.Enclosing
public class Hoist {
  private final Config config;

  /** Creates a Config. */
  public static Config config() {
    return ImmutableHoist.Config.builder()
        .withParserConfig(SqlParser.config())
        .build();
  }

  /** Creates a Hoist. */
  public static Hoist create(Config config) {
    return new Hoist(config);
  }

  private Hoist(Config config) {
    this.config = requireNonNull(config, "config");
  }

  /** Converts a {@link Variable} to a string "?N",
   * where N is the {@link Variable#ordinal}. */
  public static String ordinalString(Variable v) {
    return "?" + v.ordinal;
  }

  /** Converts a {@link Variable} to a string "?N",
   * where N is the {@link Variable#ordinal},
   * if the fragment is a character literal. Other fragments are unchanged. */
  public static String ordinalStringIfChar(Variable v) {
    if (v.node instanceof SqlLiteral
        && ((SqlLiteral) v.node).getTypeName() == SqlTypeName.CHAR) {
      return "?" + v.ordinal;
    } else {
      return v.sql();
    }
  }

  /** Hoists literals in a given SQL string, returning a {@link Hoisted}. */
  public Hoisted hoist(String sql) {
    final List<Variable> variables = new ArrayList<>();
    final SqlParser parser = SqlParser.create(sql, config.parserConfig());
    final SqlNode node;
    try {
      node = parser.parseQuery();
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }
    node.accept(new SqlShuttle() {
      @Override public @Nullable SqlNode visit(SqlLiteral literal) {
        variables.add(new Variable(sql, variables.size(), literal));
        return super.visit(literal);
      }
    });
    return new Hoisted(sql, variables);
  }

  /** Configuration. */
  @Value.Immutable(singleton = false)
  public interface Config {
    /** Returns the configuration for the SQL parser. */
    SqlParser.Config parserConfig();

    /** Sets {@link #parserConfig()}. */
    Config withParserConfig(SqlParser.Config parserConfig);
  }

  /** Variable. */
  public static class Variable {
    /** Original SQL of whole statement. */
    public final String originalSql;
    /** Zero-based ordinal in statement. */
    public final int ordinal;
    /** Parse tree node (typically a literal). */
    public final SqlNode node;
    /** Zero-based position within the SQL text of start of node. */
    public final int start;
    /** Zero-based position within the SQL text after end of node. */
    public final int end;

    private Variable(String originalSql, int ordinal, SqlNode node) {
      this.originalSql = requireNonNull(originalSql, "originalSql");
      this.ordinal = ordinal;
      this.node = requireNonNull(node, "node");
      final SqlParserPos pos = node.getParserPosition();
      start =
          SqlParserUtil.lineColToIndex(originalSql,
              pos.getLineNum(), pos.getColumnNum());
      end =
          SqlParserUtil.lineColToIndex(originalSql,
              pos.getEndLineNum(), pos.getEndColumnNum()) + 1;

      checkArgument(ordinal >= 0);
      checkArgument(start >= 0);
      checkArgument(start <= end);
      checkArgument(end <= originalSql.length());
    }

    /** Returns SQL text of the region of the statement covered by this
     * Variable. */
    public String sql() {
      return originalSql.substring(start, end);
    }
  }

  /** Result of hoisting. */
  public static class Hoisted {
    public final String originalSql;
    public final List<Variable> variables;

    Hoisted(String originalSql, List<Variable> variables) {
      this.originalSql = originalSql;
      this.variables = ImmutableList.copyOf(variables);
    }

    @Override public String toString() {
      return substitute(Hoist::ordinalString);
    }

    /** Returns the SQL string with variables replaced according to the
     * given substitution function. */
    public String substitute(Function<Variable, String> fn) {
      final StringBuilder b = new StringBuilder(originalSql);
      for (Variable variable : Lists.reverse(variables)) {
        final String s = fn.apply(variable);
        b.replace(variable.start, variable.end, s);
      }
      return b.toString();
    }
  }
}
