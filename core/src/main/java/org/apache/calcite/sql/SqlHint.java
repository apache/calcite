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

import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;

import static java.util.Objects.requireNonNull;

/**
 * A <code>SqlHint</code> is a node of a parse tree which represents
 * a sql hint expression.
 *
 * <p>Basic hint grammar is: hint_name[(option1, option2 ...)].
 * The hint_name should be a simple identifier, the options part is optional.
 * Every option can be of four formats:
 *
 * <ul>
 *   <li>simple identifier</li>
 *   <li>literal</li>
 *   <li>key value pair whose key is a simple identifier and value is a string literal</li>
 *   <li>key value pair whose key and value are both string literal</li>
 * </ul>
 *
 * <p>The option format can not be mixed in, they should either be all simple identifiers
 * or all literals or all key value pairs.
 *
 * <p>We support 2 kinds of hints in the parser:
 * <ul>
 *   <li>Query hint, right after the select keyword, i.e.:
 *   <pre>
 *     select &#47;&#42;&#43; hint1, hint2, ... &#42;&#47; ...
 *   </pre>
 *   </li>
 *   <li>Table hint: right after the referenced table name, i.e.:
 *   <pre>
 *     select f0, f1, f2 from t1 &#47;&#42;&#43; hint1, hint2, ... &#42;&#47; ...
 *   </pre>
 *   </li>
 * </ul>
 */
public class SqlHint extends SqlCall {
  //~ Instance fields --------------------------------------------------------

  private final SqlIdentifier name;
  private final SqlNodeList options;
  private final HintOptionFormat optionFormat;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("HINT", SqlKind.HINT) {
        @Override public SqlCall createCall(
            @Nullable SqlLiteral functionQualifier,
            SqlParserPos pos,
            @Nullable SqlNode... operands) {
          return new SqlHint(pos,
              (SqlIdentifier) requireNonNull(operands[0], "name"),
              (SqlNodeList) requireNonNull(operands[1], "options"),
              ((SqlLiteral) requireNonNull(operands[2], "optionFormat"))
                  .getValueAs(HintOptionFormat.class));
        }
      };

  //~ Constructors -----------------------------------------------------------

  public SqlHint(
      SqlParserPos pos,
      SqlIdentifier name,
      SqlNodeList options,
      HintOptionFormat optionFormat) {
    super(pos);
    this.name = name;
    this.optionFormat = optionFormat;
    this.options = options;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, options, optionFormat.symbol(SqlParserPos.ZERO));
  }

  /**
   * Returns the sql hint name.
   */
  public String getName() {
    return name.getSimple();
  }

  /** Returns the hint option format. */
  public HintOptionFormat getOptionFormat() {
    return optionFormat;
  }

  /**
   * Returns a string list if the hint option is a list of
   * simple SQL identifier, or a list of literals,
   * else returns an empty list.
   */
  public List<String> getOptionList() {
    if (optionFormat == HintOptionFormat.ID_LIST) {
      return ImmutableList.copyOf(SqlIdentifier.simpleNames(options));
    } else if (optionFormat == HintOptionFormat.LITERAL_LIST) {
      return options.stream()
          .map(node -> {
            SqlLiteral literal = (SqlLiteral) node;
            return requireNonNull(literal.toValue(),
                () -> "null hint literal in " + options);
          })
          .collect(toImmutableList());
    } else {
      return ImmutableList.of();
    }
  }

  /**
   * Returns a key value string map if the hint option is a list of
   * pair, each pair contains a simple SQL identifier and a string literal;
   * else returns an empty map.
   */
  public Map<String, String> getOptionKVPairs() {
    if (optionFormat == HintOptionFormat.KV_LIST) {
      final Map<String, String> attrs = new HashMap<>();
      for (int i = 0; i < options.size() - 1; i += 2) {
        final SqlNode k = options.get(i);
        final SqlNode v = options.get(i + 1);
        attrs.put(getOptionKeyAsString(k), ((SqlLiteral) v).getValueAs(String.class));
      }
      return ImmutableMap.copyOf(attrs);
    } else {
      return ImmutableMap.of();
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, leftPrec, rightPrec);
    if (!this.options.isEmpty()) {
      SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      for (int i = 0; i < options.size(); i++) {
        SqlNode option = options.get(i);
        SqlNode nextOption = i < options.size() - 1 ? options.get(i + 1) : null;
        writer.sep(",", false);
        option.unparse(writer, leftPrec, rightPrec);
        if (optionFormat == HintOptionFormat.KV_LIST && nextOption != null) {
          writer.keyword("=");
          nextOption.unparse(writer, leftPrec, rightPrec);
          i += 1;
        }
      }
      writer.endList(frame);
    }
  }

  /** Enumeration that represents hint option format. */
  public enum HintOptionFormat implements Symbolizable {
    /**
     * The hint has no options.
     */
    EMPTY,
    /**
     * The hint options are as literal list.
     */
    LITERAL_LIST,
    /**
     * The hint options are as simple identifier list.
     */
    ID_LIST,
    /**
     * The hint options are list of key-value pairs.
     * For each pair,
     * the key is a simple identifier or string literal,
     * the value is a string literal.
     */
    KV_LIST
  }

  //~ Tools ------------------------------------------------------------------

  private static String getOptionKeyAsString(SqlNode node) {
    assert node instanceof SqlIdentifier || SqlUtil.isLiteral(node);
    if (node instanceof SqlIdentifier) {
      return ((SqlIdentifier) node).getSimple();
    }
    return ((SqlLiteral) node).getValueAs(String.class);
  }
}
