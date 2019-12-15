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
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A <code>SqlHint</code> is a node of a parse tree which represents
 * a sql hint expression.
 *
 * <p>Basic hint grammar is: hint_name[(option1, option2 ...)].
 * The hint_name should be a simple identifier, the options part is optional.
 * For every option, it can be a simple identifier or a key value pair whose key
 * is a simple identifier and value is a string literal. The identifier option and key
 * value pair can not be mixed in, they should be either all simple identifiers
 * or all key value pairs.
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
      new SqlSpecialOperator("HINT", SqlKind.HINT);

  //~ Constructors -----------------------------------------------------------

  public SqlHint(SqlParserPos pos, SqlIdentifier name, SqlNodeList options) {
    super(pos);
    this.name = name;
    this.optionFormat = inferHintOptionFormat(options);
    this.options = options;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, options);
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
   * simple SQL identifier, else an empty list.
   */
  public List<String> getOptionList() {
    if (optionFormat == HintOptionFormat.ID_LIST) {
      final List<String> attrs = options.getList().stream()
          .map(node -> ((SqlIdentifier) node).getSimple())
          .collect(Collectors.toList());
      return ImmutableList.copyOf(attrs);
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
        attrs.put(((SqlIdentifier) k).getSimple(),
            ((SqlLiteral) v).getValueAs(String.class));
      }
      return ImmutableMap.copyOf(attrs);
    } else {
      return ImmutableMap.of();
    }
  }

  public Map<Object, Object> getOptionLiteralKVPairs() {
    if (optionFormat == HintOptionFormat.LITERAL_KV_LIST) {
      final Map<Object, Object> attrs = new HashMap<>();
      for (int i = 0; i < options.size() - 1; i += 2) {
        final SqlNode k = options.get(i);
        final SqlNode v = options.get(i + 1);
        ((SqlLiteral) k).getValue();
        attrs.put(((SqlLiteral) k).getValue(), ((SqlLiteral) k).getValue());
      }
      return ImmutableMap.copyOf(attrs);
    } else {
      return ImmutableMap.of();
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, leftPrec, rightPrec);
    if (this.options.size() > 0) {
      SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      for (int i = 0; i < options.size(); i++) {
        SqlNode option = options.get(i);
        SqlNode nextOption = i < options.size() - 1 ? options.get(i + 1) : null;
        writer.sep(",", false);
        option.unparse(writer, leftPrec, rightPrec);
        if (nextOption instanceof SqlLiteral) {
          writer.print("=");
          nextOption.unparse(writer, leftPrec, rightPrec);
          i += 1;
        }
      }
      writer.endList(frame);
    }
  }

  /** Enumeration that represents hint option format. */
  enum HintOptionFormat {
    /**
     * The hint has no options.
     */
    EMPTY,
    /**
     * The hint options are as simple identifier list.
     */
    ID_LIST,
    /**
     * The hint options are list of key-value pairs. For each pair,
     * the key is a simple identifier, the value is a string literal.
     */
    KV_LIST,

    LITERAL_KV_LIST,
  }

  //~ Tools ------------------------------------------------------------------

  /** Infer the hint options format. */
  private static HintOptionFormat inferHintOptionFormat(SqlNodeList options) {
    if (options.size() == 0) {
      return HintOptionFormat.EMPTY;
    }
    if (options.getList().stream().allMatch(opt -> opt instanceof SqlIdentifier)) {
      return HintOptionFormat.ID_LIST;
    }
    if (isOptionsAsKVPairs(options)) {
      return HintOptionFormat.KV_LIST;
    }
    if (isOptionsAsLiteralKVPairs(options)) {
      return HintOptionFormat.LITERAL_KV_LIST;
    }
    throw new AssertionError("The hint options should either be empty, "
        + "or simple identifier list, "
        + "or key-value pairs whose pair key is simple identifier and value is string literal, "
        + "or key-value pairs whose key and value are both literals.");
  }

  /** Decides if the hint options is as key-value pair format. */
  private static boolean isOptionsAsKVPairs(SqlNodeList options) {
    if (options.size() > 0 && options.size() % 2 == 0) {
      for (int i = 0; i < options.size() - 1; i += 2) {
        boolean isKVPair = options.get(i) instanceof SqlIdentifier
            && options.get(i + 1) instanceof SqlLiteral
            && ((SqlLiteral) options.get(i + 1)).getTypeName() == SqlTypeName.CHAR;
        if (!isKVPair) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private static boolean isOptionsAsLiteralKVPairs(SqlNodeList options) {
    if (options.size() > 0 && options.size() % 2 == 0) {
      for (int i = 0; i < options.size() - 1; i += 2) {
        boolean isKVPair = options.get(i) instanceof SqlLiteral
            && options.get(i + 1) instanceof SqlLiteral;
        if (!isKVPair) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}

// End SqlHint.java
