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
 *   <li>Top mode hint, right after the select keyword, i.e.:
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
  private final List<String> optionList;
  private final Map<String, String> optionKVPairs;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("HINT", SqlKind.HINT);

  //~ Constructors -----------------------------------------------------------

  public SqlHint(SqlParserPos pos, SqlIdentifier name, SqlNodeList options) {
    super(pos);
    this.name = name;
    this.options = options;
    this.optionList = getOptionAsList();
    this.optionKVPairs = getOptionAsKVPairs();
  }

  //~ Methods ----------------------------------------------------------------

  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableList.of(name, options);
  }

  /**
   * @return The sql hint name
   */
  public String getName() {
    return name.getSimple();
  }

  public List<String> getOptionList() {
    return optionList;
  }

  public Map<String, String> getOptionKVPairs() {
    return optionKVPairs;
  }

  /**
   * @return A string list if the hint option is a list of
   * simple SQL identifier, else an empty list
   */
  private List<String> getOptionAsList() {
    if (this.options.size() == 0 || isOptionsAsKVPairs()) {
      return ImmutableList.of();
    } else {
      final List<String> attrs = this.options.getList().stream()
          .map(node -> ((SqlIdentifier) node).getSimple())
          .collect(Collectors.toList());
      return ImmutableList.copyOf(attrs);
    }
  }

  /**
   * @return A key value string map if the hint option is a list of
   * pair that contains a simple SQL identifier and a string literal,
   * else an empty map
   */
  private Map<String, String> getOptionAsKVPairs() {
    if (this.options.size() == 0 || !isOptionsAsKVPairs()) {
      return ImmutableMap.of();
    } else {
      final Map<String, String> attrs = new HashMap<>();
      for (int i = 0; i < this.options.size() - 1; i += 2) {
        final SqlNode k = this.options.get(i);
        final SqlNode v = this.options.get(i + 1);
        assert k instanceof SqlIdentifier;
        assert v instanceof SqlLiteral;
        attrs.put(((SqlIdentifier) k).getSimple(),
            ((SqlLiteral) v).getValueAs(String.class));
      }
      return ImmutableMap.copyOf(attrs);
    }
  }

  private boolean isOptionsAsKVPairs() {
    return this.options.getList()
        .stream()
        .anyMatch(node -> node instanceof SqlLiteral);
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
}

// End SqlHint.java
