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

import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.format.FormatElementEnum;
import org.apache.calcite.util.format.FormatModelElement;
import org.apache.calcite.util.format.FormatModelElementAlias;
import org.apache.calcite.util.format.FormatModelElementLiteral;
import org.apache.calcite.util.format.FormatModelUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.util.format.FormatElementEnum.D;
import static org.apache.calcite.util.format.FormatElementEnum.DAY;
import static org.apache.calcite.util.format.FormatElementEnum.DD;
import static org.apache.calcite.util.format.FormatElementEnum.DDD;
import static org.apache.calcite.util.format.FormatElementEnum.DY;
import static org.apache.calcite.util.format.FormatElementEnum.HH24;
import static org.apache.calcite.util.format.FormatElementEnum.IW;
import static org.apache.calcite.util.format.FormatElementEnum.MI;
import static org.apache.calcite.util.format.FormatElementEnum.MM;
import static org.apache.calcite.util.format.FormatElementEnum.MON;
import static org.apache.calcite.util.format.FormatElementEnum.MONTH;
import static org.apache.calcite.util.format.FormatElementEnum.Q;
import static org.apache.calcite.util.format.FormatElementEnum.SS;
import static org.apache.calcite.util.format.FormatElementEnum.TZR;
import static org.apache.calcite.util.format.FormatElementEnum.WW;
import static org.apache.calcite.util.format.FormatElementEnum.YYYY;

/** A <a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlqr/Format-Models.html">
 * format model</a> is a character literal that describes the format of {@code DATETIME} or {@code
 * NUMBER} data stored in a character string.
 *
 * <p>{@link #unparse(SqlWriter, SqlCall, int, int)} calls
 * {@link SqlDialect#getFormatElement(FormatElementEnum)} for known elements and aliases. Consider
 * overriding this method if a dialect's format elements differs from those in {@link
 * FormatElementEnum}
 */
public class FormatModel extends SqlInternalOperator {

  private final List<FormatModelElement> elements;
  private final ImmutableMap<String, FormatModelElement> fmtModelParseMap;

  /**
   * TODO(CALCITE-2980): This should live elsewhere and be associated with {@link SqlLibrary}
   * or {@link org.apache.calcite.config.Lex}.
   */
  public static final ImmutableMap<String, FormatModelElement> BIG_QUERY_FORMAT_ELEMENT_PARSE_MAP =
      FormatModelElement.listToMap(
          new ImmutableList.Builder<FormatModelElement>()
              .add(FormatModelElementAlias.create("%A", DAY))
              .add(FormatModelElementAlias.create("%a", DY))
              .add(FormatModelElementAlias.create("%B", MONTH))
              .add(FormatModelElementAlias.create("%b", MON))
              .add(FormatModelElementAlias.create("%d", DD))
              .add(FormatModelElementAlias.create("%H", HH24))
              .add(FormatModelElementAlias.create("%h", MON))
              .add(FormatModelElementAlias.create("%j", DDD))
              .add(FormatModelElementAlias.create("%k", HH24))
              .add(FormatModelElementAlias.create("%M", MI))
              .add(FormatModelElementAlias.create("%m", MM))
              .add(FormatModelElementAlias.create("%Q", Q))
              .add(
                  FormatModelElementAlias.create("%R",
                      Arrays.asList(HH24, new FormatModelElementLiteral(":"), MI),
                      "The time in the format %H:%M"))
              .add(FormatModelElementAlias.create("%S", SS))
              .add(FormatModelElementAlias.create("%U", WW))
              .add(FormatModelElementAlias.create("%u", D))
              .add(FormatModelElementAlias.create("%V", IW))
              .add(FormatModelElementAlias.create("%W", WW))
              .add(FormatModelElementAlias.create("%Y", YYYY))
              .add(FormatModelElementAlias.create("%Z", TZR))
              .build());


  public FormatModel(String fmtString, SqlLibrary library) {
    super("FORMAT_MODEL", SqlKind.FORMAT_MODEL, MDX_PRECEDENCE, true,
        ReturnTypes.explicit(SqlTypeName.ANY).andThen(SqlTypeTransforms.TO_NULLABLE), null,
        OperandTypes.IGNORE_ANY);
    assert fmtString != null;
    this.fmtModelParseMap = formatModelParseMapFromLibrary(library);
    this.elements = FormatModelUtil.parse(fmtString, fmtModelParseMap);
  }

  /**
   * Returns a map of element patterns to be used by
   * {@link FormatModelUtil#parse(String, ImmutableMap)}.
   */
  private static ImmutableMap formatModelParseMapFromLibrary(SqlLibrary library) {
    switch (library) {
    default:
      return BIG_QUERY_FORMAT_ELEMENT_PARSE_MAP;
    }
  }

  /**
   * Returns the elements used within the format model string.
   *
   * @return list of {@link FormatModelElement} used in the format model string.
   */
  public List<FormatModelElement> getElements() {
    return this.elements;
  }

  /**
   * Populates {@code buf} by iterating over {@code fmtElements} and calling {@link
   * SqlDialect#getFormatElement(FormatElementEnum)} for aliased or standard elements and {@link
   * FormatModelElementLiteral#getLiteral()} for literals.
   */
  private void unparseElements(SqlDialect dialect, StringBuilder buf,
      List<FormatModelElement> fmtElements) {
    fmtElements.forEach(ele -> {
      if (ele.getClass() == FormatModelElementLiteral.class) {
        buf.append(ele.getLiteral());
      } else if (ele.isAlias()) {
        // recursive case - an alias represents one or more standard elements, or another alias.
        unparseElements(dialect, buf, ele.getElements());
      } else {
        buf.append(dialect.getFormatElement(FormatElementEnum.valueOf(ele.getToken())));
      }
    });
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    StringBuilder buf = new StringBuilder();
    SqlDialect dialect = writer.getDialect();
    unparseElements(dialect, buf, this.elements);
    writer.literal(dialect.quoteStringLiteral(buf.toString()));
  }
}
