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
package org.apache.calcite.util;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlTrimFunction;

/**
 * The usage of this class is to put the common code that can be used by multiple dialect for
 * RelToSql conversion.
 */
public class RelToSqlConverterUtil {

  private RelToSqlConverterUtil() {
  }


  /**
   * This method will make regex pattern based on the TRIM flag
   *
   * @param call     SqlCall contains the values that needs to be trimmed
   * @param trimFlag It will contain the trimFlag either BOTH,LEADING or TRAILING
   * @return It will return the regex pattern of the character to be trimmed.
   */
  public static SqlCharStringLiteral makeRegexNodeFromCall(SqlNode call, SqlLiteral trimFlag) {
    String regexPattern = ((SqlCharStringLiteral) call).toValue();
    regexPattern = escapeSpecialChar(regexPattern);
    switch (trimFlag.getValueAs(SqlTrimFunction.Flag.class)) {
    case LEADING:
      regexPattern = "^(".concat(regexPattern).concat(")*");
      break;
    case TRAILING:
      regexPattern = "(".concat(regexPattern).concat(")*$");
      break;
    default:
      regexPattern = "^(".concat(regexPattern).concat(")*|(")
          .concat(regexPattern).concat(")*$");
      break;
    }
    return SqlLiteral.createCharString(regexPattern,
        call.getParserPosition());
  }

  /**
   * This method will escpae the special character
   *
   * @param inputString String
   * @return Escape character if any special character is present in the string
   */
  private static String escapeSpecialChar(String inputString) {
    final String[] specialCharacters = {
        "\\", "^", "$", "{", "}", "[", "]", "(", ")", ".",
        "*", "+", "?", "|", "<", ">", "-", "&", "%", "@"
    };

    for (int i = 0; i < specialCharacters.length; i++) {
      if (inputString.contains(specialCharacters[i])) {
        inputString = inputString.replace(specialCharacters[i], "\\" + specialCharacters[i]);
      }
    }
    return inputString;
  }
}
