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
import org.apache.calcite.util.NlsString;

/**
 * Composition of a string literal and an array value.
 *
 * <p> In some cases string literal can be cast to array value
 * (for example: SELECT ARRAY[0,1] = '{0,1}'). An object of this class stores a string literal
 * and array value gotten from the string literal.
 *
 * {@link org.apache.calcite.sql.validate.implicit.TypeCoercionImpl#coerceStringToArray}
 */
public class SqlArrayCharStringLiteral extends SqlCharStringLiteral {

  protected final SqlCall arrayValue;

  protected SqlArrayCharStringLiteral(SqlCall arrayValue, NlsString val, SqlParserPos pos) {
    super(val, pos);
    this.arrayValue = arrayValue;
  }

  public SqlCall asArrayValue() {
    return arrayValue;
  }
}
