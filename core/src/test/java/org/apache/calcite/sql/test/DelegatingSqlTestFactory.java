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
package org.eigenbase.sql.test;

import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.advise.SqlAdvisor;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorWithHints;

/**
* Implementation of {@link SqlTestFactory} that delegates
 * everything to an underlying factory.
 *
 * <p>Generally a chain starts with a
 * {@link org.eigenbase.sql.test.DefaultSqlTestFactory}, and continues with a
 * succession of objects that derive from {@code DelegatingSqlTestFactory}
 * and override one method.</p>
 *
 * <p>Methods such as {@link org.eigenbase.sql.test.SqlTester#withConformance}
 * help create such chains.</p>
*/
public class DelegatingSqlTestFactory implements SqlTestFactory {
  private final SqlTestFactory factory;

  public DelegatingSqlTestFactory(SqlTestFactory factory) {
    this.factory = factory;
  }

  public Object get(String name) {
    return factory.get(name);
  }

  public SqlOperatorTable createOperatorTable() {
    return factory.createOperatorTable();
  }

  public SqlAdvisor createAdvisor(SqlValidatorWithHints validator) {
    return factory.createAdvisor(validator);
  }

  public SqlValidator getValidator(SqlTestFactory factory) {
    return this.factory.getValidator(factory);
  }

  public SqlParser createParser(SqlTestFactory factory, String sql) {
    return this.factory.createParser(factory, sql);
  }
}

// End DelegatingSqlTestFactory.java
