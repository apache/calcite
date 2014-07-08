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
package org.apache.optiq.sql.test;

import org.apache.optiq.sql.SqlOperatorTable;
import org.apache.optiq.sql.advise.SqlAdvisor;
import org.apache.optiq.sql.parser.SqlParser;
import org.apache.optiq.sql.validate.SqlValidator;
import org.apache.optiq.sql.validate.SqlValidatorWithHints;

/**
* Creates the objects needed to run a SQL parsing or validation test.
 *
 * @see org.apache.optiq.sql.test.SqlTester
*/
public interface SqlTestFactory {
  SqlOperatorTable createOperatorTable();
  SqlParser createParser(SqlTestFactory factory, String sql);
  SqlValidator getValidator(SqlTestFactory factory);
  SqlAdvisor createAdvisor(SqlValidatorWithHints validator);
  Object get(String name);
}

// End SqlTestFactory.java
