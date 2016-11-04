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
package org.apache.calcite.sql.parser.extension;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;

import org.junit.Test;

/**
 * Testing for extension functionality of the base SQL parser impl.
 *
 * This test runs all test cases of the base {@link SqlParserTest}, as well
 * as verifying specific extension points.
 */
public class ExtensionSqlParserTest extends SqlParserTest {

  @Override protected SqlParserImplFactory parserImplFactory() {
    return ExtensionSqlParserImpl.FACTORY;
  }

  @Test
  public void testAlterSystemExtension() throws SqlParseException {
    check("alter system upload jar '/path/to/jar'",
      "ALTER SYSTEM UPLOAD JAR '/path/to/jar'");
  }

  @Test
  public void testAlterSystemExtensionWithoutAlter() throws SqlParseException {
    // We need to include the scope for custom alter operations
    checkFails("^upload^ jar '/path/to/jar'",
      "(?s).*Encountered \"upload\" at .*");
  }
}

// End ExtensionSqlParserTest.java
