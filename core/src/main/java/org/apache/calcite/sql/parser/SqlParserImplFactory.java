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
package org.eigenbase.sql.parser;

import java.io.Reader;

/**
 * Factory for {@link org.eigenbase.sql.parser.SqlAbstractParserImpl} objects.
 *
 * <p>A parser factory allows you to include a custom parser in
 * {@link net.hydromatic.optiq.tools.Planner} created through
 * {@link net.hydromatic.optiq.tools.Frameworks}.</p>
 */
public interface SqlParserImplFactory {

  /**
   * Get the underlying parser implementation.
   *
   * @return {@link SqlAbstractParserImpl} object.
   */
  SqlAbstractParserImpl getParser(Reader stream);
}
