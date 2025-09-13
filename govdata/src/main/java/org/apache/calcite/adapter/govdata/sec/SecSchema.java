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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * SEC-specific schema implementation that extends FileSchema.
 */
public class SecSchema extends FileSchema implements CommentableSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecSchema.class);

  @SuppressWarnings("unchecked")
  public SecSchema(Schema parentSchema, String name, Map<String, Object> operand) {
    // Cast parentSchema to SchemaPlus which FileSchema expects
    super((SchemaPlus) parentSchema, name,
          getSourceDirectory(operand),
          (List<Map<String, Object>>) operand.get("tables"));
    LOGGER.debug("SecSchema created with name: {}", name);
  }

  private static File getSourceDirectory(Map<String, Object> operand) {
    String directory = (String) operand.get("directory");
    if (directory != null) {
      return new File(directory);
    }
    // Use default SEC cache directory
    String cacheHome = System.getenv("SEC_CACHE_HOME");
    if (cacheHome == null) {
      cacheHome = System.getProperty("user.home") + "/.calcite/sec-cache";
    }
    return new File(cacheHome);
  }

  @Override public @Nullable String getComment() {
    return "Securities and Exchange Commission financial data including "
        + "XBRL filings (10-K, 10-Q, 8-K), insider trading transactions, "
        + "stock prices, and earnings transcripts. "
        + "Enables financial analysis, regulatory compliance monitoring, "
        + "and investment research across public companies.";
  }
}
