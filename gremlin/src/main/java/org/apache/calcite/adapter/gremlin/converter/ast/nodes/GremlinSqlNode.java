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
package org.apache.calcite.adapter.gremlin.converter.ast.nodes;

import org.apache.calcite.adapter.gremlin.converter.SqlMetadata;
import org.apache.calcite.sql.SqlNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class in the GremlinSql equivalent of SqlNode.
 */
public abstract class GremlinSqlNode {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinSqlNode.class);
    private final SqlNode sqlNode;
    private final SqlMetadata sqlMetadata;

  public GremlinSqlNode(SqlNode sqlNode, SqlMetadata sqlMetadata) {
    this.sqlNode = sqlNode;
    this.sqlMetadata = sqlMetadata;
  }
}
