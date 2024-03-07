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
package org.apache.calcite.sql.ddl;

import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Base class for parse tree of {@code GRANT} and {@code REVOKE} statements.
 */
public abstract class SqlAuthCommand extends SqlDdl {

  public final SqlNodeList accesses;
  public final SqlNodeList objects;
  public final ObjectType type;
  public final SqlNodeList users;

  /** Creates a {@code SqlAuthorization}. */
  protected SqlAuthCommand(SqlOperator operator, SqlParserPos pos, SqlNodeList accesses,
      SqlNodeList objects, ObjectType type, SqlNodeList users) {
    super(operator, pos);
    this.accesses = requireNonNull(accesses, "accesses");
    this.objects = requireNonNull(objects, "objects");
    this.type = requireNonNull(type, "type");
    this.users = requireNonNull(users, "users");
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableList.of(accesses, objects, users);
  }

  /**
   * Object type for SqlAuthCommand.
   */
  public enum ObjectType {
    TABLE, SCHEMA, ROOT_SCHEMA
  }

}
