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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;

/**
 * Namespace for a {@code MATCH_RECOGNIZE} clause.
 */
public class MatchRecognizeNamespace extends AbstractNamespace {
  private final SqlMatchRecognize matchRecognize;

  /** Creates a MatchRecognizeNamespace. */
  protected MatchRecognizeNamespace(SqlValidatorImpl validator,
      SqlMatchRecognize matchRecognize,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.matchRecognize = matchRecognize;
  }

  @Override public RelDataType validateImpl(RelDataType targetRowType) {
    validator.validateMatchRecognize(matchRecognize);
    return rowType;
  }

  @Override public SqlMatchRecognize getNode() {
    return matchRecognize;
  }
}

// End MatchRecognizeNamespace.java
