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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Extension to {@link SubstitutionVisitor}.
 */
@Deprecated // Kept for backward compatibility and to be removed before 2.0
public class MaterializedViewSubstitutionVisitor extends SubstitutionVisitor {

  public MaterializedViewSubstitutionVisitor(RelNode target_, RelNode query_) {
    super(target_, query_, DEFAULT_RULES);
  }

  public MaterializedViewSubstitutionVisitor(RelNode target_, RelNode query_,
      RelBuilderFactory relBuilderFactory) {
    super(target_, query_, DEFAULT_RULES, relBuilderFactory);
  }
}

// End MaterializedViewSubstitutionVisitor.java
