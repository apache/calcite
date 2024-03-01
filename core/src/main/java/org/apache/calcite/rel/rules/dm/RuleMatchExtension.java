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
package org.apache.calcite.rel.rules.dm;

import org.apache.calcite.plan.RelOptRuleCall;

/**
 * We divide Rule into two part
 * 1. Pattern - Which define when particular rule is being matched.
 * 2. Execution(OnMatch) - What kind of action or logic is being executed.
 *
 * For certain situation we need to execute some logic outside calcite,
 * however pattern define in Calcite end.
 *
 * To achieve this we provide extension, and we just need to implement it.
 * Logic is being executed as part of OnMatch call from the calcite itself.
 */
public interface RuleMatchExtension {

    void execute(RelOptRuleCall call);
}
