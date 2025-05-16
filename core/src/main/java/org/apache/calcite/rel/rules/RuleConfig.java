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
package org.apache.calcite.rel.rules;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The {@code @RuleConfig} annotation is exclusively used for testing purposes
 * to identify configuration names when a rule contains multiple Configs.
 *
 * <p>Usage Example:
 *
 * <pre>
 * &#64;RuleConfig(value = "FILTER")
 * public static final ExpandDisjunctionForJoinInputsRule
 *   EXPAND_FILTER_DISJUNCTION_LOCAL =
 *       ExpandDisjunctionForJoinInputsRule.Config.FILTER.toRule();
 * &#64;RuleConfig(value = "JOIN")
 * public static final ExpandDisjunctionForJoinInputsRule
 *   EXPAND_JOIN_DISJUNCTION_LOCAL =
 *       ExpandDisjunctionForJoinInputsRule.Config.JOIN.toRule();
 * </pre>
 *
 * <p>Key Characteristics:
 * <ul>
 *   <li>Test-scoped annotation (not used in production code)</li>
 *   <li>Required when a rule class has multiple Configs</li>
 *   <li>Annotation value must match the static variable that holds the Config</li>
 * </ul>
 *
 * @see CoreRules#EXPAND_FILTER_DISJUNCTION_GLOBAL
 * @see CoreRules#EXPAND_JOIN_DISJUNCTION_LOCAL
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RuleConfig {
  String value();
}
