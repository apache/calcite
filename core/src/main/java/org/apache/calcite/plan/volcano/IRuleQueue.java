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
package org.apache.calcite.plan.volcano;

public interface IRuleQueue<T> {

  /**
   * Add a RuleMatch into the queue
   * @param match rule match to add
   */
  void addMatch(VolcanoRuleMatch match);

  /**
   * Achieve the next RuleMatch of a specific category
   * @return A RuleMatch of the specify category or null if there is no rule match
   */
  VolcanoRuleMatch popMatch(T category);

  /**
   * clear rule matchs of a specific category
   * @param category the specific category of rule matches to clear
   */
  void clear(T category);

  /**
   * clear this rule queue
   */
  void clear();
}
