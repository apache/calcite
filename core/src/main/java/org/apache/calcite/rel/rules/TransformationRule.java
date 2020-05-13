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

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.PhysicalNode;

/**
 * Logical transformation rule, only logical operator can be rule operand,
 * and only generate logical alternatives. It is only visible to
 * {@link VolcanoPlanner}, {@link HepPlanner} will ignore this interface.
 * That means, in {@link HepPlanner}, the rule that implements
 * {@link TransformationRule} can still match with physical operator of
 * {@link PhysicalNode} and generate physical alternatives.
 *
 * <p>But in {@link VolcanoPlanner}, {@link TransformationRule} doesn't match
 * with physical operator that implements {@link PhysicalNode}. It is not
 * allowed to generate physical operators in {@link TransformationRule},
 * unless you are using it in {@link HepPlanner}.</p>
 *
 * @see VolcanoPlanner
 * @see SubstitutionRule
 */
public interface TransformationRule {
}
