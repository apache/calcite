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
package org.apache.calcite.rel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;

import java.util.List;

/**
 * Interface for physical nodes.
 */
public interface PhysicalNode extends RelNode {

  /**
   * This method called after the inputs were optimized with the desired traits
   * (usually {@link RelOptRule#convert(RelNode, RelTrait)} is used for this purpose in
   * implementation rules).
   * This method should return a copy of the given Rel with the traits derived from the inputs.
   * @param newInputs New inputs of this rel.
   * @return A copy of the given Rel with the traits derived from the inputs.
   */
  PhysicalNode withNewInputs(List<RelNode> newInputs);
}
