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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Most {@link EnumerableRel} nodes assume that their convention and the convention of their inputs
 * is {@link EnumerableConvention}, and this interface helps to validate that.
 */
interface EnumerableUtils {
  static boolean assertSelfAndInputs(RelNode node) {
    assert assertConvention(node) && assertConvention(node.getInputs());
    return true;
  }

  static boolean assertConvention(RelNode node) {
    assert node.getConvention() instanceof EnumerableConvention
        : "convention must be ENUMERABLE, got " + node.getConvention()
        + " in " + RelOptUtil.toString(node);
    return true;
  }

  static boolean assertConvention(List<RelNode> nodes) {
    for (RelNode node : nodes) {
      assertConvention(node);
    }
    return true;
  }
}
