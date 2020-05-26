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

/**
 * Represents a logical node in Calcite
 */
public interface LogicalNode extends RelNode {

  /**
   * Confidence levels of statistics estimation
   */
  enum StatsEstimateConfidenceLevel {
    None,
    Low,
    Medium,
    High
  }

  /**
   * Get confidence level of statistics estimation
   * @return Confidence level of statistics estimation
   */
  default StatsEstimateConfidenceLevel getStatsEstimateConfidence() {
    return StatsEstimateConfidenceLevel.Medium;
  }
}
