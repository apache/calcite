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
package org.apache.calcite.tools.visualizer;

import org.apache.calcite.plan.RelOptCost;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.Locale;

/**
 * VisualizerNodeInfo is a helper data class for Volcano Visualizer.
 * This class will be serialized to JSON and read by the Javascript file.
 */
public class VisualizerNodeInfo {

  private String label;
  private boolean isSubset;
  private String explanation;
  private String finalCost;

  public VisualizerNodeInfo(String label, boolean isSubset, String explanation,
      @Nullable RelOptCost finalCost, Double rowCount) {
    this.label = label;
    this.isSubset = isSubset;
    this.explanation = explanation;
    this.finalCost = formatCost(rowCount, finalCost);
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public boolean isSubset() {
    return isSubset;
  }

  public void setSubset(boolean subset) {
    isSubset = subset;
  }

  public String getExplanation() {
    return explanation;
  }

  public void setExplanation(String explanation) {
    this.explanation = explanation;
  }

  public String getFinalCost() {
    return finalCost;
  }

  public void setFinalCost(String finalCost) {
    this.finalCost = finalCost;
  }

  private static String formatCost(Double rowCount, @Nullable RelOptCost cost) {
    if (cost == null) {
      return "null";
    }
    String originalStr = cost.toString();
    if (originalStr.contains("inf") || originalStr.contains("huge")
        || originalStr.contains("tiny")) {
      return originalStr;
    }
    return new MessageFormat("\nrowCount: {0}\nrows: {1}\ncpu:  {2}\nio:   {3}'}'",
        Locale.ROOT).format(new String[] { formatCostScientific(rowCount),
        formatCostScientific(cost.getRows()),
        formatCostScientific(cost.getCpu()),
        formatCostScientific(cost.getIo()) }
    );
  }

  private static String formatCostScientific(double costNumber) {
    long costRounded = Math.round(costNumber);
    DecimalFormat formatter = (DecimalFormat) DecimalFormat.getInstance(Locale.ROOT);
    formatter.applyPattern("#.#############################################E0");
    return formatter.format(costRounded);
  }

}
