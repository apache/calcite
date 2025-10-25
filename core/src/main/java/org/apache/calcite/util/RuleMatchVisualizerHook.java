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
package org.apache.calcite.util;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.visualizer.RuleMatchVisualizer;
import org.apache.calcite.runtime.Hook;

import java.io.File;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Utility class to enable RuleMatchVisualizer for Calcite connections.
 *
 * <p>This class provides hooks to automatically attach a RuleMatchVisualizer
 * to planners when a connection specifies the ruleVisualizerDir property.
 *
 * <p>Usage in JDBC URL:
 * <blockquote><pre>
 * jdbc:calcite:ruleVisualizerDir=/tmp/calcite-viz
 * </pre></blockquote>
 *
 * <p>Or programmatically:
 * <blockquote><pre>
 * RuleMatchVisualizerHook.enable("/tmp/calcite-viz");
 * </pre></blockquote>
 */
public class RuleMatchVisualizerHook {
  public static final RuleMatchVisualizerHook INSTANCE = new RuleMatchVisualizerHook();

  private final Map<RelOptPlanner, RuleMatchVisualizer> visualizerMap = new HashMap<>();
  private final AtomicInteger queryCounter = new AtomicInteger(0);

  private Hook.Closeable hookCloseable = Hook.Closeable.EMPTY;

  /** Private constructor to prevent instantiation. */
  private RuleMatchVisualizerHook() {}

  /**
   * Enables the visualizer for all subsequent queries with the specified output directory.
   *
   * @param outputDir Directory where visualization files will be created
   */
  public synchronized void enable(String outputDir) {
    hookCloseable.close();

    // Ensure the output directory exists
    File dir = new File(outputDir);
    if (!dir.exists()) {
      boolean madeDir = dir.mkdirs();
      assert madeDir : "Failed to create directory: " + outputDir;
    }

    // Install the hook
    hookCloseable = Hook.PLANNER.addThread((Consumer<RelOptPlanner>) planner -> {
      attachVisualizer(planner, outputDir);
    });
  }

  /**
   * Enables the visualizer using the connection's configuration.
   * This method checks if the connection has the ruleVisualizerDir property set.
   *
   * @param connection The Calcite connection
   */
  public synchronized void enableFromConnection(CalciteConnection connection) {
    CalciteConnectionConfig config = connection.config();
    String vizDir = config.ruleVisualizerDir();

    if (vizDir != null && !vizDir.isEmpty()) {
      enable(vizDir);
    }
  }

  /**
   * Disables the visualizer.
   */
  public synchronized void disable() {
    hookCloseable.close();

      // Write any pending visualizations
    for (RuleMatchVisualizer viz : visualizerMap.values()) {
      viz.writeToFile();
    }
    visualizerMap.clear();
  }

  /**
   * Attaches a visualizer to the given planner.
   */
  private void attachVisualizer(RelOptPlanner planner, String outputDir) {

      // Check if we've already attached a visualizer to this planner
    if (visualizerMap.containsKey(planner)) {
      return;
    }

    int queryNum = queryCounter.incrementAndGet();
    int queryStart = (int) System.currentTimeMillis() / 1000;
    String suffix = String.format(Locale.ROOT, "query_%d_%d", queryNum, queryStart);

    // Create and attach the visualizer
    RuleMatchVisualizer visualizer = new RuleMatchVisualizer(outputDir, suffix);
    visualizer.attachTo(planner);
    visualizerMap.put(planner, visualizer);

    // For HepPlanner, we need to manually write the output
    if (planner instanceof HepPlanner) {
      // Add a hook to write the visualization after the planner finishes
      Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(relRoot -> {
        RuleMatchVisualizer viz = visualizerMap.get(planner);
        if (viz != null) {
          viz.writeToFile();
          visualizerMap.remove(planner);
        }
      });
    }
    // VolcanoPlanner automatically calls writeToFile() when done

    System.out.println("RuleMatchVisualizer enabled: Output will be written to "
        + outputDir + File.separator + suffix + "*");
  }

  /**
   * Checks the system property and enables visualization if set.
   * This can be called at application startup.
   */
  public void checkSystemProperty() {
    String vizDir = System.getProperty("calcite.visualizer.dir");
    if (vizDir != null && !vizDir.isEmpty()) {
      enable(vizDir);
    }
  }
}
