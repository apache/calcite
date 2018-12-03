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

import java.util.ArrayList;
import java.util.List;

/**
 * MulticastRelOptListener implements the {@link RelOptListener} interface by
 * forwarding events on to a collection of other listeners.
 */
public class MulticastRelOptListener implements RelOptListener {
  //~ Instance fields --------------------------------------------------------

  private final List<RelOptListener> listeners;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new empty multicast listener.
   */
  public MulticastRelOptListener() {
    listeners = new ArrayList<>();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Adds a listener which will receive multicast events.
   *
   * @param listener listener to add
   */
  public void addListener(RelOptListener listener) {
    listeners.add(listener);
  }

  // implement RelOptListener
  public void relEquivalenceFound(RelEquivalenceEvent event) {
    for (RelOptListener listener : listeners) {
      listener.relEquivalenceFound(event);
    }
  }

  // implement RelOptListener
  public void ruleAttempted(RuleAttemptedEvent event) {
    for (RelOptListener listener : listeners) {
      listener.ruleAttempted(event);
    }
  }

  // implement RelOptListener
  public void ruleProductionSucceeded(RuleProductionEvent event) {
    for (RelOptListener listener : listeners) {
      listener.ruleProductionSucceeded(event);
    }
  }

  // implement RelOptListener
  public void relChosen(RelChosenEvent event) {
    for (RelOptListener listener : listeners) {
      listener.relChosen(event);
    }
  }

  // implement RelOptListener
  public void relDiscarded(RelDiscardedEvent event) {
    for (RelOptListener listener : listeners) {
      listener.relDiscarded(event);
    }
  }
}

// End MulticastRelOptListener.java
