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

import org.apache.calcite.rel.RelNode;

import java.util.EventListener;
import java.util.EventObject;

/**
 * RelOptListener defines an interface for listening to events which occur
 * during the optimization process.
 */
public interface RelOptListener extends EventListener {
  //~ Methods ----------------------------------------------------------------

  /**
   * Notifies this listener that a relational expression has been registered
   * with a particular equivalence class after an equivalence has been either
   * detected or asserted. Equivalence classes may be either logical (all
   * expressions which yield the same result set) or physical (all expressions
   * which yield the same result set with a particular calling convention).
   *
   * @param event details about the event
   */
  void relEquivalenceFound(RelEquivalenceEvent event);

  /**
   * Notifies this listener that an optimizer rule is being applied to a
   * particular relational expression. This rule is called twice; once before
   * the rule is invoked, and once after. Note that the rel attribute of the
   * event is always the old expression.
   *
   * @param event details about the event
   */
  void ruleAttempted(RuleAttemptedEvent event);

  /**
   * Notifies this listener that an optimizer rule has been successfully
   * applied to a particular relational expression, resulting in a new
   * equivalent expression (relEquivalenceFound will also be called unless the
   * new expression is identical to an existing one). This rule is called
   * twice; once before registration of the new rel, and once after. Note that
   * the rel attribute of the event is always the new expression; to get the
   * old expression, use event.getRuleCall().rels[0].
   *
   * @param event details about the event
   */
  void ruleProductionSucceeded(RuleProductionEvent event);

  /**
   * Notifies this listener that a relational expression is no longer of
   * interest to the planner.
   *
   * @param event details about the event
   */
  void relDiscarded(RelDiscardedEvent event);

  /**
   * Notifies this listener that a relational expression has been chosen as
   * part of the final implementation of the query plan. After the plan is
   * complete, this is called one more time with null for the rel.
   *
   * @param event details about the event
   */
  void relChosen(RelChosenEvent event);

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Event class for abstract event dealing with a relational expression. The
   * source of an event is typically the RelOptPlanner which initiated it.
   */
  abstract class RelEvent extends EventObject {
    private final RelNode rel;

    protected RelEvent(Object eventSource, RelNode rel) {
      super(eventSource);
      this.rel = rel;
    }

    public RelNode getRel() {
      return rel;
    }
  }

  /** Event indicating that a relational expression has been chosen. */
  class RelChosenEvent extends RelEvent {
    public RelChosenEvent(Object eventSource, RelNode rel) {
      super(eventSource, rel);
    }
  }

  /** Event indicating that a relational expression has been found to
   * be equivalent to an equivalence class. */
  class RelEquivalenceEvent extends RelEvent {
    private final Object equivalenceClass;
    private final boolean isPhysical;

    public RelEquivalenceEvent(
        Object eventSource,
        RelNode rel,
        Object equivalenceClass,
        boolean isPhysical) {
      super(eventSource, rel);
      this.equivalenceClass = equivalenceClass;
      this.isPhysical = isPhysical;
    }

    public Object getEquivalenceClass() {
      return equivalenceClass;
    }

    public boolean isPhysical() {
      return isPhysical;
    }
  }

  /** Event indicating that a relational expression has been discarded. */
  class RelDiscardedEvent extends RelEvent {
    public RelDiscardedEvent(Object eventSource, RelNode rel) {
      super(eventSource, rel);
    }
  }

  /** Event indicating that a planner rule has fired. */
  abstract class RuleEvent extends RelEvent {
    private final RelOptRuleCall ruleCall;

    protected RuleEvent(
        Object eventSource,
        RelNode rel,
        RelOptRuleCall ruleCall) {
      super(eventSource, rel);
      this.ruleCall = ruleCall;
    }

    public RelOptRuleCall getRuleCall() {
      return ruleCall;
    }
  }

  /** Event indicating that a planner rule has been attempted. */
  class RuleAttemptedEvent extends RuleEvent {
    private final boolean before;

    public RuleAttemptedEvent(
        Object eventSource,
        RelNode rel,
        RelOptRuleCall ruleCall,
        boolean before) {
      super(eventSource, rel, ruleCall);
      this.before = before;
    }

    public boolean isBefore() {
      return before;
    }
  }

  /** Event indicating that a planner rule has produced a result. */
  class RuleProductionEvent extends RuleAttemptedEvent {
    public RuleProductionEvent(
        Object eventSource,
        RelNode rel,
        RelOptRuleCall ruleCall,
        boolean before) {
      super(eventSource, rel, ruleCall, before);
    }
  }
}

// End RelOptListener.java
