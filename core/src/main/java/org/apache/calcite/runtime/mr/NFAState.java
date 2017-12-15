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
package org.apache.calcite.runtime.mr;

/**
 * each transition state in NFA
 */
public class NFAState {

  private String alpha;
  private int toState;

  public NFAState() {

  }

  public NFAState(String alphaID, int toState) {
    this.alpha = alphaID;
    this.toState = toState;
  }
  public void setAlpha(String alpha) {
    this.alpha = alpha;
  }

  public String getAlpha() {
    return alpha;
  }

  public void setTo(int toState) {
    this.toState = toState;
  }

  public int getTo() {
    return toState;
  }


  public NFAState copy() {
    NFAState copy = new NFAState();
    copy.setAlpha(alpha);
    copy.setTo(toState);
    return copy;
  }

  public NFAState copy(int offSet) {
    NFAState copy = new NFAState();
    copy.setAlpha(alpha);
    copy.setTo(toState + offSet);
    return copy;
  }

  @Override public boolean equals(Object object) {
    NFAState other  = (NFAState) object;
    return this == object || other.getAlpha().equals(alpha) && other.getTo() == toState;
  }

  @Override public int hashCode() {
    return alpha.hashCode() * 1 + toState;
  }

  public String toString() {
    return alpha + " to " + toState;
  }

}

// End NFAState.java
