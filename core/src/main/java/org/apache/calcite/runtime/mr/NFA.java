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

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * an NFA graph
 */
public class NFA {
  private boolean strictStarts = false;

  private boolean strictEnds = false;

  public static final int START_STATE = 0;

  private static final int ACCEPT_STATE = -1;

  private static final String EMPTY_ALPHA = "--";

  public static final NFAState FINAL_STATE = new NFAState(EMPTY_ALPHA, NFA.ACCEPT_STATE);

  private int numOfStates;

  private final List<List<NFAState>> allStates;

  private final List<List<NFAState>> allStatesNoFinals;

  private final BitSet finalStates;

  public NFA(int numOfStates) {
    this.numOfStates = numOfStates;
    finalStates = new BitSet(numOfStates);
    allStates = new ArrayList<>();
    allStatesNoFinals = new ArrayList<>();
    for (int i = 0; i < numOfStates; i++) {
      allStates.add(new ArrayList<NFAState>());
    }
  }

  public void addState(int state, NFAState tran) {
    List<NFAState> list = allStates.get(state);
    list.add(tran);
  }

  public void addState(int from, String alphaID, int to) {
    addState(from, new NFAState(alphaID, to));
  }

  public Set<String> getAlphas() {
    Set<String> alphas = new HashSet<>();
    for (int state = 0; state < numOfStates; state++) {
      List<NFAState> nfaStates = allStates.get(state);
      for (NFAState nfaState : nfaStates) {
        alphas.add(nfaState.getAlpha());
      }
    }
    return alphas;
  }

  public boolean isFinal(int state) {
    return finalStates.get(state);
  }

  public void setFinal(int state) {
    if (!isFinal(state)) {
      List<NFAState> list = allStates.get(state);
      finalStates.set(state, true);
      int idx = search(FINAL_STATE, list);
      if (idx < 0) {
        list.add(FINAL_STATE);
      }
    }
  }

  public BitSet getFinals() {
    return finalStates;
  }

  public List<Integer> getFinalsList() {
    List<Integer> result = new ArrayList<>();
    for (int i = 0; i < getNumOfStates(); i++) {
      if (finalStates.get(i)) {
        result.add(i);
      }
    }
    return result;
  }

  public void copyNFAStates(int state, List<NFAState> nfaStates, int offset) {
    for (int i = 0; i < nfaStates.size(); i++) {
      NFAState tran = nfaStates.get(i);
      if (tran.equals(FINAL_STATE) && search(FINAL_STATE, allStates.get(state)) == -1) {
        addState(state, FINAL_STATE);
      } else {
        addState(state, tran.copy(offset));
      }
    }
  }

  /**
   * get all nfa starts from the starting
   */
  public List<NFAState> getStartStates() {
    return allStates.get(START_STATE);
  }

  public void copyNFAStates(NFA copy) {
    for (int i = 0; i < copy.getNumOfStates(); i++) {
      for (NFAState tran : copy.allStates.get(i)) {
        addState(i, tran.copy());
      }
    }
  }

  public void copyFinalsFrom(NFA copy) {
    BitSet finalCopy = copy.getFinals();
    for (int i = 0; i < finalCopy.size(); i++) {
      if (finalCopy.get(i)) {
        finalStates.set(i, true);
      }
    }
  }

  public NFA copy() {
    NFA copy = new NFA(numOfStates);
    for (int i = 0; i < numOfStates; i++) {
      copy.allStates.set(i, new ArrayList<NFAState>(allStates.get(i)));
    }
    copy.finalStates.or(finalStates);
    copy.strictEnds = strictEnds;
    copy.strictStarts = strictStarts;
    return copy;
  }

  // concat two NFAs, eg. (A B)* and (C D), add transitions for C to list of B
  public void concatNFAStatesTo(int state, List<NFAState> nfaStates, int offset) {
    List<NFAState> list = new ArrayList<>();
    for (NFAState nfaState : nfaStates) {
      nfaState = nfaState.equals(FINAL_STATE) ? FINAL_STATE : nfaState.copy(offset);
      list.add(nfaState);
    }
    List<NFAState> tmp = allStates.get(state);
    int idx = search(FINAL_STATE, allStates.get(state));
    tmp.remove(idx);
    tmp.addAll(idx, list);
  }

  private int search(NFAState nfaState, List<NFAState> nfaStates) {
    for (int i = 0; i < nfaStates.size(); i++) {
      if (nfaState.equals(nfaStates.get(i))) {
        return i;
      }
    }
    return -1;
  }

  public int getNumOfStates() {
    return numOfStates;
  }

  private void saveStatesNoFinals() {
    allStatesNoFinals.clear();
    for (int state = 0; state < allStates.size(); state++) {
      List<NFAState> list = allStates.get(state);
      List<NFAState> copy = new ArrayList<>();
      for (NFAState nfaState : list) {
        if (nfaState.equals(FINAL_STATE)) {
          continue;
        }
        copy.add(nfaState.copy());
      }
      allStatesNoFinals.add(copy);
    }
  }

  public List<NFAState> getStateNoFinals(int state) {
    if (allStatesNoFinals.size() == 0) {
      saveStatesNoFinals();
    }
    return allStatesNoFinals.get(state);
  }

  public List<NFAState> getStatesAt(int state) {
    return allStates.get(state);
  }

  public String toString() {
    return print(allStates, true);
  }

  private String print(List<List<NFAState>> allStates, boolean withFinals) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < allStates.size(); i++) {
      for (NFAState nfaState : allStates.get(i)) {
        if (!withFinals) {
          if (nfaState.equals(NFA.FINAL_STATE)) {
            continue;
          }
        }
        builder.append("State: ").append(i).append(" on ").append(nfaState);
        builder.append("\n");
      }
    }
    for (int i = 0; i < numOfStates; i++) {
      if (finalStates.get(i)) {
        builder.append("Final: ").append(i);
        builder.append("\n");
      }
    }
    return builder.toString().trim();
  }

  public List<Pair<Integer, Pair<String, Integer>>> getAllStatesNoFinal() {
    final List<Pair<Integer, Pair<String, Integer>>> result = new ArrayList<>();
    for (int i = 0; i < getNumOfStates(); i++) {
      for (NFAState state : getStateNoFinals(i)) {
        String alpha = state.getAlpha();
        int to = state.getTo();
        result.add(Pair.of(i, Pair.of(alpha, to)));
      }
    }
    return result;
  }

  public boolean isStrictStarts() {
    return strictStarts;
  }

  public void setStrictStarts(boolean strictStarts) {
    this.strictStarts = strictStarts;
  }

  public boolean isStrictEnds() {
    return strictEnds;
  }

  public void setStrictEnds(boolean strictEnds) {
    this.strictEnds = strictEnds;
  }
}

// End NFA.java
