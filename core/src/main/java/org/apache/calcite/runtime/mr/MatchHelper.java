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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 *
 */
public abstract class MatchHelper {

  protected boolean longestFirst = true;

  //list of aggregations
  protected List<String> aggrFun;

  //list of vars to be exclude from output, works isAllRows = true
  protected Set<String> excludeVars;

  // Z = (A, B), Y = (A, C) ==> A = [Z, Y], when a matching for A is found, update sets for Z and Y
  protected Map<String, Set<String>> reverseSubSets;

  //Z = (A, B), Y = (A, C)
  protected Map<String, Set<String>> subSets;

  //NFA parsed
  protected Nfa nfa;

  // partition key list, the order of partition key does not matter
  protected Integer[] partitionKey;

  // if all rows should be returned
  protected boolean isAllRows = false;

  protected MatchingFactory matchingFactory;

  private Map<Integer, Object> dynamicParams = new HashMap<>();

  public void init(boolean isAllRows,
                   Nfa nfa,
                   List<String> aggrFun,
                   Set<String> excludeVars,
                   Map<String, Set<String>> subSets,
                   Integer[] partitionKey) {
    this.setNfa(nfa.copy());
    this.setAggrFun(new ArrayList<String>(aggrFun));
    this.setExcludeVars(new HashSet<String>(excludeVars));
    this.setSubSets(new HashMap<String, Set<String>>(subSets));
    this.setPartitionKey(partitionKey);
    this.setAllRows(isAllRows);
    this.reverseSubSets = new HashMap<>();

    for (Map.Entry<String, Set<String>> subset : subSets.entrySet()) {
      String key = subset.getKey();
      for (String item : subset.getValue()) {
        Set<String> newSet;
        if (reverseSubSets.containsKey(item)) {
          newSet = reverseSubSets.get(item);
        } else {
          newSet = new HashSet<>();
          reverseSubSets.put(item, newSet);
        }
        newSet.add(key);
      }
    }
    this.setMatchingFactory(new MatchingFactory(aggrFun, getReverseSubSets()));
  }

  public abstract boolean defines(Map<Integer, Tuple> tuples,
                                  Map<Integer, Integer> tupleMap,
                                  Matching matching,
                                  String alpha);

  public abstract int measures(List<Tuple> output,
                               Map<Integer, Tuple> tuples,
                               Map<Integer, Integer> tupleMap,
                               Object[] keyData,
                               int mrIdx,
                               Matching matching);


  public abstract void updateAggregates(Map<Integer, Tuple> tuples,
                                        Map<Integer, Integer> tupleMap,
                                        Matching matching,
                                        String alpha);

  public abstract void addMatching(Queue<Matching> inputs,
                                   Matching matching,
                                   BitSet matchStartsAt,
                                   MatchingFactory mFactory);

  public abstract List<Object[]> parserResult(List<Tuple> output);

  protected List<Tuple> tableToList(List<Object[]> data) {
    List<Tuple> result = new ArrayList<>();
    if (data == null || data.isEmpty()) {
      return result;
    }

    for (Object[] objs : data) {
      Tuple newRow = new Tuple(objs.length);
      for (int i = 0; i < objs.length; i++) {
        newRow.setData(i, objs[i]);
      }
    }
    return result;
  }

  public List<String> getAggrFun() {
    return aggrFun;
  }

  public void setAggrFun(List<String> aggrFun) {
    this.aggrFun = aggrFun;
  }

  public Set<String> getExcludeVars() {
    return excludeVars;
  }

  public void setExcludeVars(Set<String> excludeVars) {
    this.excludeVars = excludeVars;
  }

  public Map<String, Set<String>> getReverseSubSets() {
    return reverseSubSets;
  }

  public void setReverseSubSets(Map<String, Set<String>> reverseSubSets) {
    this.reverseSubSets = reverseSubSets;
  }

  public Map<String, Set<String>> getSubSets() {
    return subSets;
  }

  public void setSubSets(Map<String, Set<String>> subSets) {
    this.subSets = subSets;
  }

  public Nfa getNfa() {
    return nfa;
  }

  public void setNfa(Nfa nfa) {
    this.nfa = nfa;
  }

  public Integer[] getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(Integer[] partitionKey) {
    this.partitionKey = partitionKey;
  }

  public boolean isAllRows() {
    return isAllRows;
  }

  public void setAllRows(boolean allRows) {
    isAllRows = allRows;
  }

  public MatchingFactory getMatchingFactory() {
    return matchingFactory;
  }

  public void setMatchingFactory(MatchingFactory matchingFactory) {
    this.matchingFactory = matchingFactory;
  }

  public boolean isLongestFirst() {
    return longestFirst;
  }

  public void setLongestFirst(boolean longestFirst) {
    this.longestFirst = longestFirst;
  }

  public Map<Integer, Object> getDynamicParams() {
    return dynamicParams;
  }

  public void setDynamicParams(Map<Integer, Object> dynamicParams) {
    this.dynamicParams = dynamicParams;
  }
}

// End MatchHelper.java
