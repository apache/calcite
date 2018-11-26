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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Matching object for compare
 */
public class Matching implements Comparable<Matching> {

  private int status;
  /** Start tuple ID */
  private int startTid;
  /** Next tuple ID */
  private int nextTid;
  private Matching backup;

  private final List<Integer> tuples = new ArrayList<>();
  private final Map<String, Set<String>> reverseSubSets = new HashMap<>();
  private final Map<String, List<Integer>> classifier = new HashMap<>();
  private final Map<String, List<Object>> aggrs = new HashMap<>();

  public Matching(int sta, int tid) {
    status = sta;
    startTid = tid;
    nextTid = tid;
  }

  public Matching(int sta, int tid, List<String> funs, Map<String,
      Set<String>> sets) {
    this(sta, tid);
    if (funs != null) {
      for (String fun : funs) {
        aggrs.put(fun, new ArrayList<>());
      }
    }
    if (sets != null) {
      for (Map.Entry<String, Set<String>> set : sets.entrySet()) {
        reverseSubSets.put(set.getKey(), new HashSet<>(set.getValue()));
      }
    }
  }

  public int getStatus() {
    return status;
  }

  public void setStatus(int status) {
    this.status = status;
  }

  public int getStartTid() {
    return startTid;
  }

  public void setStartTid(int startTid) {
    this.startTid = startTid;
  }

  public int getNextTid() {
    return nextTid;
  }

  public void setNextTid(int nextTid) {
    this.nextTid = nextTid;
  }

  public Matching getBak() {
    return backup;
  }

  public void setBak(Matching backMatching) {
    backup = backMatching;
  }

  public List<Integer> getTuples() {
    return tuples;
  }

  public void addTuple(int tupleId, String alpha) {
    tuples.add(tupleId);

    List<Integer> list;
    if (classifier.containsKey(alpha)) {
      list = classifier.get(alpha);
    } else {
      list = new ArrayList<>();
      classifier.put(alpha, list);
    }
    list.add(tupleId);

    // check if it is used in any subset
    if (reverseSubSets.containsKey(alpha)) {
      for (String token : reverseSubSets.get(alpha)) {
        if (classifier.containsKey(token)) {
          list = classifier.get(token);
        } else {
          list = new ArrayList<>();
          classifier.put(token, list);
        }
        list.add(tupleId);
      }
    }
  }

  public List<Integer> getClassifier(String alphaID) {
    if (classifier.containsKey(alphaID)) {
      return classifier.get(alphaID);
    }
    return null;
  }

  public void setClassifier(String alphaId, List<Integer> list) {
    classifier.put(alphaId, list);
  }

  public Map<String, List<Integer>> getClassifier() {
    return classifier;
  }

  public List<Object> getAggrResult(String aggrFun) {
    return aggrs.get(aggrFun);
  }

  public Matching copy() {
    Matching copy = new Matching(status, startTid);
    copy.nextTid = nextTid;
    copy.tuples.addAll(new ArrayList<>(getTuples()));
    for (Map.Entry<String, List<Integer>> et : classifier.entrySet()) {
      copy.classifier.put(et.getKey(), new ArrayList<>(et.getValue()));
    }
    for (Map.Entry<String, List<Object>> et : aggrs.entrySet()) {
      copy.aggrs.put(et.getKey(), new ArrayList<>(et.getValue()));
    }
    for (Map.Entry<String, Set<String>> et : reverseSubSets.entrySet()) {
      copy.reverseSubSets.put(et.getKey(), new HashSet<>(et.getValue()));
    }
    return copy;
  }

  // matching with smaller start id and next tuple id will be sorted first
  public int compareTo(@Nonnull Matching matching) {
    if (getStartTid() != matching.getStartTid()) {
      return getStartTid() - matching.getStartTid();
    }
    if (getNextTid() != matching.getNextTid()) {
      return getNextTid() - matching.getNextTid();
    }
    return 0;
  }
}

// End Matching.java
