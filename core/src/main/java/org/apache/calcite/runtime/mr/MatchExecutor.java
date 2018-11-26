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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

/**
 * Executor for MATCH_RECOGNIZE.
 */
public class MatchExecutor {

  private MatchExecutor() {}

  public static List<Object[]> executeMatch(List<Object[]> inputData,
      MatchHelper mr) {
    List<Tuple> resultOutput = new ArrayList<>();
    final Set<String> alphaEval = new HashSet<>();
    final Set<String> alphaMatch = new HashSet<>();
    final Map<String, Queue> inputsPerGroup = new HashMap<>();

    int mrCounter = 0;
    boolean isEof = false;
    int maxTID = 0;
    int tupleCounter = 0;

    Map<Integer, Tuple> tuples = new HashMap<>();
    Map<String, Map<Integer, Integer>> tuplesPerPart = new HashMap<>();
    Map<String, Object[]> pKeys = new HashMap<>();
    Map<String, Integer> startTIDs = new HashMap<>();

    do {
      int prevSize = tuples.size();
      Set<String> keySets = parseTuples(inputData, mr, tuples,
          tuplesPerPart, pKeys, startTIDs, tupleCounter);
      maxTID = Collections.max(tuples.keySet());
      if (keySets.size() == 0) {
        break;
      }
      tupleCounter += tuples.size() - prevSize;
      isEof = tuples.size() - prevSize < 0;

      // for each partition, do the matching
      int smallestID = tupleCounter;
      for (String pKey : keySets) {
        final BitSet matchStartsAt = new BitSet();
        Queue<Matching> input;
        if (inputsPerGroup.containsKey(pKey)) {
          input = inputsPerGroup.get(pKey);
        } else {
          input = new PriorityQueue<>();
          inputsPerGroup.put(pKey, input);
        }

        Map<Integer, Integer> tupleList = tuplesPerPart.get(pKey);

        if (input.isEmpty()) {
          int minID = tupleList.size() == 0 ? 0 : Collections.min(tupleList.keySet());
          input.add(mr.getMatchingFactory().create(minID));
        }

        Queue<Matching> tmpQueue = new PriorityQueue<>();
        int nextTupleIdx;
        while (!input.isEmpty()) {
          Matching top = input.poll();
          int status = top.getStatus();
          nextTupleIdx = top.getNextTid();

          if (!tupleList.containsKey(nextTupleIdx)) {
            tmpQueue.add(top);
            continue;
          }

          alphaEval.clear();
          alphaMatch.clear();

          List<NfaState> nfaStates = mr.getNfa().getStateNoFinals(status);
          int matchCounter = 0;
          for (NfaState tt : nfaStates) {
            String alpha = tt.getAlpha();
            Matching activeTemp = null;
            if (alphaMatch.contains(alpha)) {
              activeTemp = top.copy();
            } else if (!alphaEval.contains(alpha) && !alphaMatch.contains(alpha)) {
              alphaEval.add(alpha);
              if (mr.defines(tuples, tupleList, top, alpha)) {
                alphaMatch.add(alpha);
                activeTemp = top.copy();
                matchCounter++;
              }
            }
            //if no match is found
            if (activeTemp != null) {
              //update aggregations
              mr.updateAggregates(tuples, tupleList, activeTemp, alpha);
              activeTemp.setStatus(tt.getTo());
              activeTemp.setNextTid(nextTupleIdx + 1);

              //add tuple id to the matching
              activeTemp.addTuple(nextTupleIdx, alpha);

              if (mr.getNfa().isFinal(tt.getTo())) {
                if (mr.isLongestFirst()
                    && mr.getNfa().getStateNoFinals(tt.getTo()).size() > 0) {
                  Matching copy = activeTemp.copy();
                  if (!mr.getNfa().isStrictEnds()) {
                    copy.setBak(activeTemp);
                  }
                  input.add(copy);
                } else {
                  if (!mr.getNfa().isStrictEnds()
                      || mr.getNfa().isStrictEnds()
                      && isEof && activeTemp.getNextTid() > maxTID) {
                    mrCounter = mr.measures(resultOutput, tuples, tuplesPerPart.get(pKey),
                        pKeys.get(pKey), mrCounter, activeTemp);
                  } else {
                    input.add(activeTemp.copy());
                  }
                }
                if (!mr.getNfa().isStrictStarts()) {
                  mr.addMatching(input, activeTemp, matchStartsAt, mr.getMatchingFactory());
                }
              } else {
                input.add(activeTemp);
              }
            }
          }

          if (matchCounter == 0) {
            // no valid transition found
            if (top.getBak() != null) {
              if (!mr.getNfa().isStrictEnds()
                  || mr.getNfa().isStrictEnds()
                  && isEof && top.getNextTid() > maxTID) {
                mr.measures(resultOutput, tuples,
                    tuplesPerPart.get(pKey), pKeys.get(pKey), mrCounter,
                    top.getBak());
              }
            } else if (!mr.getNfa().isStrictStarts()) {
              int reStart = top.getStartTid() + 1;
              if (!matchStartsAt.get(reStart) && !mr.getNfa().isStrictStarts()) {
                matchStartsAt.set(reStart);
                input.add(mr.getMatchingFactory().create(reStart));
              }
            }
          }
        }
        if (tmpQueue.size() > 0) {
          int peekID = tmpQueue.peek().getStartTid();
          if (tupleList.containsKey(peekID)) {
            smallestID = Math.min(smallestID, tupleList.get(peekID));
          }
          inputsPerGroup.put(pKey, tmpQueue);
        }
      }

      tuples = MatchUtils.cleanUp(tuples, tuplesPerPart, smallestID);
    } while (!isEof);

    for (Map.Entry<String, Queue> entry : inputsPerGroup.entrySet()) {
      String pKey = entry.getKey();
      Queue queue = entry.getValue();

      while (!queue.isEmpty()) {
        Matching top = (Matching) queue.poll();
        int status = top.getStatus();
        if (!mr.getNfa().isStrictEnds()
            || mr.getNfa().isStrictEnds()
            && isEof && top.getNextTid() > maxTID) {
          if (mr.getNfa().isFinal(status)) {
            mrCounter = mr.measures(resultOutput, tuples,
                tuplesPerPart.get(pKey), pKeys.get(pKey), mrCounter, top);
          } else if (top.getBak() != null) {
            mrCounter = mr.measures(resultOutput, tuples,
                tuplesPerPart.get(pKey), pKeys.get(pKey), mrCounter, top.getBak());
          }
        }
      }
    }

    return mr.parserResult(resultOutput);
  }

  private static Set<String> parseTuples(List<Object[]> dataSet,
      MatchHelper mr,
      Map<Integer, Tuple> tuples,
      Map<String, Map<Integer, Integer>> tuplesPerPart,
      Map<String, Object[]> pKeys,
      Map<String, Integer> startTIDs,
      int offset) {
    List<Object[]> results = new ArrayList<>();
    Map<String, List<Integer>> newTuplesPerPart = new HashMap<>();
    if (dataSet != null) {
      if (offset >= dataSet.size()) {
        return new HashSet<>();
      } else {
        results = dataSet.subList(offset, dataSet.size());
      }
    }

    String pKey = null;
    List<Integer> tupleList;
    int currentTID = offset;
    for (Object[] obj : results) {
      Tuple tuple = new Tuple(currentTID, obj);
      tuples.put(currentTID++, tuple);

      if (mr.getPartitionKey() != null) {
        Object[] keys = new Object[mr.getPartitionKey().length];
        for (int i = 0; i < mr.getPartitionKey().length; i++) {
          keys[i] = tuple.getData(mr.getPartitionKey()[i]);
        }
        pKey = MatchUtils.objectsToS(keys);
        if (!pKeys.containsKey(pKey)) {
          pKeys.put(pKey, keys);
        }
      }

      if (newTuplesPerPart.containsKey(pKey)) {
        tupleList = newTuplesPerPart.get(pKey);
      } else {
        tupleList = new ArrayList<>();
        newTuplesPerPart.put(pKey, tupleList);
      }
      tupleList.add(tuple.getTid());
    }

    for (Map.Entry<String, List<Integer>> entry : newTuplesPerPart.entrySet()) {
      pKey = entry.getKey();
      List<Integer> list = entry.getValue();
      Map<Integer, Integer> tupleMap;
      int startID;
      if (tuplesPerPart.containsKey(pKey)) {
        tupleMap = tuplesPerPart.get(pKey);
        startID = startTIDs.get(pKey);
      } else {
        tupleMap = new HashMap<>();
        tuplesPerPart.put(pKey, tupleMap);
        startID = 0;
      }

      for (int i = 0; i < list.size(); i++) {
        tupleMap.put(startID + i, list.get(i));
      }
      startTIDs.put(pKey, startID + list.size());
    }
    return newTuplesPerPart.keySet();
  }
}

// End MatchExecutor.java
