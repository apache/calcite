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
package org.apache.calcite.benchmarks;

import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;

import com.google.common.collect.Lists;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks for {@link org.apache.calcite.util.graph.DefaultDirectedGraph}
 */
public class DefaultDirectedGraphBenchmark {

  private static class Node {
    final int id;

    private Node(int id) {
      this.id = id;
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Node node = (Node) o;
      return id == node.id;
    }

    @Override public int hashCode() {
      return Objects.hash(id);
    }
  }

  /**
   * State object for the benchmarks.
   */
  @State(Scope.Benchmark)
  public static class GraphState {

    static final int NUM_LAYERS = 8;

    DirectedGraph<Node, DefaultEdge> graph;

    List<Node> nodes;

    List<Node> nodesToRemove;

    @Setup(Level.Invocation)
    public void setUp() {
      nodes = new ArrayList<>();

      // create a binary tree
      graph = DefaultDirectedGraph.create();

      // create nodes
      int curId = 1;
      Node root = new Node(curId++);
      nodes.add(root);
      graph.addVertex(root);
      List<Node> prevLayerNodes = Lists.newArrayList(root);

      for (int i = 1; i < NUM_LAYERS; i++) {
        List<Node> curLayerNodes = new ArrayList<>();
        for (int j = 0; j < prevLayerNodes.size(); j++) {
          Node curNode = prevLayerNodes.get(j);
          Node leftChild = new Node(curId++);
          Node rightChild = new Node(curId++);

          curLayerNodes.add(leftChild);
          curLayerNodes.add(rightChild);

          nodes.add(leftChild);
          nodes.add(rightChild);

          graph.addVertex(leftChild);
          graph.addVertex(rightChild);

          graph.addEdge(curNode, leftChild);
          graph.addEdge(curNode, rightChild);
        }
        prevLayerNodes = curLayerNodes;
      }
      nodesToRemove = Lists.newArrayList(nodes.get(0), nodes.get(3), nodes.get(7));
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int getInwardEdgesBenchmark(GraphState state) {
    int sum = 0;
    int curId = 1;
    for (int i = 0; i < GraphState.NUM_LAYERS; i++) {
      // get the first node in each layer
      Node curNode = state.nodes.get(curId - 1);
      sum += state.graph.getInwardEdges(curNode).size();
      curId *= 2;
    }
    return sum;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int getOutwardEdgesBenchmark(GraphState state) {
    int sum = 0;
    int curId = 1;
    for (int i = 0; i < GraphState.NUM_LAYERS; i++) {
      // get the first node in each layer
      Node curNode = state.nodes.get(curId - 1);
      sum += state.graph.getOutwardEdges(curNode).size();
      curId *= 2;
    }
    return sum;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public boolean addVertexBenchmark(GraphState state) {
    return state.graph.addVertex(new Node(100));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public DefaultEdge addEdgeBenchmark(GraphState state) {
    return state.graph.addEdge(state.nodes.get(0), state.nodes.get(5));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public DefaultEdge getEdgeBenchmark(GraphState state) {
    return state.graph.getEdge(state.nodes.get(0), state.nodes.get(1));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public boolean removeEdgeBenchmark(GraphState state) {
    return state.graph.removeEdge(state.nodes.get(0), state.nodes.get(1));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void removeAllVerticesBenchmark(GraphState state) {
    state.graph.removeAllVertices(state.nodesToRemove);
  }


  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(DefaultDirectedGraphBenchmark.class.getName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
