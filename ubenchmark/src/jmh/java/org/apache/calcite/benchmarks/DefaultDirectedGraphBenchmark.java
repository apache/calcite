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
 * Benchmarks for {@link org.apache.calcite.util.graph.DefaultDirectedGraph}.
 */
public class DefaultDirectedGraphBenchmark {

  /** Node in the graph. */
  private static class Node {
    final int id;

    private Node(int id) {
      this.id = id;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof Node
          && ((Node) o).id == id;
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

    List<Node> tenNodes;
    List<Node> twentyNodes;
    List<Node> thirtyNodes;
    List<Node> fortyNodes;
    List<Node> fiftyNodes;
    List<Node> sixtyNodes;
    List<Node> seventyNodes;
    List<Node> eightyNodes;
    List<Node> ninetyNodes;

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
        for (Node node : prevLayerNodes) {
          Node leftChild = new Node(curId++);
          Node rightChild = new Node(curId++);

          curLayerNodes.add(leftChild);
          curLayerNodes.add(rightChild);

          nodes.add(leftChild);
          nodes.add(rightChild);

          graph.addVertex(leftChild);
          graph.addVertex(rightChild);

          graph.addEdge(node, leftChild);
          graph.addEdge(node, rightChild);
        }
        prevLayerNodes = curLayerNodes;
      }

      int tenNodeCount = (int) (nodes.size() * 0.1);
      int twentyNodeCount = (int) (nodes.size() * 0.2);
      int thirtyNodeCount = (int) (nodes.size() * 0.3);
      int fortyNodeCount = (int) (nodes.size() * 0.4);
      int fiftyNodeCount = (int) (nodes.size() * 0.5);
      int sixtyNodeCount = (int) (nodes.size() * 0.6);
      int seventyNodeCount = (int) (nodes.size() * 0.7);
      int eightyNodeCount = (int) (nodes.size() * 0.8);
      int ninetyNodeCount = (int) (nodes.size() * 0.9);
      tenNodes = nodes.subList(0, tenNodeCount);
      twentyNodes = nodes.subList(0, twentyNodeCount);
      thirtyNodes = nodes.subList(0, thirtyNodeCount);
      fortyNodes = nodes.subList(0, fortyNodeCount);
      fiftyNodes = nodes.subList(0, fiftyNodeCount);
      sixtyNodes = nodes.subList(0, sixtyNodeCount);
      seventyNodes = nodes.subList(0, seventyNodeCount);
      eightyNodes = nodes.subList(0, eightyNodeCount);
      ninetyNodes = nodes.subList(0, ninetyNodeCount);
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
  public void removeAllVertices10Benchmark(GraphState state) {
    state.graph.removeAllVertices(state.tenNodes);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void removeAllVertices20Benchmark(GraphState state) {
    state.graph.removeAllVertices(state.twentyNodes);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void removeAllVertices30Benchmark(GraphState state) {
    state.graph.removeAllVertices(state.thirtyNodes);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void removeAllVertices40Benchmark(GraphState state) {
    state.graph.removeAllVertices(state.fortyNodes);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void removeAllVertices50Benchmark(GraphState state) {
    state.graph.removeAllVertices(state.fiftyNodes);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void removeAllVertices60Benchmark(GraphState state) {
    state.graph.removeAllVertices(state.sixtyNodes);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void removeAllVertices70Benchmark(GraphState state) {
    state.graph.removeAllVertices(state.seventyNodes);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void removeAllVertices80Benchmark(GraphState state) {
    state.graph.removeAllVertices(state.eightyNodes);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void removeAllVertices90Benchmark(GraphState state) {
    state.graph.removeAllVertices(state.ninetyNodes);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void removeAllVertices100Benchmark(GraphState state) {
    state.graph.removeAllVertices(state.nodes);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(DefaultDirectedGraphBenchmark.class.getName())
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
