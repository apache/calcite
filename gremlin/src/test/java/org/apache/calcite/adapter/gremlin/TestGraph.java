package org.apache.calcite.adapter.gremlin;

import org.apache.tinkerpop.gremlin.structure.Graph;

public interface TestGraph {
    void populate(Graph graph);
}
