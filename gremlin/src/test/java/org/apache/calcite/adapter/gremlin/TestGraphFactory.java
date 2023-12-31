package org.apache.calcite.adapter.gremlin;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class TestGraphFactory {
    private static final TestGraph SPACE_GRAPH = new  SpaceTestGraph();
    private static final  TestGraph DATA_TYPE_GRAPH = new  DataTypeGraph();

    public static Graph createGraph(final GremlinSqlBaseTest.DataSet dataSet) {
        final Graph graph = TinkerGraph.open();
        switch (dataSet) {
            case SPACE:
                SPACE_GRAPH.populate(graph);
                break;
            case DATA_TYPES:
                DATA_TYPE_GRAPH.populate(graph);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported graph " + dataSet.name());
        }
        return graph;
    }
}
