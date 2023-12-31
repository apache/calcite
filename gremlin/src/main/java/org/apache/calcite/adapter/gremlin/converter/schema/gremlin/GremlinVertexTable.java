package org.apache.calcite.adapter.gremlin.converter.schema.gremlin;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Getter;

@Getter
public class GremlinVertexTable extends GremlinTableBase {
    private final List<String> inEdges;
    private final List<String> outEdges;

    public GremlinVertexTable(final String label, final List<GremlinProperty> columns, final List<String> inEdges, final List<String> outEdges) {
        super(label, true, convert(label, columns, inEdges, outEdges));
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

    // String for edges because 1 vertex can be connected to many edges (may required representation like '[1, 2, 3]"
    // Long type for vertices because an edge can only be connected to one vertex (on each side).
    private static Map<String, GremlinProperty> convert(
            final String label, final List<GremlinProperty> columns,
            final List<String> inEdges, final List<String> outEdges) {
        final Map<String, GremlinProperty> columnsWithPKFK =
                columns.stream().collect(Collectors.toMap(GremlinProperty::getName, t -> t));

        // Uppercase vertex label appended with '_ID' represents an vertex, this is a string type.
        final GremlinProperty pk = new GremlinProperty(label + ID, "string");
        columnsWithPKFK.put(pk.getName(), pk);

        // Get in and out foreign keys of edge.
        inEdges.forEach(inEdgeLabel -> {
            // Uppercase edge label appended with 'IN_ID'/'OUT_ID' represents a connected edge, this is a string type.
            final GremlinProperty inFk = new GremlinProperty(inEdgeLabel + IN_ID, "string");
            columnsWithPKFK.put(inFk.getName(), inFk);
        });
        outEdges.forEach(outEdgeLabel -> {
            // Uppercase edge label appended with 'IN_ID'/'OUT_ID' represents a connected edge, this is a string type.
            final GremlinProperty inFk = new GremlinProperty(outEdgeLabel + OUT_ID, "string");
            columnsWithPKFK.put(inFk.getName(), inFk);
        });
        return columnsWithPKFK;
    }

    public boolean hasInEdge(final String label) {
        return inEdges.stream().anyMatch(e -> e.equalsIgnoreCase(label.replace(IN_ID, "")));
    }

    public boolean hasOutEdge(final String label) {
        return outEdges.stream().anyMatch(e -> e.equalsIgnoreCase(label.replace(OUT_ID, "")));
    }
}
