package org.apache.calcite.adapter.gremlin.converter.schema.gremlin;

import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Getter;

@Getter
public class GremlinEdgeTable extends GremlinTableBase {
    private final List<Pair<String, String>> inOutVertexPairs;

    public GremlinEdgeTable(final String label, final List<GremlinProperty> columns,
                            final List<Pair<String, String>> inOutVertexPairs) {
        super(label, false, convert(label, columns, inOutVertexPairs));
        this.inOutVertexPairs = inOutVertexPairs;
    }

    private static Map<String, GremlinProperty> convert(
            final String label, final List<GremlinProperty> columns,
            final List<Pair<String, String>> inOutTablePairs) {
        final Map<String, GremlinProperty> columnsWithPKFK =
                columns.stream().collect(Collectors.toMap(GremlinProperty::getName, t -> t));

        // Uppercase edge label appended with '_ID' represents an edge, this is a string type.
        final GremlinProperty pk = new GremlinProperty(label + GremlinTableBase.ID, "string");
        columnsWithPKFK.put(pk.getName(), pk);

        // Get in and out foreign keys of edge.
        inOutTablePairs.forEach(inOutPair -> {
            // Uppercase vertex label appended with 'IN_ID'/'OUT_ID' represents a connected vertex, this is a string type.
            final GremlinProperty inFk = new GremlinProperty(inOutPair.getKey() + GremlinTableBase.IN_ID, "string");
            final GremlinProperty outFk = new GremlinProperty(inOutPair.getValue() + GremlinTableBase.OUT_ID, "string");
            columnsWithPKFK.put(inFk.getName(), inFk);
            columnsWithPKFK.put(outFk.getName(), inFk);
        });
        return columnsWithPKFK;
    }

    public boolean isEdgeBetween(final String in, final String out) {
        for (final Pair<String, String> inOutPair : inOutVertexPairs) {
            if (inOutPair.getKey().equalsIgnoreCase(in + GremlinTableBase.IN_ID)
                    && inOutPair.getValue().equalsIgnoreCase(out + GremlinTableBase.OUT_ID)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasInVertex(final String inVertexLabel) {
        final String label = inVertexLabel.replace(IN_ID, "");
        for (final Pair<String, String> pair : inOutVertexPairs) {
            if (pair.getKey().equalsIgnoreCase(label)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasOutVertex(final String outVertexLabel) {
        final String label = outVertexLabel.replace(IN_ID, "");
        for (final Pair<String, String> pair : inOutVertexPairs) {
            if (pair.getValue().equalsIgnoreCase(label)) {
                return true;
            }
        }
        return false;
    }
}
