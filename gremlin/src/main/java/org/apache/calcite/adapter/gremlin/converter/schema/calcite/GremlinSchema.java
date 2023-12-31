package org.apache.calcite.adapter.gremlin.converter.schema.calcite;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinEdgeTable;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinTableBase;
import org.apache.calcite.adapter.gremlin.converter.schema.gremlin.GremlinVertexTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class GremlinSchema extends AbstractSchema {
    private final List<GremlinVertexTable> vertices;
    private final List<GremlinEdgeTable> edges;

    @Override
    protected Map<String, Table> getTableMap() {
        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        builder.putAll(vertices.stream().collect(Collectors.toMap(GremlinTableBase::getLabel, t -> t)));
        builder.putAll(edges.stream().collect(Collectors.toMap(GremlinTableBase::getLabel, t -> t)));
        final Map<String, Table> tableMap = builder.build();
        return tableMap;
    }

    public List<GremlinVertexTable> getVertices() {
        return new ArrayList<>(vertices);
    }

    public List<GremlinEdgeTable> getEdges() {
        return new ArrayList<>(edges);
    }

    public List<GremlinTableBase> getAllTables() {
        final List<GremlinTableBase> gremlinTableBases = new ArrayList<>();
        gremlinTableBases.addAll(vertices);
        gremlinTableBases.addAll(edges);
        return gremlinTableBases;
    }
}
