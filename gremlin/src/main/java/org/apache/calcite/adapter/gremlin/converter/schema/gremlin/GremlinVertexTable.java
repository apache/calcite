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
package org.apache.calcite.adapter.gremlin.converter.schema.gremlin;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GremlinVertexTable extends GremlinTableBase {
    private final List<String> inEdges;
    private final List<String> outEdges;

    public GremlinVertexTable(final String label, final List<GremlinProperty> columns, final List<String> inEdges, final List<String> outEdges) {
        super(label, true, convert(label, columns, inEdges, outEdges));
        this.inEdges = inEdges;
        this.outEdges = outEdges;
    }

  public List<String> getInEdges() {
    return inEdges;
  }

  public List<String> getOutEdges() {
    return outEdges;
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
