package org.apache.calcite.adapter.gremlin.converter.schema.gremlin;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class GremlinProperty {
    private final String name;
    private final String type;
}
