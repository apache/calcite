package org.apache.calcite.adapter.gremlin.converter.schema.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public interface GremlinRel extends RelNode {
    /**
     * Calling convention for relational operations that occur in Gremlin.
     */
    Convention CONVENTION = new Convention.Impl("GREMLIN", GremlinRel.class);
}
