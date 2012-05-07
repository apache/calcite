package org.eigenbase.relopt;

import org.eigenbase.reltype.RelDataType;

import java.util.List;

/**
 * Node in a planner.
 */
public interface RelOptNode {
    /**
     * Returns the ID of this relational expression, unique among all relational
     * expressions created since the server was started.
     *
     * @return Unique ID
     */
    int getId();

    /**
     * Returns a string which concisely describes the definition of this
     * relational expression. Two relational expressions are equivalent if and
     * only if their digests are the same.
     *
     * <p>The digest does not contain the relational expression's identity --
     * that would prevent similar relational expressions from ever comparing
     * equal -- but does include the identity of children (on the assumption
     * that children have already been normalized).
     *
     * <p>If you want a descriptive string which contains the identity, call
     * {@link Object#toString()}, which always returns "rel#{id}:{digest}".
     */
    String getDigest();

    /**
     * Retrieves this RelNode's traits. Note that although the RelTraitSet
     * returned is modifiable, it <b>must not</b> be modified during
     * optimization. It is legal to modify the traits of a RelNode before or
     * after optimization, although doing so could render a tree of RelNodes
     * unimplementable. If a RelNode's traits need to be modified during
     * optimization, clone the RelNode and change the clone's traits.
     *
     * @return this RelNode's trait set
     */
    RelTraitSet getTraitSet();

    // TODO: We don't want to require that nodes have very detailed row type. It
    // may not even be known at planning time.
    RelDataType getRowType();

    /**
     * Returns a string which describes the relational expression and, unlike
     * {@link #getDigest()}, also includes the identity. Typically returns
     * "rel#{id}:{digest}".
     */
    String getDescription();

    List<? extends RelOptNode> getInputs();

    /**
     * Returns the cluster this relational expression belongs to.
     *
     * @return cluster
     */
    RelOptCluster getCluster();
}

// End RelNode.java
