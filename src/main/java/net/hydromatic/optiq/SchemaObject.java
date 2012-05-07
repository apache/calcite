package net.hydromatic.optiq;

/**
 * A member of a {@link Schema}.
 *
 * <p>May be a {@link Function} or an {@link Overload}.</p>
 */
public interface SchemaObject {
    /**
     * The name of this schema object.
     */
    String getName();
}

// End SchemaObject.java
