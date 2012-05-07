package net.hydromatic.optiq;

/**
 * Schema that can be modified.
 */
public interface MutableSchema extends Schema {
    void add(SchemaObject schemaObject);
    void add(String name, Schema schema);
}

// End MutableSchema.java
