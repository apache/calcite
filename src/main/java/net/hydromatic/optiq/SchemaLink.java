package net.hydromatic.optiq;

/**
 * A schema included in another schema with a given name.
 *
 * @author jhyde
 */
public final class SchemaLink implements SchemaObject {
    public final String name;
    public final Schema schema;

    public SchemaLink(String name, Schema schema) {
        this.name = name;
        this.schema = schema;
    }

    public String getName() {
        return name;
    }
}

// End SchemaLink.java
