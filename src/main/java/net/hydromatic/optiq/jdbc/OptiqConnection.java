package net.hydromatic.optiq.jdbc;

import net.hydromatic.optiq.MutableSchema;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.sql.Connection;

/**
 * Extension to OPTIQ's implementation of
 * {@link java.sql.Connection JDBC connection} allows schemas to be defined
 * dynamically.
 *
 * <p>You can start off with an empty connection (no schemas), define one
 * or two schemas, and start querying them.</p>
 */
public interface OptiqConnection extends Connection {
    /**
     * Returns the root schema.
     *
     * <p>You can define objects (such as relations) in this schema, and
     * also nested schemas.</p>
     *
     * @return Root schema
     */
    MutableSchema getRootSchema();

    RelDataTypeFactory getTypeFactory();
}

// End OptiqConnection.java
