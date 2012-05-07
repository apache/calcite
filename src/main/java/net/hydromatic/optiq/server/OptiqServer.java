package net.hydromatic.optiq.server;

/**
 * Server.
 *
 * <p>Represents shared state among connections, and will have monitoring and
 * management facilities.
 */
public interface OptiqServer {
    void removeStatement(OptiqServerStatement optiqServerStatement);

    void addStatement(OptiqServerStatement optiqServerStatement);
}

// End OptiqServer.java
