package net.hydromatic.optiq.jdbc;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Factory for JDBC objects.
 *
 * <p>There is an implementation for each supported JDBC version.</p>
 */
interface Factory {
    OptiqConnectionImpl newConnection(
        UnregisteredDriver driver,
        Factory factory,
        String url,
        Properties info);

    OptiqStatement newStatement(OptiqConnectionImpl connection);

    OptiqPreparedStatement newPreparedStatement(
        OptiqConnectionImpl connection, String sql) throws SQLException;

    /**
     * Creates a result set. You will then need to call
     * {@link net.hydromatic.optiq.jdbc.OptiqResultSet#execute()} on it.
     *
     * @param optiqStatement Statement
     * @param prepareResult Result
     * @return Result set
     */
    OptiqResultSet newResultSet(
        OptiqStatement optiqStatement,
        OptiqPrepare.PrepareResult prepareResult);
}

// End Factory.java
