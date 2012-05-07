package net.hydromatic.optiq.jdbc;

/**
 * OPTIQ JDBC driver.
 */
public class Driver extends UnregisteredDriver {
    static {
        new Driver().register();
    }
}

// End Driver.java
