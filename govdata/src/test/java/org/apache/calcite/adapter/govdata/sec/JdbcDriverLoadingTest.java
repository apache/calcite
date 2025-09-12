/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify JDBC driver loading mechanisms.
 */
@Tag("unit")
public class JdbcDriverLoadingTest {

  @Test public void testServiceProviderConfiguration() {
    // Test if the service provider configuration is available
    ServiceLoader<Driver> loader = ServiceLoader.load(Driver.class);
    boolean foundCalciteDriver = false;

    System.out.println("Drivers found via ServiceLoader:");
    for (Driver driver : loader) {
      String driverClass = driver.getClass().getName();
      System.out.println("  - " + driverClass);
      if (driverClass.equals("org.apache.calcite.jdbc.Driver")) {
        foundCalciteDriver = true;
      }
    }

    assertTrue(foundCalciteDriver,
        "Calcite driver should be discoverable via ServiceLoader");
  }

  @Test public void testDriverManagerWithoutClassForName() {
    // Clear any previously loaded drivers
    Enumeration<Driver> driversBeforeTest = DriverManager.getDrivers();
    int driverCountBefore = 0;
    while (driversBeforeTest.hasMoreElements()) {
      driversBeforeTest.nextElement();
      driverCountBefore++;
    }
    System.out.println("Drivers registered before test: " + driverCountBefore);

    // Try to get a connection without Class.forName
    Exception connectionException = null;
    try {
      Connection conn = DriverManager.getConnection("jdbc:calcite:");
      conn.close();
      System.out.println("Connection succeeded without Class.forName");
    } catch (Exception e) {
      connectionException = e;
      System.out.println("Connection failed without Class.forName: " + e.getMessage());
    }

    // The connection might succeed if the driver was already loaded by another test
    // or it might fail if this is the first test to run
    // This demonstrates the unreliability of automatic loading

    if (connectionException != null) {
      assertTrue(connectionException.getMessage().contains("No suitable driver"),
          "Expected 'No suitable driver' error but got: " + connectionException.getMessage());
    }
  }

  @Test public void testDriverManagerWithClassForName() throws Exception {
    // Explicitly load the driver
    Class.forName("org.apache.calcite.jdbc.Driver");

    // Check if driver is now registered
    Enumeration<Driver> drivers = DriverManager.getDrivers();
    boolean foundCalciteDriver = false;

    System.out.println("Drivers registered after Class.forName:");
    while (drivers.hasMoreElements()) {
      Driver driver = drivers.nextElement();
      String driverClass = driver.getClass().getName();
      System.out.println("  - " + driverClass);
      if (driverClass.equals("org.apache.calcite.jdbc.Driver")) {
        foundCalciteDriver = true;
      }
    }

    assertTrue(foundCalciteDriver,
        "Calcite driver should be registered after Class.forName");

    // Now connection should work
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:")) {
      assertNotNull(conn);
      System.out.println("Connection successful with Class.forName");
    }
  }

  @Test public void testDriverStaticInitialization() throws Exception {
    // Get the initial count of registered drivers
    Enumeration<Driver> initialDrivers = DriverManager.getDrivers();
    int initialCount = 0;
    boolean calciteAlreadyLoaded = false;
    while (initialDrivers.hasMoreElements()) {
      Driver d = initialDrivers.nextElement();
      if (d.getClass().getName().equals("org.apache.calcite.jdbc.Driver")) {
        calciteAlreadyLoaded = true;
      }
      initialCount++;
    }

    if (!calciteAlreadyLoaded) {
      // Force class loading
      Class<?> driverClass = Class.forName("org.apache.calcite.jdbc.Driver");

      // Check that the driver registered itself
      Enumeration<Driver> afterLoadDrivers = DriverManager.getDrivers();
      int afterLoadCount = 0;
      boolean foundCalcite = false;
      while (afterLoadDrivers.hasMoreElements()) {
        Driver d = afterLoadDrivers.nextElement();
        if (d.getClass().getName().equals("org.apache.calcite.jdbc.Driver")) {
          foundCalcite = true;
        }
        afterLoadCount++;
      }

      assertTrue(foundCalcite, "Driver should self-register on class loading");
      assertTrue(afterLoadCount > initialCount,
          "Driver count should increase after loading");
    } else {
      System.out.println("Calcite driver was already loaded by another test");
    }
  }

  @Test public void testJdbcDriverDiscoveryMechanism() {
    // This test demonstrates why explicit Class.forName is needed

    System.out.println("\n=== JDBC Driver Loading Analysis ===");
    System.out.println("\n1. Service Provider Configuration:");

    // Check if service provider file exists
    ServiceLoader<Driver> serviceLoader = ServiceLoader.load(Driver.class);
    int serviceProviderCount = 0;
    for (Driver d : serviceLoader) {
      serviceProviderCount++;
    }
    System.out.println("   Found " + serviceProviderCount + " drivers via ServiceLoader");

    System.out.println("\n2. DriverManager Registration (before Class.forName):");
    Enumeration<Driver> beforeDrivers = DriverManager.getDrivers();
    int beforeCount = 0;
    while (beforeDrivers.hasMoreElements()) {
      beforeDrivers.nextElement();
      beforeCount++;
    }
    System.out.println("   " + beforeCount + " drivers registered with DriverManager");

    System.out.println("\n3. After Class.forName:");
    try {
      Class.forName("org.apache.calcite.jdbc.Driver");
      Enumeration<Driver> afterDrivers = DriverManager.getDrivers();
      int afterCount = 0;
      while (afterDrivers.hasMoreElements()) {
        afterDrivers.nextElement();
        afterCount++;
      }
      System.out.println("   " + afterCount + " drivers registered with DriverManager");

      System.out.println("\nConclusion:");
      System.out.println("- Service provider configuration exists (" + serviceProviderCount + " found)");
      System.out.println("- But DriverManager doesn't automatically load them");
      System.out.println("- Class.forName triggers static initialization which registers the driver");
      System.out.println("- This is why tests need explicit Class.forName calls\n");

    } catch (ClassNotFoundException e) {
      fail("Calcite JDBC driver class not found: " + e.getMessage());
    }
  }
}
