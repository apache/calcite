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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.BaseFileTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test join reordering with unique source files to eliminate cache conflicts.
 */
@Tag("unit")
public class UniqueFileJoinTest extends BaseFileTest {

  @Test
  public void testJoinWithUniqueFiles() throws Exception {
    // Create unique temporary directory and files
    File tempDir = new File(System.getProperty("java.io.tmpdir"), "test-" + System.nanoTime());
    tempDir.mkdirs();
    String uniqueId = String.valueOf(System.nanoTime());
    
    try {
      // Create unique EMPS file with predictable table name
      File empsFile = new File(tempDir, "EMPSJOINREORDER.html");
      try (FileWriter writer = new FileWriter(empsFile)) {
        writer.write("<!--\n");
        writer.write("Licensed to the Apache Software Foundation (ASF) under one or more\n");
        writer.write("contributor license agreements.\n");
        writer.write("-->\n");
        writer.write("<html>\n");
        writer.write("  <body>\n");
        writer.write("    <table>\n");
        writer.write("      <thead>\n");
        writer.write("        <tr>\n");
        writer.write("          <th>EMPNO</th>\n");
        writer.write("          <th>NAME</th>\n");
        writer.write("          <th>DEPTNO</th>\n");
        writer.write("        </tr>\n");
        writer.write("      </thead>\n");
        writer.write("      <tbody>\n");
        writer.write("        <tr>\n");
        writer.write("          <td>100</td>\n");
        writer.write("          <td>Sales</td>\n");  // This name will match DEPTS
        writer.write("          <td>30</td>\n");
        writer.write("        </tr>\n");
        writer.write("        <tr>\n");
        writer.write("          <td>110</td>\n");
        writer.write("          <td>Marketing</td>\n");  // This name will match DEPTS
        writer.write("          <td>20</td>\n");
        writer.write("        </tr>\n");
        writer.write("      </tbody>\n");
        writer.write("    </table>\n");
        writer.write("  </body>\n");
        writer.write("</html>\n");
      }

      // Create unique DEPTS file with predictable table name
      File deptsFile = new File(tempDir, "DEPTSJOINREORDER.html");
      try (FileWriter writer = new FileWriter(deptsFile)) {
        writer.write("<!--\n");
        writer.write("Licensed to the Apache Software Foundation (ASF) under one or more\n");
        writer.write("contributor license agreements.\n");
        writer.write("-->\n");
        writer.write("<html>\n");
        writer.write("  <body>\n");
        writer.write("    <table>\n");
        writer.write("      <thead>\n");
        writer.write("        <tr>\n");
        writer.write("          <th>DEPTNO</th>\n");
        writer.write("          <th>NAME</th>\n");
        writer.write("        </tr>\n");
        writer.write("      </thead>\n");
        writer.write("      <tbody>\n");
        writer.write("        <tr>\n");
        writer.write("          <td>10</td>\n");
        writer.write("          <td>Sales</td>\n");
        writer.write("        </tr>\n");
        writer.write("        <tr>\n");
        writer.write("          <td>20</td>\n");
        writer.write("          <td>Marketing</td>\n");
        writer.write("        </tr>\n");
        writer.write("        <tr>\n");
        writer.write("          <td>30</td>\n");
        writer.write("          <td>Accounts</td>\n");
        writer.write("        </tr>\n");
        writer.write("      </tbody>\n");
        writer.write("    </table>\n");
        writer.write("  </body>\n");
        writer.write("</html>\n");
      }

      // Create a model that uses our unique files
      String model = createUniqueModel(tempDir, uniqueId);
      
      Properties info = new Properties();
      info.setProperty("model", "inline:" + model);
      info.setProperty("lex", "ORACLE");
      info.setProperty("unquotedCasing", "TO_LOWER");
      
      try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
           Statement statement = connection.createStatement()) {
        
        // Verify we can access the tables (they get the __t1 suffix from the file adapter)
        ResultSet tables = connection.getMetaData().getTables(null, "SALES", null, new String[]{"TABLE"});
        boolean foundEmps = false, foundDepts = false;
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          if ("empsjoinreorder__t1".equals(tableName)) foundEmps = true;
          if ("deptsjoinreorder__t1".equals(tableName)) foundDepts = true;
        }
        tables.close();
        
        assertTrue(foundEmps, "Should find empsjoinreorder__t1 table");
        assertTrue(foundDepts, "Should find deptsjoinreorder__t1 table");
        
        // Test the join with unique files - this should work without cache conflicts
        String sql = "select e.\"empno\", e.\"name\", d.\"deptno\" " +
                     "from \"SALES\".\"empsjoinreorder__t1\" e " +
                     "join \"SALES\".\"deptsjoinreorder__t1\" d " +
                     "on e.\"name\" = d.\"name\"";
        
        try (ResultSet rs = statement.executeQuery(sql)) {
          // Verify we get results from the join
          assertTrue(rs.next(), "Join should return at least one result");
          
          int empno = rs.getInt("empno");
          String empName = rs.getString("name");
          int deptno = rs.getInt("deptno");
          
          // Verify the join worked correctly
          assertTrue(empno == 100 || empno == 110, "Should get employee 100 or 110");
          assertTrue("Sales".equals(empName) || "Marketing".equals(empName), "Should get Sales or Marketing employee");
          assertTrue(deptno == 10 || deptno == 20, "Should get department 10 or 20");
          
          // Check if we get a second result
          if (rs.next()) {
            int empno2 = rs.getInt("empno");
            String empName2 = rs.getString("name");
            int deptno2 = rs.getInt("deptno");
            
            // Verify the second join result  
            assertTrue(empno2 == 100 || empno2 == 110, "Should get employee 100 or 110");
            assertTrue("Sales".equals(empName2) || "Marketing".equals(empName2), "Should get Sales or Marketing employee");
            assertTrue(deptno2 == 10 || deptno2 == 20, "Should get department 10 or 20");
          }
        }
      }
      
      
    } finally {
      // Cleanup - delete temporary files
      deleteDirectoryQuietly(tempDir);
    }
  }
  
  private String createUniqueModel(File tempDir, String uniqueId) {
    String engineLine = "";
    String engine = getExecutionEngine();
    if (engine != null && !engine.isEmpty()) {
      engineLine = "        \"executionEngine\": \"" + engine.toLowerCase() + "\",\n";
    }
    return "{\n" +
           "  \"version\": \"1.0\",\n" +
           "  \"defaultSchema\": \"SALES\",\n" +
           "  \"schemas\": [\n" +
           "    {\n" +
           "      \"name\": \"SALES\",\n" +
           "      \"type\": \"custom\",\n" +
           "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n" +
           "      \"operand\": {\n" +
           "        \"directory\": \"" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n" +
           "        \"baseDirectory\": \"" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n" +
           engineLine +
           "        \"flavor\": \"TRANSLATABLE\"\n" +
           "      }\n" +
           "    }\n" +
           "  ]\n" +
           "}";
  }
  
  private void deleteDirectoryQuietly(File directory) {
    try {
      if (directory.isDirectory()) {
        File[] children = directory.listFiles();
        if (children != null) {
          for (File child : children) {
            deleteDirectoryQuietly(child);
          }
        }
      }
      directory.delete();
    } catch (Exception e) {
      // Ignore cleanup failures
      System.err.println("Warning: Could not delete temp file: " + directory + " (not a test failure)");
    }
  }
}