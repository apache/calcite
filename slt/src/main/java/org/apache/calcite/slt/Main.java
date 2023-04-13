/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 * SPDX-License-Identifier: Apache-2.0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 *
 */

package org.apache.calcite.slt;

import com.beust.jcommander.ParameterException;

import org.apache.calcite.slt.executors.SqlSLTTestExecutor;
import org.apache.calcite.sql.parser.SqlParseException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Execute all SqlLogicTest tests.
 */
public class Main {
  static final String SLT_GIT = "https://github.com/gregrahn/sqllogictest/archive/refs/heads/master.zip";

  static class TestLoader extends SimpleFileVisitor<Path> {
    int errors = 0;
    final TestStatistics statistics;
    public final ExecutionOptions options;

    /**
     * Creates a new class that reads tests from a directory tree and executes them.
     */
    TestLoader(ExecutionOptions options) {
      this.statistics = new TestStatistics(options.stopAtFirstError);
      this.options = options;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      SqlSLTTestExecutor executor;
      try {
        executor = this.options.getExecutor();
      } catch (IOException | SQLException e) {
        // Can't add exceptions to the overridden method visitFile
        throw new RuntimeException(e);
      }
      String extension = Utilities.getFileExtension(file.toString());
      if (attrs.isRegularFile() && extension != null && extension.equals("test")) {
        SLTTestFile test = null;
        try {
          System.out.println("Running " + file);
          test = new SLTTestFile(file.toString());
          test.parse();
        } catch (Exception ex) {
          System.err.println("Error while executing test " + file + ": " + ex.getMessage());
          this.errors++;
        }
        if (test != null) {
          try {
            TestStatistics stats = executor.execute(test, options);
            this.statistics.add(stats);
          } catch (SqlParseException | IOException |
                   SQLException | NoSuchAlgorithmException ex) {
            // Can't add exceptions to the overridden method visitFile
            throw new IllegalArgumentException(ex);
          }
        }
      }
      return FileVisitResult.CONTINUE;
    }
  }

  static void abort(ExecutionOptions options, @Nullable String message) {
    if (message != null)
      System.err.println(message);
    options.usage();
    System.exit(1);
  }

  @Nullable
  static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
    String name = zipEntry.getName();
    name = name.replace("sqllogictest-master/", "");
    if (name.isEmpty())
      return null;
    File destFile = new File(destinationDir, name);
    String destDirPath = destinationDir.getCanonicalPath();
    String destFilePath = destFile.getCanonicalPath();
    if (!destFilePath.startsWith(destDirPath + File.separator)) {
      throw new IOException("Entry is outside of the target dir: " + name);
    }
    return destFile;
  }

  public static void install(File directory) throws IOException {
    File zip = File.createTempFile("out", ".zip", new File("."));
    System.out.println("Downloading SLT from " + SLT_GIT + " into " + zip.getAbsolutePath());
    zip.deleteOnExit();
    InputStream in = new URL(SLT_GIT).openStream();
    Files.copy(in, zip.toPath(), StandardCopyOption.REPLACE_EXISTING);

    System.out.println("Unzipping data");
    ZipInputStream zis = new ZipInputStream(Files.newInputStream(zip.toPath()));
    ZipEntry zipEntry = zis.getNextEntry();
    while (zipEntry != null) {
      File newFile = newFile(directory, zipEntry);
      if (newFile != null) {
        System.out.println("Creating " + newFile.getPath());
        if (zipEntry.isDirectory()) {
          if (!newFile.isDirectory() && !newFile.mkdirs()) {
            throw new IOException("Failed to create directory " + newFile);
          }
        } else {
          File parent = newFile.getParentFile();
          if (!parent.isDirectory() && !parent.mkdirs()) {
            throw new IOException("Failed to create directory " + parent);
          }

          FileOutputStream fos = new FileOutputStream(newFile);
          int len;
          byte[] buffer = new byte[1024];
          while ((len = zis.read(buffer)) > 0) {
            fos.write(buffer, 0, len);
          }
          fos.close();
        }
      }
      zipEntry = zis.getNextEntry();
    }
    zis.closeEntry();
    zis.close();
  }

  @SuppressWarnings("java:S4792") // Log configuration is safe
  public static void main(String[] argv) throws IOException {
    Logger rootLogger = LogManager.getLogManager().getLogger("");
    rootLogger.setLevel(Level.WARNING);
    for (Handler h : rootLogger.getHandlers()) {
      h.setLevel(Level.INFO);
    }

    ExecutionOptions options = new ExecutionOptions();
    try {
      options.parse(argv);
      System.out.println(options);
    } catch (ParameterException ex) {
      abort(options, null);
    }
    if (options.help)
      abort(options, null);
    if (options.sltDirectory == null)
      abort(options, "Please specify the directory with the SqlLogicTest suite using the -d flag");

    File dir = new File(options.sltDirectory);
    if (dir.exists()) {
      if (!dir.isDirectory())
        abort(options, options.sltDirectory + " is not a directory");
      if (options.install)
        System.err.println("Directory " + options.sltDirectory + " exists; skipping download");
    } else {
      if (options.install) {
        install(dir);
      } else {
        abort(options, options.sltDirectory + " does not exist and no installation was specified");
      }
    }

    TestLoader loader = new TestLoader(options);
    for (String file : options.getDirectories()) {
      Path path = Paths.get(options.sltDirectory + "/test/" + file);
      Files.walkFileTree(path, loader);
    }
    System.out.println("Files that could not be not parsed: " + loader.errors);
    System.out.println(loader.statistics);
  }
}
