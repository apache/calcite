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
package org.apache.calcite.adapter.gremlin.driver;

import org.apache.tinkerpop.gremlin.structure.io.Storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigureSupport {

    public static final String TEST_DATA_RELATIVE_DIR = "test-case-data";

    protected ConfigureSupport() {
    }

    /**
     * Creates a {@link File} reference that points to a directory relative to the supplied class in the
     * {@code /target} directory. Each {@code childPath} passed introduces a new sub-directory and all are placed
     * below the {@link #TEST_DATA_RELATIVE_DIR}.  For example, calling this method with "a", "b", and "c" as the
     * {@code childPath} arguments would yield a relative directory like: {@code test-case-data/clazz/a/b/c}. It is
     * a good idea to use the test class for the {@code clazz} argument so that it's easy to find the data if
     * necessary after test execution.
     * <p>
     * Avoid using makeTestDataPath(...).getAbsolutePath() and makeTestDataPath(...).toString() that produces
     * platform-dependent paths, that are incompatible with regular expressions and escape characters.
     * Instead use {@link Storage#toPath(File)}
     */
    public static File makeTestDataPath(final Class<?> clazz, final String... childPath) {
        final File root = getRootOfBuildDirectory(clazz);
        final List<String> cleanedPaths = Stream.of(childPath).map(ConfigureSupport::cleanPathSegment).collect(Collectors.toList());

        // use the class name in the directory structure
        cleanedPaths.add(0, cleanPathSegment(clazz.getSimpleName()));

        final File f = new File(new File(root, TEST_DATA_RELATIVE_DIR), String.join(Storage.FILE_SEPARATOR, cleanedPaths));
        if (!f.exists())
            f.mkdirs();
        return f;
    }

    /**
     * Creates a {@link File} reference that .  For example, calling this method with "a", "b", and "c" as the
     * {@code childPath} arguments would yield a relative directory like: {@code test-case-data/clazz/a/b/c}. It is
     * a good idea to use the test class for the {@code clazz} argument so that it's easy to find the data if
     * necessary after test execution.
     *
     * @return UNIX-formatted path to a directory in the underlying {@link Storage}. The directory is relative to the
     * supplied class in the {@code /target} directory. Each {@code childPath} passed introduces a new sub-directory
     * and all are placed below the {@link #TEST_DATA_RELATIVE_DIR}
     */
    public static String makeTestDataDirectory(final Class<?> clazz, final String... childPath) {
        return Storage.toPath(makeTestDataPath(clazz, childPath));
    }

    /**
     * @param clazz
     * @param fileName
     * @return UNIX-formatted path to a fileName in the underlying {@link Storage}. The file is relative to the
     * supplied class in the {@code /target} directory.
     */
    public static String makeTestDataFile(final Class<?> clazz, final String fileName) {
        return Storage.toPath(new File(makeTestDataPath(clazz), fileName));
    }

    /**
     * @param clazz
     * @param subdir
     * @param fileName
     * @return UNIX-formatted path to a subdir/fileName in the underlying {@link Storage}. The file is relative to the
     * supplied class in the {@code /target} directory.
     */
    public static String makeTestDataFile(final Class<?> clazz, final String subdir, final String fileName) {
        return Storage.toPath(new File(makeTestDataPath(clazz, subdir), fileName));
    }


    /**
     * Gets and/or creates the root of the test data directory.  This  method is here as a convenience and should not
     * be used to store test data.  Use {@link #makeTestDataPath(Class, String...)} instead.
     */
    public static File getRootOfBuildDirectory(final Class<?> clazz) {
        final File root;

        // build.dir gets sets during runs of tests with maven via the surefire configuration in the pom.xml
        // if that is not set as an environment variable, then the path is computed based on the location of the
        // requested class.  the computed version at least as far as intellij is concerned comes drops it into
        // /target/test-classes.  the build.dir had to be added because maven doesn't seem to like a computed path
        // as it likes to find that path in the .m2 directory and other weird places......
        final String buildDirectory = System.getProperty("build.dir");

        if (null == buildDirectory) {
            final String clsUri = clazz.getName().replace(".", "/") + ".class";
            final URL url = clazz.getClassLoader().getResource(clsUri);
            final String clsPath = url.getPath();
            final String computePath = clsPath.substring(0, clsPath.length() - clsUri.length());

            root = new File(computePath).getParentFile();
        } else {
            root = new File(buildDirectory);
        }
        if (!root.exists())
            root.mkdirs();
        return root;
    }

    /**
     * Creates a {@link File} reference in the path returned from {@link #makeTestDataPath} in a subdirectory
     * called {@code temp}.
     */
    public static File generateTempFile(final Class<?> clazz, final String fileName, final String fileNameSuffix) throws IOException {
        final File path = makeTestDataPath(clazz, "temp");
        if (!path.exists())
            path.mkdirs();
        return File.createTempFile(fileName, fileNameSuffix, path);
    }

    /**
     * Copies a file stored as part of a resource to the file system in the path returned from
     * {@link #makeTestDataPath} in a subdirectory called {@code temp/resources}.
     */
    public static File generateTempFileFromResource(final Class<?> resourceClass, final String resourceName, final String extension) throws IOException {
        return generateTempFileFromResource(resourceClass, resourceClass, resourceName, extension);
    }

    /**
     * Copies a file stored as part of a resource to the file system in the path returned from
     * {@link #makeTestDataPath} in a subdirectory called {@code temp/resources}.
     */
    public static File generateTempFileFromResource(final Class<?> graphClass, final Class<?> resourceClass, final String resourceName, final String extension) throws IOException {
        return generateTempFileFromResource(graphClass, resourceClass, resourceName, extension, true);
    }

    /**
     * Copies a file stored as part of a resource to the file system in the path returned from
     * {@link #makeTestDataPath} in a subdirectory called {@code temp/resources}.
     */
    public static File generateTempFileFromResource(final Class<?> graphClass, final Class<?> resourceClass,
                                                    final String resourceName, final String extension, final boolean overwrite) throws IOException {
        final File temp = makeTestDataPath(graphClass, "resources");
        final File tempFile = new File(temp, resourceName + extension);

        // these checks are present mostly for windows compatibility where an outputstream created on a non-existent
        // file will cause an error.
        if(tempFile.exists() && !overwrite){
            // overwrite is disabled and file already exists -> reuse as-is
            return tempFile;
        }
        if(!tempFile.getParentFile().exists()){
            Files.createDirectories(tempFile.getParentFile().toPath());
        }
        // either the file does not exist or needs to be overwritten, drop it
        Files.deleteIfExists(tempFile.toPath());
        // create the new file
        Files.createFile(tempFile.toPath());

        // fill it with the desired contents
        try (final FileOutputStream outputStream = new FileOutputStream(tempFile)) {
            final byte[] buf = new byte[1024]; // use a buffer to boost the read/write speed
            int sz;
            try (final InputStream inputStream = resourceClass.getResourceAsStream(resourceName)) {
                while ((sz = inputStream.read(buf)) != -1) {
                    outputStream.write(buf, 0, sz);
                }
            }
        }
        return tempFile;
    }

    /**
     * Removes characters that aren't acceptable in a file path (mostly for windows).
     */
    public static String cleanPathSegment(final String toClean) {
        final String cleaned = toClean.replaceAll("[.\\\\/:*?\"<>|\\[\\]\\(\\)]", "");
        if (cleaned.length() == 0)
            throw new IllegalStateException("Path segment " + toClean + " has not valid characters and is thus empty");
        return cleaned;
    }
}
