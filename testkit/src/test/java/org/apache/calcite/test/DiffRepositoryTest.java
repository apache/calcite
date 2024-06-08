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
package org.apache.calcite.test;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests DiffRepository for fixtures.
 *
 * @see DiffRepository */

/**
 * Stub of DiffRepository to create a fake source JAR file.
 */
class JarStubDiffRepository extends DiffRepository {
  /**
   * Creates a DiffRepository.
   *
   * @param refFile        Reference file
   * @param logFile        Log file
   * @param baseRepository Parent repository or null
   * @param filter         Filter or null
   * @param indent         Indentation of XML file
   */
  private JarStubDiffRepository(URL refFile, File logFile,
      @Nullable DiffRepository baseRepository, Filter filter, int indent) {
    super(refFile, logFile, baseRepository, filter, indent);
  }

  public static DiffRepository lookup(Class<?> clazz) {
    final JarKey key = new JarKey(clazz, null, null, 2);
    return REPOSITORY_CACHE.getUnchecked(key);
  }
  static final LoadingCache<JarKey, DiffRepository> REPOSITORY_CACHE =
      CacheBuilder.newBuilder().build(CacheLoader.from(JarKey::toRepo));
  private static class JarKey extends Key {
    JarKey(Class<?> clazz, DiffRepository baseRepository, Filter filter, int indent) {
      super(clazz, baseRepository, filter, indent);
      // STUBS THE REF FILE TO PRETEND IT LIVES IN A JAR
      this.refFilePath = "mypath/calcite-stub.jar!/randompath/abcdef.xml";;
    }
  }
}
public class DiffRepositoryTest {

  /**
   * Test that a DiffRepository from a file in a JAR
   * would create a log file outside of the JAR.
   **/
  @Test void testLogFilePathInJar() {
    final DiffRepository diffRepo = JarStubDiffRepository.lookup(FixtureTest.class);
    assertEquals("/tmp/calcite-stub/randompath/abcdef_actual.xml", diffRepo.logFile.getPath());
  }
}
