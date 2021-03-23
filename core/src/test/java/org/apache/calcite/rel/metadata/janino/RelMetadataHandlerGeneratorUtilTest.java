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
package org.apache.calcite.rel.metadata.janino;

import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.util.Sources;

import com.google.common.io.CharStreams;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

/**
 * Test {@link RelMetadataHandlerGeneratorUtil}.
 */
class RelMetadataHandlerGeneratorUtilTest {
  private static final Path RESULT_DIR = Paths.get("build/metadata");

  @Test void testAllPredicatesGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.AllPredicates.Handler.class);
  }

  @Test void testCollationGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.Collation.Handler.class);
  }

  @Test void testColumnOriginGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.ColumnOrigin.Handler.class);
  }

  @Test void testColumnUniquenessGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.ColumnUniqueness.Handler.class);
  }

  @Test void testCumulativeCostGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.CumulativeCost.Handler.class);
  }

  @Test void testDistinctRowCountGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.DistinctRowCount.Handler.class);
  }

  @Test void testDistributionGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.Distribution.Handler.class);
  }

  @Test void testExplainVisibilityGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.ExplainVisibility.Handler.class);
  }

  @Test void testExpressionLineageGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.ExpressionLineage.Handler.class);
  }

  @Test void testLowerBoundCostGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.LowerBoundCost.Handler.class);
  }

  @Test void testMaxRowCountGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.MaxRowCount.Handler.class);
  }

  @Test void testMemoryGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.Memory.Handler.class);
  }

  @Test void testMinRowCountGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.MinRowCount.Handler.class);
  }

  @Test void testNodeTypesGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.NodeTypes.Handler.class);
  }

  @Test void testNonCumulativeCostGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.NonCumulativeCost.Handler.class);
  }

  @Test void testParallelismGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.Parallelism.Handler.class);
  }

  @Test void testPercentageOriginalRowsGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.PercentageOriginalRows.Handler.class);
  }

  @Test void testPopulationSizeGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.PopulationSize.Handler.class);
  }

  @Test void testPredicatesGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.Predicates.Handler.class);
  }

  @Test void testRowCountGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.RowCount.Handler.class);
  }

  @Test void testSelectivityGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.Selectivity.Handler.class);
  }

  @Test void testSizeGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.Size.Handler.class);
  }

  @Test void testTableReferencesGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.TableReferences.Handler.class);
  }

  @Test void testUniqueKeysGenerateHandler() {
    testGenerateHandler(BuiltInMetadata.UniqueKeys.Handler.class);
  }

  /**
   * Performance a regression test on the generated code for a given handler.
   */
  private void testGenerateHandler(Class<? extends MetadataHandler<?>> handlerClass) {
    RelMetadataHandlerGeneratorUtil.HandlerNameAndGeneratedCode nameAndGeneratedCode =
        RelMetadataHandlerGeneratorUtil.generateHandler(handlerClass,
            DefaultRelMetadataProvider.INSTANCE.handlers(handlerClass));
    String resourcePath =
        nameAndGeneratedCode.getHandlerName().replace(".", "/") + ".java";
    writeActualResults(resourcePath,
        nameAndGeneratedCode.getGeneratedCode());
    String expected = readResource(resourcePath);
    assert !expected.contains("\r") : "Expected code should not contain \\r";
    assert !nameAndGeneratedCode.getGeneratedCode().equals("\r")
        : "Generated code should not contain \\r";
    Assertions.assertEquals(expected, nameAndGeneratedCode.getGeneratedCode());
  }

  private static String readResource(String resourceName) {
    URL url = castNonNull(
        RelMetadataHandlerGeneratorUtilTest.class.getClassLoader().getResource(resourceName));
    try (Reader reader = Sources.of(url).reader()) {
      return CharStreams.toString(reader).replace("\r\n", "\n");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void writeActualResults(String resourceName, String expectedResults) {
    try {
      Path target = RESULT_DIR.resolve(resourceName);
      Files.createDirectories(target.getParent());
      if (Files.exists(target)) {
        Files.delete(target);
      }
      try (Writer writer = Files.newBufferedWriter(target)) {
        writer.write(expectedResults);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
