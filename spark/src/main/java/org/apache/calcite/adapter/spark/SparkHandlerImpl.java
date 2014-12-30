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
package org.apache.calcite.adapter.spark;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.util.javac.JaninoCompiler;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of
 * {@link org.apache.calcite.jdbc.CalcitePrepare.SparkHandler}. Gives the core
 * Calcite engine access to rules that only exist in the Spark module.
 */
public class SparkHandlerImpl implements CalcitePrepare.SparkHandler {
  private final HttpServer classServer;
  private final AtomicInteger classId;
  private final JavaSparkContext sparkContext =
      new JavaSparkContext("local[1]", "calcite");

  private static SparkHandlerImpl instance;
  private static final File SRC_DIR = new File("/tmp");
  private static final File CLASS_DIR = new File("spark/target/classes");

  /** Creates a SparkHandlerImpl. */
  private SparkHandlerImpl() {
    classServer = new HttpServer(CLASS_DIR);

    // Start the classServer and store its URI in a spark system property
    // (which will be passed to executors so that they can connect to it)
    classServer.start();
    System.setProperty("spark.repl.class.uri", classServer.uri());

    // Generate a starting point for class names that is unlikely to clash with
    // previous classes. A better solution would be to clear the class directory
    // on startup.
    final Calendar calendar = Calendar.getInstance();
    classId = new AtomicInteger(
        calendar.get(Calendar.HOUR_OF_DAY) * 10000
        + calendar.get(Calendar.MINUTE) * 100
        + calendar.get(Calendar.SECOND));
  }

  /** Creates a SparkHandlerImpl, initializing on first call. Calcite-core calls
   * this via reflection. */
  @SuppressWarnings("UnusedDeclaration")
  public static CalcitePrepare.SparkHandler instance() {
    if (instance == null) {
      instance = new SparkHandlerImpl();
    }
    return instance;
  }

  public RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel,
      boolean restructure) {
    RelNode root2 =
        planner.changeTraits(rootRel,
            rootRel.getTraitSet().plus(SparkRel.CONVENTION));
    return planner.changeTraits(root2, rootRel.getTraitSet());
  }

  public void registerRules(RuleSetBuilder builder) {
    for (RelOptRule rule : SparkRules.rules()) {
      builder.addRule(rule);
    }
    builder.removeRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
  }

  public Object sparkContext() {
    return sparkContext;
  }

  public boolean enabled() {
    return true;
  }

  public ArrayBindable compile(ClassDeclaration expr, String s) {
    try {
      String className = "CalciteProgram" + classId.getAndIncrement();
      File file = new File(SRC_DIR, className + ".java");
      FileWriter fileWriter = new FileWriter(file, false);
      String source = "public class " + className + "\n"
          + "    implements " + ArrayBindable.class.getName()
          + ", " + Serializable.class.getName()
          + " {\n"
          + s + "\n"
          + "}\n";

      System.out.println("======================");
      System.out.println(source);
      System.out.println("======================");

      fileWriter.write(source);
      fileWriter.close();
      JaninoCompiler compiler = new JaninoCompiler();
      compiler.getArgs().setDestdir(CLASS_DIR.getAbsolutePath());
      compiler.getArgs().setSource(source, file.getAbsolutePath());
      compiler.getArgs().setFullClassName(className);
      compiler.compile();
      Class<?> clazz = Class.forName(className);
      Object o = clazz.newInstance();
      return (ArrayBindable) o;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}

// End SparkHandlerImpl.java
