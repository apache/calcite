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
package net.hydromatic.optiq.impl.spark;

import net.hydromatic.linq4j.expressions.ClassDeclaration;

import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.rules.java.JavaRules;
import net.hydromatic.optiq.runtime.Bindable;
import net.hydromatic.optiq.runtime.Typed;

import org.eigenbase.javac.JaninoCompiler;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of {@link OptiqPrepare.SparkHandler}. Gives the core Optiq
 * engine access to rules that only exist in the Spark module.
 */
public class SparkHandlerImpl implements OptiqPrepare.SparkHandler {
  private final HttpServer classServer;
  private final AtomicInteger classId;
  private final JavaSparkContext sparkContext =
      new JavaSparkContext("local[1]", "optiq");

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

  /** Creates a SparkHandlerImpl, initializing on first call. Optiq-core calls
   * this via reflection. */
  @SuppressWarnings("UnusedDeclaration")
  public static OptiqPrepare.SparkHandler instance() {
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
    builder.removeRule(JavaRules.ENUMERABLE_VALUES_RULE);
  }

  public Object sparkContext() {
    return sparkContext;
  }

  public boolean enabled() {
    return true;
  }

  public Bindable compile(ClassDeclaration expr, String s) {
    try {
      String className = "OptiqProgram" + classId.getAndIncrement();
      File file = new File(SRC_DIR, className + ".java");
      FileWriter fileWriter = new FileWriter(file, false);
      String source =
          "public class " + className + "\n"
          + "    implements " + Bindable.class.getName()
          + ", " + Typed.class.getName()
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
      return (Bindable) o;
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
