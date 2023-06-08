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
package org.apache.calcite.util;

import org.apache.calcite.runtime.PairList;
import org.apache.calcite.runtime.Unit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * A text processor similar to Awk.
 *
 * <p>Example use:
 *
 * <blockquote><pre>{@code
 * File file;
 * final Puffin.Program program =
 *   Puffin.builder()
 *       .add(line -> !line.startsWith("#"),
 *           line -> counter.incrementAndGet())
 *       .after(context ->
 *           context.println("There were " + counter.get()
 *               + " uncommented lines"))
 *       .build();
 * program.execute(Source.of(file), System.out);
 * }</pre></blockquote>
 *
 * <p>prints the following to stdout:
 *
 * <blockquote>{@code
 * There were 3 uncommented lines.
 * }</blockquote>
 */
public class Puffin {
  private Puffin() {
  }

  /** Creates a Builder.
   *
   * @param fileStateFactory Creates the state for each file
   * @return Builder
   * @param <G> Type of state that is created when we start processing
   * @param <F> Type of state that is created when we start processing a file
   */
  public static <G, F> Builder<G, F> builder(Supplier<G> globalStateFactory,
      Function<G, F> fileStateFactory) {
    return new BuilderImpl<>(globalStateFactory, fileStateFactory,
        PairList.of(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
        new ArrayList<>());
  }

  /** Creates a Builder with no state. */
  public static Builder<Unit, Unit> builder() {
    return builder(() -> Unit.INSTANCE, u -> u);
  }

  /** Fluent interface for constructing a Program.
   *
   * @param <G> Type of state that is created when we start processing
   * @param <F> Type of state that is created when we start processing a file
   * @see Puffin#builder */
  public interface Builder<G, F> {
    /** Adds a predicate and action to be invoked on each line of a source. */
    Builder<G, F> add(Predicate<Line<G, F>> linePredicate,
        Consumer<Line<G, F>> action);

    /** Adds an action to be called before each source. */
    Builder<G, F> beforeSource(Consumer<Context<G, F>> action);

    /** Adds an action to be called after each source. */
    Builder<G, F> afterSource(Consumer<Context<G, F>> action);

    /** Adds an action to be called before all sources. */
    Builder<G, F> before(Consumer<Context<G, F>> action);

    /** Adds an action to be called after all sources. */
    Builder<G, F> after(Consumer<Context<G, F>> action);

    /** Builds the program. */
    Program<G> build();
  }

  /** A Puffin program. You can execute it on a file.
   *
   * @param <G> Type of state that is created when we start processing */
  public interface Program<G> {
    /** Executes this program. */
    G execute(Stream<? extends Source> sources, PrintWriter out);

    /** Executes this program, writing to an output stream such as
     * {@link System#out}. */
    default void execute(Stream<? extends Source> sources, OutputStream out) {
      try (PrintWriter w = Util.printWriter(out)) {
        execute(sources, w);
      }
    }

    /** Executes this program on a single source. */
    default void execute(Source source, OutputStream out) {
      execute(Stream.of(source), out);
    }
  }

  /** A line in a file.
   *
   * <p>Created by an executing program and passed to the predicate
   * and action that you registered in
   * {@link Builder#add(Predicate, Consumer)}.
   *
   * @param <G> Type of state that is created when we start processing
   * @param <F> Type of state that is created when we start processing a file
   */
  public interface Line<G, F> {
    G globalState();
    F state();
    int fnr();
    Source source();
    boolean startsWith(String prefix);
    boolean contains(CharSequence s);
    boolean endsWith(String suffix);
    boolean matches(String regex);
    String line();
  }

  /** Context for executing a Puffin program within a given file.
   *
   * @param <G> Type of state that is created when we start processing
   * @param <F> Type of state that is created when we start processing a file */
  public static class Context<G, F> {
    final PrintWriter out;
    final Source source;
    final F fileState;
    final G globalState;
    private final Function<String, Pattern> patternCache;

    /** Holds the current line. */
    String line = "";

    /** Holds the current line number in the file (starting from 1).
     *
     * <p>Corresponds to the Awk variable {@code FNR}, which stands for "file
     * number of records". */
    int fnr = 0;

    Context(PrintWriter out, Source source,
        Function<String, Pattern> patternCache, G globalState,
        F fileState) {
      this.out = requireNonNull(out, "out");
      this.source = requireNonNull(source, "source");
      this.patternCache = requireNonNull(patternCache, "patternCache");
      this.globalState = requireNonNull(globalState, "globalState");
      this.fileState = requireNonNull(fileState, "fileState");
    }

    public F state() {
      return fileState;
    }

    public G globalState() {
      return globalState;
    }

    public void println(String s) {
      out.println(s);
    }

    Pattern pattern(String regex) {
      return patternCache.apply(regex);
    }
  }

  /** Extension to {@link Context} that also implements {@link Line}.
   *
   * <p>We don't want clients to know that {@code Context} implements
   * {@code Line}, but neither do we want to create a new {@code Line} object
   * for every line in the file. Making this a subclass accomplishes both
   * goals.
   *
   * @param <G> Type of state that is created when we start processing
   * @param <F> Type of state that is created when we start processing a file */
  static class ContextImpl<G, F> extends Context<G, F> implements Line<G, F> {

    ContextImpl(PrintWriter out, Source source,
        Function<String, Pattern> patternCache, G globalState, F state) {
      super(out, source, patternCache, globalState, state);
    }

    @Override public int fnr() {
      return fnr;
    }

    @Override public Source source() {
      return source;
    }

    @Override public boolean startsWith(String prefix) {
      return line.startsWith(prefix);
    }

    @Override public boolean contains(CharSequence s) {
      return line.contains(s);
    }

    @Override public boolean endsWith(String suffix) {
      return line.endsWith(suffix);
    }

    @Override public boolean matches(String regex) {
      return pattern(regex).matcher(line).matches();
    }

    @Override public String line() {
      return line;
    }
  }

  /** Implementation of {@link Program}.
   *
   * @param <G> Type of state that is created when we start processing
   * @param <F> Type of state that is created when we start processing a file */
  private static class ProgramImpl<G, F> implements Program<G> {
    private final Supplier<G> globalStateFactory;
    private final Function<G, F> fileStateFactory;
    private final PairList<Predicate<Line<G, F>>, Consumer<Line<G, F>>> onLineList;
    private final ImmutableList<Consumer<Context<G, F>>> beforeSourceList;
    private final ImmutableList<Consumer<Context<G, F>>> afterSourceList;
    private final ImmutableList<Consumer<Context<G, F>>> beforeList;
    private final ImmutableList<Consumer<Context<G, F>>> afterList;
    @SuppressWarnings("Convert2MethodRef")
    private final LoadingCache<String, Pattern> patternCache0 =
        CacheBuilder.newBuilder()
            .build(CacheLoader.from(regex -> Pattern.compile(regex)));
    private final Function<String, Pattern> patternCache =
        patternCache0::getUnchecked;

    private ProgramImpl(Supplier<G> globalStateFactory,
        Function<G, F> fileStateFactory,
        PairList<Predicate<Line<G, F>>, Consumer<Line<G, F>>> onLineList,
        ImmutableList<Consumer<Context<G, F>>> beforeSourceList,
        ImmutableList<Consumer<Context<G, F>>> afterSourceList,
        ImmutableList<Consumer<Context<G, F>>> beforeList,
        ImmutableList<Consumer<Context<G, F>>> afterList) {
      this.globalStateFactory = globalStateFactory;
      this.fileStateFactory = fileStateFactory;
      this.onLineList = onLineList;
      this.beforeSourceList = beforeSourceList;
      this.afterSourceList = afterSourceList;
      this.beforeList = beforeList;
      this.afterList = afterList;
    }

    @Override public G execute(Stream<? extends Source> sources,
        PrintWriter out) {
      final G globalState = globalStateFactory.get();
      final Source source0 = Sources.of("");
      final F fileState0 = fileStateFactory.apply(globalState);
      final ContextImpl<G, F> x0 =
          new ContextImpl<G, F>(out, source0, patternCache, globalState,
              fileState0);
      beforeList.forEach(action -> action.accept(x0));
      sources.forEach(source -> execute(globalState, source, out));
      afterList.forEach(action -> action.accept(x0));
      return globalState;
    }

    private void execute(G globalState, Source source, PrintWriter out) {
      try (Reader r = source.reader();
           BufferedReader br = new BufferedReader(r)) {
        final F fileState = fileStateFactory.apply(globalState);
        final ContextImpl<G, F> x =
            new ContextImpl<G, F>(out, source, patternCache, globalState,
                fileState);
        beforeSourceList.forEach(action -> action.accept(x));
        for (;;) {
          String lineText = br.readLine();
          if (lineText == null) {
            break;
          }
          ++x.fnr;
          x.line = lineText;
          onLineList.forEach((predicate, action) -> {
            if (predicate.test(x)) {
              action.accept(x);
            }
          });
        }
        afterSourceList.forEach(action -> action.accept(x));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }


  /** Implementation of Builder.
   *
   * @param <G> Type of state that is created when we start processing
   * @param <F> Type of state that is created when we start processing a file */
  private static class BuilderImpl<G, F> implements Builder<G, F> {
    private final Supplier<G> globalStateFactory;
    private final Function<G, F> fileStateFactory;
    final PairList<Predicate<Line<G, F>>, Consumer<Line<G, F>>> onLineList;
    final List<Consumer<Context<G, F>>> beforeSourceList;
    final List<Consumer<Context<G, F>>> afterSourceList;
    final List<Consumer<Context<G, F>>> beforeList;
    final List<Consumer<Context<G, F>>> afterList;

    private BuilderImpl(Supplier<G> globalStateFactory,
        Function<G, F> fileStateFactory,
        PairList<Predicate<Line<G, F>>, Consumer<Line<G, F>>> onLineList,
        List<Consumer<Context<G, F>>> beforeSourceList,
        List<Consumer<Context<G, F>>> afterSourceList,
        List<Consumer<Context<G, F>>> beforeList,
        List<Consumer<Context<G, F>>> afterList) {
      this.globalStateFactory = globalStateFactory;
      this.fileStateFactory = fileStateFactory;
      this.onLineList = onLineList;
      this.beforeSourceList = beforeSourceList;
      this.afterSourceList = afterSourceList;
      this.beforeList = beforeList;
      this.afterList = afterList;
    }

    @Override public Builder<G, F> add(Predicate<Line<G, F>> linePredicate,
        Consumer<Line<G, F>> action) {
      onLineList.add(linePredicate, action);
      return this;
    }

    @Override public Builder<G, F> beforeSource(Consumer<Context<G, F>> action) {
      beforeSourceList.add(action);
      return this;
    }

    @Override public Builder<G, F> afterSource(Consumer<Context<G, F>> action) {
      afterSourceList.add(action);
      return this;
    }

    @Override public Builder<G, F> before(Consumer<Context<G, F>> action) {
      beforeList.add(action);
      return this;
    }

    @Override public Builder<G, F> after(Consumer<Context<G, F>> action) {
      afterList.add(action);
      return this;
    }

    @Override public Program<G> build() {
      return new ProgramImpl<>(globalStateFactory, fileStateFactory,
          onLineList.immutable(),
          ImmutableList.copyOf(beforeSourceList),
          ImmutableList.copyOf(afterSourceList),
          ImmutableList.copyOf(beforeList),
          ImmutableList.copyOf(afterList));
    }
  }
}
