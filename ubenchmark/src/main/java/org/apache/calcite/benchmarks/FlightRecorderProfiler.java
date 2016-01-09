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
package org.apache.calcite.benchmarks;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.ExternalProfiler;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.Result;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Captures Flight Recorder log.
 * Note: Flight Recorder is available in OracleJDK only.
 * Usage of Flight Recorder in production requires a LICENSE FEE, however Flight Recorder is free
 * for use in test systems.
 * It is assumed you would not use Calcite benchmarks for running a production system, thus it is
 * believed to be safe.
 */
public class FlightRecorderProfiler implements ExternalProfiler {
  @Override public Collection<String> addJVMInvokeOptions(BenchmarkParams params) {
    return Collections.emptyList();
  }

  @Override public Collection<String> addJVMOptions(BenchmarkParams params) {
    StringBuilder sb = new StringBuilder();
    for (String param : params.getParamsKeys()) {
      if (sb.length() != 0) {
        sb.append('-');
      }
      sb.append(param).append('-').append(params.getParam(param));
    }

    long duration =
        getDurationSeconds(params.getWarmup()) + getDurationSeconds(params.getMeasurement());
    return Arrays.asList(
        "-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder",
        "-XX:StartFlightRecording=settings=profile,duration=" + duration + "s,filename="
            + params.getBenchmark() + "_" + sb + ".jfr");
  }

  private long getDurationSeconds(IterationParams warmup) {
    return warmup.getTime().convertTo(TimeUnit.SECONDS) * warmup.getCount();
  }

  @Override public void beforeTrial(BenchmarkParams benchmarkParams) {

  }

  @Override public Collection<? extends Result> afterTrial(BenchmarkResult br, long pid,
      File stdOut, File stdErr) {
    return Collections.emptyList();
  }

  @Override public boolean allowPrintOut() {
    return true;
  }

  @Override public boolean allowPrintErr() {
    return true;
  }

  @Override public String getDescription() {
    return "Collects Java Flight Recorder profile";
  }
}

// End FlightRecorderProfiler.java
