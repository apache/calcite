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
package org.apache.calcite.dropwizard.metrics.hadoop;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Dropwizard-Metrics {@link com.codahale.metrics.Reporter} which also acts as a Hadoop Metrics2
 * {@link MetricsSource}. Configure it like other Reporters.
 *
 * <pre>
 * final HadoopMetrics2Reporter metrics2Reporter = HadoopMetrics2Reporter.forRegistry(metrics)
 *     .build(DefaultMetricsSystem.initialize("Phoenix"), // The application-level name
 *            "QueryServer", // Component name
 *            "Phoenix Query Server", // Component description
 *            "General"); // Name for each metric record
 * metrics2Reporter.start(30, TimeUnit.SECONDS);
 * </pre>
 */
public class HadoopMetrics2Reporter extends ScheduledReporter implements MetricsSource {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopMetrics2Reporter.class);
  private static final String EMPTY_STRING = "";

  /**
   * Returns a new {@link Builder} for {@link HadoopMetrics2Reporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link HadoopMetrics2Reporter}
   */
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * A builder to create {@link HadoopMetrics2Reporter} instances.
   */
  public static class Builder {
    private final MetricRegistry registry;
    private MetricFilter filter;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private String recordContext;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.filter = MetricFilter.ALL;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
    }

    /**
     * Convert rates to the given time unit. Defaults to {@link TimeUnit#SECONDS}.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = Objects.requireNonNull(rateUnit);
      return this;
    }

    /**
     * Convert durations to the given time unit. Defaults to {@link TimeUnit#MILLISECONDS}.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = Objects.requireNonNull(durationUnit);
      return this;
    }

    /**
     * Only report metrics which match the given filter. Defaults to {@link MetricFilter#ALL}.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = Objects.requireNonNull(filter);
      return this;
    }

    /**
     * A "context" name that will be added as a tag on each emitted metric record. Defaults to
     * no "context" attribute on each record.
     *
     * @param recordContext The "context" tag
     * @return {@code this}
     */
    public Builder recordContext(String recordContext) {
      this.recordContext = Objects.requireNonNull(recordContext);
      return this;
    }

    /**
     * Builds a {@link HadoopMetrics2Reporter} with the given properties, making metrics available
     * to the Hadoop Metrics2 framework (any configured {@link MetricsSource}s.
     *
     * @param metrics2System The Hadoop Metrics2 system instance.
     * @param jmxContext The JMX "path", e.g. {@code "MyServer,sub=Requests"}.
     * @param description A description these metrics.
     * @param recordName A suffix included on each record to identify it.
     *
     * @return a {@link HadoopMetrics2Reporter}
     */
    public HadoopMetrics2Reporter build(MetricsSystem metrics2System, String jmxContext,
        String description, String recordName) {
      return new HadoopMetrics2Reporter(registry,
          rateUnit,
          durationUnit,
          filter,
          metrics2System,
          Objects.requireNonNull(jmxContext),
          description,
          recordName,
          recordContext);
    }
  }

  private final MetricsRegistry metrics2Registry;
  private final MetricsSystem metrics2System;
  private final String recordName;
  private final String context;

  @SuppressWarnings("rawtypes")
  private final ConcurrentLinkedQueue<Entry<String, Gauge>> dropwizardGauges;
  private final ConcurrentLinkedQueue<Entry<String, Counter>> dropwizardCounters;
  private final ConcurrentLinkedQueue<Entry<String, Histogram>> dropwizardHistograms;
  private final ConcurrentLinkedQueue<Entry<String, Meter>> dropwizardMeters;
  private final ConcurrentLinkedQueue<Entry<String, Timer>> dropwizardTimers;

  private HadoopMetrics2Reporter(MetricRegistry registry, TimeUnit rateUnit, TimeUnit durationUnit,
      MetricFilter filter, MetricsSystem metrics2System, String jmxContext, String description,
      String recordName, String context) {
    super(registry, "hadoop-metrics2-reporter", filter, rateUnit, durationUnit);
    this.metrics2Registry = new MetricsRegistry(Interns.info(jmxContext, description));
    this.metrics2System = metrics2System;
    this.recordName = recordName;
    this.context = context;

    this.dropwizardGauges = new ConcurrentLinkedQueue<>();
    this.dropwizardCounters = new ConcurrentLinkedQueue<>();
    this.dropwizardHistograms = new ConcurrentLinkedQueue<>();
    this.dropwizardMeters = new ConcurrentLinkedQueue<>();
    this.dropwizardTimers = new ConcurrentLinkedQueue<>();

    // Register this source with the Metrics2 system.
    // Make sure this is the last thing done as getMetrics() can be called at any time after.
    this.metrics2System.register(Objects.requireNonNull(jmxContext),
        Objects.requireNonNull(description), this);
  }

  @Override public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(recordName);
    if (null != context) {
      builder.setContext(context);
    }

    snapshotAllMetrics(builder);

    metrics2Registry.snapshot(builder, all);
  }

  /**
   * Consumes the current metrics collected by dropwizard and adds them to the {@code builder}.
   *
   * @param builder A record builder
   */
  void snapshotAllMetrics(MetricsRecordBuilder builder) {
    // Pass through the gauges
    @SuppressWarnings("rawtypes")
    Iterator<Entry<String, Gauge>> gaugeIterator = dropwizardGauges.iterator();
    while (gaugeIterator.hasNext()) {
      @SuppressWarnings("rawtypes")
      Entry<String, Gauge> gauge = gaugeIterator.next();
      final MetricsInfo info = Interns.info(gauge.getKey(), EMPTY_STRING);
      final Object o = gauge.getValue().getValue();

      // Figure out which gauge types metrics2 supports and call the right method
      if (o instanceof Integer) {
        builder.addGauge(info, (int) o);
      } else if (o instanceof Long) {
        builder.addGauge(info, (long) o);
      } else if (o instanceof Float) {
        builder.addGauge(info, (float) o);
      } else if (o instanceof Double) {
        builder.addGauge(info, (double) o);
      } else {
        LOG.info("Ignoring Gauge ({}) with unhandled type: {}", gauge.getKey(), o.getClass());
      }

      gaugeIterator.remove();
    }

    // Pass through the counters
    Iterator<Entry<String, Counter>> counterIterator = dropwizardCounters.iterator();
    while (counterIterator.hasNext()) {
      Entry<String, Counter> counter = counterIterator.next();
      MetricsInfo info = Interns.info(counter.getKey(), EMPTY_STRING);
      LOG.info("Adding counter {} {}", info, counter.getValue().getCount());
      builder.addCounter(info, counter.getValue().getCount());
      counterIterator.remove();
    }

    // Pass through the histograms
    Iterator<Entry<String, Histogram>> histogramIterator = dropwizardHistograms.iterator();
    while (histogramIterator.hasNext()) {
      final Entry<String, Histogram> entry = histogramIterator.next();
      final String name = entry.getKey();
      final Histogram histogram = entry.getValue();

      addSnapshot(builder, name, EMPTY_STRING, histogram.getSnapshot(), histogram.getCount());

      histogramIterator.remove();
    }

    // Pass through the meter values
    Iterator<Entry<String, Meter>> meterIterator = dropwizardMeters.iterator();
    while (meterIterator.hasNext()) {
      final Entry<String, Meter> meterEntry = meterIterator.next();
      final String name = meterEntry.getKey();
      final Meter meter = meterEntry.getValue();

      addMeter(builder, name, EMPTY_STRING, meter.getMeanRate(), meter.getOneMinuteRate(),
          meter.getFiveMinuteRate(), meter.getFifteenMinuteRate());

      meterIterator.remove();
    }

    // Pass through the timers (meter + histogram)
    Iterator<Entry<String, Timer>> timerIterator = dropwizardTimers.iterator();
    while (timerIterator.hasNext()) {
      final Entry<String, Timer> timerEntry = timerIterator.next();
      final String name = timerEntry.getKey();
      final Timer timer = timerEntry.getValue();
      final Snapshot snapshot = timer.getSnapshot();

      // Add the meter info (mean rate and rate over time windows)
      addMeter(builder, name, EMPTY_STRING, timer.getMeanRate(), timer.getOneMinuteRate(),
          timer.getFiveMinuteRate(), timer.getFifteenMinuteRate());

      addSnapshot(builder, name, EMPTY_STRING, snapshot, timer.getCount());

      timerIterator.remove();
    }

    // Add in metadata about what the units the reported metrics are displayed using.
    builder.tag(Interns.info("rate_unit", "The unit of measure for rate metrics"), getRateUnit());
    builder.tag(Interns.info("duration_unit", "The unit of measure of duration metrics"),
        getDurationUnit());
  }

  /**
   * Add Dropwizard-Metrics rate information to a Hadoop-Metrics2 record builder, converting the
   * rates to the appropriate unit.
   *
   * @param builder A Hadoop-Metrics2 record builder.
   * @param name A base name for this record.
   * @param desc A description for the record.
   * @param meanRate The average measured rate.
   * @param oneMinuteRate The measured rate over the past minute.
   * @param fiveMinuteRate The measured rate over the past five minutes
   * @param fifteenMinuteRate The measured rate over the past fifteen minutes.
   */
  private void addMeter(MetricsRecordBuilder builder, String name, String desc, double meanRate,
      double oneMinuteRate, double fiveMinuteRate, double fifteenMinuteRate) {
    builder.addGauge(Interns.info(name + "_mean_rate", EMPTY_STRING), convertRate(meanRate));
    builder.addGauge(Interns.info(name + "_1min_rate", EMPTY_STRING), convertRate(oneMinuteRate));
    builder.addGauge(Interns.info(name + "_5min_rate", EMPTY_STRING), convertRate(fiveMinuteRate));
    builder.addGauge(Interns.info(name + "_15min_rate", EMPTY_STRING),
        convertRate(fifteenMinuteRate));
  }

  /**
   * Add Dropwizard-Metrics value-distribution data to a Hadoop-Metrics2 record building, converting
   * the durations to the appropriate unit.
   *
   * @param builder A Hadoop-Metrics2 record builder.
   * @param name A base name for this record.
   * @param desc A description for this record.
   * @param snapshot The distribution of measured values.
   * @param count The number of values which were measured.
   */
  private void addSnapshot(MetricsRecordBuilder builder, String name, String desc,
      Snapshot snapshot, long count) {
    builder.addGauge(Interns.info(name + "_count", desc), count);

    builder.addGauge(Interns.info(name + "_mean", desc), convertDuration(snapshot.getMean()));
    builder.addGauge(Interns.info(name + "_min", desc), convertDuration(snapshot.getMin()));
    builder.addGauge(Interns.info(name + "_max", desc), convertDuration(snapshot.getMax()));
    builder.addGauge(Interns.info(name + "_median", desc), convertDuration(snapshot.getMedian()));
    builder.addGauge(Interns.info(name + "_stddev", desc), convertDuration(snapshot.getStdDev()));

    builder.addGauge(Interns.info(name + "_75thpercentile", desc),
        convertDuration(snapshot.get75thPercentile()));
    builder.addGauge(Interns.info(name + "_95thpercentile", desc),
        convertDuration(snapshot.get95thPercentile()));
    builder.addGauge(Interns.info(name + "_98thpercentile", desc),
        convertDuration(snapshot.get98thPercentile()));
    builder.addGauge(Interns.info(name + "_99thpercentile", desc),
        convertDuration(snapshot.get99thPercentile()));
    builder.addGauge(Interns.info(name + "_999thpercentile", desc),
        convertDuration(snapshot.get999thPercentile()));
  }

  @SuppressWarnings("rawtypes")
  @Override public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    for (Entry<String, Gauge> gauge : gauges.entrySet()) {
      dropwizardGauges.add(gauge);
    }

    for (Entry<String, Counter> counter : counters.entrySet()) {
      dropwizardCounters.add(counter);
    }

    for (Entry<String, Histogram> histogram : histograms.entrySet()) {
      dropwizardHistograms.add(histogram);
    }

    for (Entry<String, Meter> meter : meters.entrySet()) {
      dropwizardMeters.add(meter);
    }

    for (Entry<String, Timer> timer : timers.entrySet()) {
      dropwizardTimers.add(timer);
    }
  }

  @Override protected String getRateUnit() {
    // Make it "events per rate_unit" to be accurate.
    return "events/" + super.getRateUnit();
  }

  // Getters visible for testing

  MetricsRegistry getMetrics2Registry() {
    return metrics2Registry;
  }

  MetricsSystem getMetrics2System() {
    return metrics2System;
  }

  String getRecordName() {
    return recordName;
  }

  String getContext() {
    return context;
  }

  @SuppressWarnings("rawtypes") ConcurrentLinkedQueue<Entry<String, Gauge>> getDropwizardGauges() {
    return dropwizardGauges;
  }

  ConcurrentLinkedQueue<Entry<String, Counter>> getDropwizardCounters() {
    return dropwizardCounters;
  }

  ConcurrentLinkedQueue<Entry<String, Histogram>> getDropwizardHistograms() {
    return dropwizardHistograms;
  }

  ConcurrentLinkedQueue<Entry<String, Meter>> getDropwizardMeters() {
    return dropwizardMeters;
  }

  ConcurrentLinkedQueue<Entry<String, Timer>> getDropwizardTimers() {
    return dropwizardTimers;
  }
}

// End HadoopMetrics2Reporter.java
