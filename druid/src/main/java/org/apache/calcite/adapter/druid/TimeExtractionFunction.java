package org.apache.calcite.adapter.druid;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

import static org.apache.calcite.adapter.druid.DruidQuery.writeFieldIf;

public class TimeExtractionFunction implements ExtractionDimensionSpec.ExtractionFunction {

  private final String format;

  private final String granularity;

  private final String timeZone;

  private final String local;

  public TimeExtractionFunction(String format, String granularity, String timeZone, String local) {
    this.format = format;
    this.granularity = granularity;
    this.timeZone = timeZone;
    this.local = local;
  }

  @Override public void write(JsonGenerator generator) throws IOException {

    generator.writeStartObject();
    generator.writeStringField("type", "timeFormat");
    writeFieldIf(generator, "format", format);
    writeFieldIf(generator, "granularity", granularity);
    writeFieldIf(generator, "timeZone", timeZone);
    writeFieldIf(generator, "local", local);
    generator.writeEndObject();
  }

  public static TimeExtractionFunction createDefault() {
    return new TimeExtractionFunction("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", null, "UTC", null);
  }

  public static TimeExtractionFunction createFromGranularity(Granularity granularity) {
    switch (granularity) {
    case DAY:
      return new TimeExtractionFunction("dd", null, "UTC", null);
    case MONTH:
      return new TimeExtractionFunction("MM", null, "UTC", null);
    case YEAR:
      return new TimeExtractionFunction("yyyy", null, "UTC", null);
    case HOUR:
      return new TimeExtractionFunction("hh", null, "UTC", null);
    default:
      throw new AssertionError("Extraction " + granularity.value + " is not valid");
    }
  }
}
