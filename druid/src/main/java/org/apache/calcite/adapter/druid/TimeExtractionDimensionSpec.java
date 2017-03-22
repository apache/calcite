package org.apache.calcite.adapter.druid;


public class TimeExtractionDimensionSpec extends ExtractionDimensionSpec {
  private final String outputName;

  public TimeExtractionDimensionSpec(ExtractionFunction extractionFunction,
          String outputName
  ) {
    super("__time", extractionFunction, outputName);
    this.outputName = outputName;
  }

  public static TimeExtractionDimensionSpec makeFullTimeExtract() {
    return new TimeExtractionDimensionSpec(
            TimeExtractionFunction.createDefault(),
            DruidConnectionImpl.DEFAULT_RESPONSE_TIMESTAMP_COLUMN
    );
  }

  public String getOutputName() {
    return outputName;
  }

  public static TimeExtractionDimensionSpec makeExtract(Granularity granularity) {
    switch (granularity) {
    case YEAR:
      return new TimeExtractionDimensionSpec(
              TimeExtractionFunction.createFromGranularity(granularity), "year");
    case MONTH:
      return new TimeExtractionDimensionSpec(
              TimeExtractionFunction.createFromGranularity(granularity), "monthOfYear");
    case DAY:
      return new TimeExtractionDimensionSpec(
              TimeExtractionFunction.createFromGranularity(granularity), "dayOfMonth");
     default:
        return null;
    }
  }
}

