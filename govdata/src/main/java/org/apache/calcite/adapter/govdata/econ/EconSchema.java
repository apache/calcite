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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.govdata.TableCommentDefinitions;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.CommentableSchema;
import org.apache.calcite.schema.CommentableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Economic data schema implementation.
 * 
 * <p>Provides SQL access to U.S. and international economic indicators including:
 * <ul>
 *   <li>BLS: Employment statistics, inflation metrics, wage growth</li>
 *   <li>Treasury: Yield curves, federal debt statistics</li>
 *   <li>World Bank: International economic comparisons</li>
 *   <li>FRED: Federal Reserve economic data (planned)</li>
 *   <li>BEA: Bureau of Economic Analysis data (planned)</li>
 * </ul>
 */
public class EconSchema extends FileSchema implements CommentableSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(EconSchema.class);
  
  private final String blsApiKey;
  private final String fredApiKey;
  private final Map<String, CommentableTable> econTables = new HashMap<>();
  
  @SuppressWarnings("unchecked")
  public EconSchema(Schema parentSchema, String name, Map<String, Object> operand) {
    // Cast parentSchema to SchemaPlus which FileSchema expects
    super((SchemaPlus) parentSchema, name,
          getSourceDirectory(operand),
          buildTableList(operand));
    
    // Extract API keys
    this.blsApiKey = extractApiKey(operand, "blsApiKey", "BLS_API_KEY");
    this.fredApiKey = extractApiKey(operand, "fredApiKey", "FRED_API_KEY");
    
    // Initialize economic tables with DDL metadata
    initializeEconTables();
    
    LOGGER.info("EconSchema created with name: {} (BLS: {}, FRED: {})", 
        name, 
        blsApiKey != null ? "configured" : "not configured",
        fredApiKey != null ? "configured" : "not configured");
  }
  
  private static File getSourceDirectory(Map<String, Object> operand) {
    String directory = (String) operand.get("directory");
    if (directory != null) {
      return new File(directory);
    }
    
    String cacheDirectory = (String) operand.get("cacheDirectory");
    if (cacheDirectory != null) {
      return new File(cacheDirectory);
    }
    
    String cacheHome = System.getenv("ECON_CACHE_DIR");
    if (cacheHome == null) {
      cacheHome = System.getProperty("user.home") + "/.calcite/econ-cache";
    }
    return new File(cacheHome);
  }
  
  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> buildTableList(Map<String, Object> operand) {
    // Avoid using operand.get("tables") as it might contain JsonTable objects from model parser
    // Instead, create our own clean list with just ECON table definitions
    List<Map<String, Object>> tables = new ArrayList<>();
    
    // Add all ECON tables to the list
    tables.addAll(createEconTableDefinitions());
    
    return tables;
  }
  
  private static List<Map<String, Object>> createEconTableDefinitions() {
    List<Map<String, Object>> tables = new ArrayList<>();
    
    // BLS tables
    Map<String, Object> employmentStats = new HashMap<>();
    employmentStats.put("name", "employment_statistics");
    employmentStats.put("type", "PartitionedParquetTable");
    employmentStats.put("comment", "U.S. employment and unemployment statistics from BLS including national "
        + "unemployment rate, labor force participation, job openings, and employment by sector. "
        + "Updated monthly with seasonal adjustments.");
    tables.add(employmentStats);
    
    Map<String, Object> inflationMetrics = new HashMap<>();
    inflationMetrics.put("name", "inflation_metrics");
    inflationMetrics.put("type", "PartitionedParquetTable");
    inflationMetrics.put("comment", "Consumer Price Index (CPI) and Producer Price Index (PPI) data tracking "
        + "inflation across different categories of goods and services. Includes urban, regional, "
        + "and sector-specific inflation rates.");
    tables.add(inflationMetrics);
    
    Map<String, Object> wageGrowth = new HashMap<>();
    wageGrowth.put("name", "wage_growth");
    wageGrowth.put("type", "PartitionedParquetTable");
    wageGrowth.put("comment", "Average hourly earnings, weekly earnings, and employment cost index by "
        + "industry and occupation. Tracks wage growth trends and labor cost pressures.");
    tables.add(wageGrowth);
    
    Map<String, Object> regionalEmployment = new HashMap<>();
    regionalEmployment.put("name", "regional_employment");
    regionalEmployment.put("type", "PartitionedParquetTable");
    regionalEmployment.put("comment", "State and metropolitan area employment statistics including "
        + "unemployment rates, job growth, and labor force participation by geographic region.");
    tables.add(regionalEmployment);
    
    // Treasury tables
    Map<String, Object> treasuryYields = new HashMap<>();
    treasuryYields.put("name", "treasury_yields");
    treasuryYields.put("type", "PartitionedParquetTable");
    treasuryYields.put("comment", "Daily U.S. Treasury yield curve rates from 1-month to 30-year maturities. "
        + "Includes nominal yields, TIPS yields, and yield curve shape indicators. "
        + "Source: U.S. Treasury Direct API.");
    tables.add(treasuryYields);
    
    Map<String, Object> federalDebt = new HashMap<>();
    federalDebt.put("name", "federal_debt");
    federalDebt.put("type", "PartitionedParquetTable");
    federalDebt.put("comment", "U.S. federal debt statistics including total public debt, debt held by public, "
        + "and intragovernmental holdings. Tracks debt levels, composition, and trends. "
        + "Source: Treasury Fiscal Data API.");
    tables.add(federalDebt);
    
    // World Bank tables
    Map<String, Object> worldIndicators = new HashMap<>();
    worldIndicators.put("name", "world_indicators");
    worldIndicators.put("type", "PartitionedParquetTable");
    worldIndicators.put("comment", "International economic indicators from World Bank for major economies. "
        + "Includes GDP, inflation, unemployment, government debt, and population statistics. "
        + "Enables comparison of U.S. economic performance with global peers.");
    tables.add(worldIndicators);
    
    // FRED tables
    Map<String, Object> fredIndicators = new HashMap<>();
    fredIndicators.put("name", "fred_indicators");
    fredIndicators.put("type", "PartitionedParquetTable");
    fredIndicators.put("comment", "Federal Reserve Economic Data (FRED) time series covering 800,000+ economic indicators. "
        + "Includes interest rates, monetary aggregates, exchange rates, commodity prices, and "
        + "economic activity measures. Primary source for U.S. economic time series data.");
    tables.add(fredIndicators);
    
    // BEA tables
    Map<String, Object> gdpComponents = new HashMap<>();
    gdpComponents.put("name", "gdp_components");
    gdpComponents.put("type", "PartitionedParquetTable");
    gdpComponents.put("comment", "Detailed GDP components from Bureau of Economic Analysis (BEA) NIPA tables. "
        + "Breaks down GDP into personal consumption, investment, government spending, and net exports. "
        + "Provides granular view of economic activity drivers and sectoral contributions to growth.");
    tables.add(gdpComponents);
    
    Map<String, Object> regionalIncome = new HashMap<>();
    regionalIncome.put("name", "regional_income");
    regionalIncome.put("type", "PartitionedParquetTable");
    regionalIncome.put("comment", "State and regional personal income statistics from BEA Regional Economic Accounts. "
        + "Includes total personal income, per capita income, and population by state. Essential "
        + "for understanding regional economic disparities and income trends across states.");
    tables.add(regionalIncome);
    
    Map<String, Object> stateGdp = new HashMap<>();
    stateGdp.put("name", "state_gdp");
    stateGdp.put("type", "PartitionedParquetTable");
    stateGdp.put("comment", "State-level GDP statistics from BEA Regional Economic Accounts (SAGDP2N table). "
        + "Provides both total GDP and per capita real GDP by state across all NAICS industry sectors. "
        + "Essential for understanding state economic output, productivity differences, and regional "
        + "economic performance comparisons across the United States.");
    tables.add(stateGdp);
    
    Map<String, Object> tradeStatistics = new HashMap<>();
    tradeStatistics.put("name", "trade_statistics");
    tradeStatistics.put("type", "PartitionedParquetTable");
    tradeStatistics.put("comment", "Detailed U.S. export and import statistics from BEA NIPA Table T40205B. "
        + "Provides comprehensive breakdown of goods and services trade by category including foods, "
        + "industrial supplies, capital goods, automotive, and consumer goods. Includes calculated "
        + "trade balances for matching export/import pairs. Essential for trade policy analysis.");
    tables.add(tradeStatistics);
    
    Map<String, Object> itaData = new HashMap<>();
    itaData.put("name", "ita_data");
    itaData.put("type", "PartitionedParquetTable");
    itaData.put("comment", "International Transactions Accounts (ITA) from BEA providing comprehensive balance "
        + "of payments statistics. Includes trade balance, current account balance, capital "
        + "account flows, and primary/secondary income balances. Critical for understanding "
        + "international financial flows and the U.S. position in global markets.");
    tables.add(itaData);
    
    Map<String, Object> industryGdp = new HashMap<>();
    industryGdp.put("name", "industry_gdp");
    industryGdp.put("type", "PartitionedParquetTable");
    industryGdp.put("comment", "GDP by Industry data from BEA showing value added by NAICS industry sectors. "
        + "Provides comprehensive breakdown of economic output by industry including "
        + "agriculture, mining, manufacturing, services, and government sectors. Available "
        + "at both annual and quarterly frequencies for detailed sectoral analysis.");
    tables.add(industryGdp);
    
    return tables;
  }
  
  private void initializeEconTables() {
    // BLS tables
    econTables.put("employment_statistics", new EmploymentStatisticsTable());
    econTables.put("inflation_metrics", new InflationMetricsTable());
    econTables.put("wage_growth", new WageGrowthTable());
    econTables.put("regional_employment", new RegionalEmploymentTable());
    
    // Treasury tables
    econTables.put("treasury_yields", new TreasuryYieldsTable());
    econTables.put("federal_debt", new FederalDebtTable());
    
    // World Bank tables
    econTables.put("world_indicators", new WorldIndicatorsTable());
  }
  
  private String extractApiKey(Map<String, Object> operand, String key, String envVar) {
    // Check explicit key in operand
    String apiKey = (String) operand.get(key);
    if (apiKey != null && !apiKey.isEmpty()) {
      return apiKey;
    }
    
    // Check environment variable
    apiKey = System.getenv(envVar);
    if (apiKey != null && !apiKey.isEmpty()) {
      return apiKey;
    }
    
    // Check system property
    String propKey = key.toLowerCase().replace("apikey", ".api.key");
    apiKey = System.getProperty(propKey);
    
    return apiKey;
  }
  
  @Override public @Nullable String getComment() {
    return "U.S. and international economic data including employment statistics, inflation metrics (CPI/PPI), "
        + "Federal Reserve interest rates, GDP components, Treasury yields, federal debt, and global economic indicators. "
        + "Data sources include Bureau of Labor Statistics (BLS), U.S. Treasury, World Bank, "
        + "Federal Reserve Economic Data (FRED), and Bureau of Economic Analysis (BEA). "
        + "Enables macroeconomic analysis, market research, and correlation with financial data.";
  }
  
  @Override
  protected Map<String, Table> getTableMap() {
    // Get parent table map from FileSchema
    Map<String, Table> tableMap = new HashMap<>(super.getTableMap());
    
    // Add ECON-specific tables with DDL metadata
    tableMap.putAll(econTables);
    
    return tableMap;
  }
  
  /**
   * Employment Statistics table implementation.
   */
  private static class EmploymentStatisticsTable implements CommentableTable {
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      
      builder.add("date", SqlTypeName.DATE)
          .add("series_id", SqlTypeName.VARCHAR)
          .add("series_name", SqlTypeName.VARCHAR)
          .add("value", SqlTypeName.DECIMAL)
          .add("unit", SqlTypeName.VARCHAR)
          .add("seasonally_adjusted", SqlTypeName.BOOLEAN)
          .add("percent_change_month", SqlTypeName.DECIMAL)
          .add("percent_change_year", SqlTypeName.DECIMAL)
          .add("category", SqlTypeName.VARCHAR)
          .add("subcategory", SqlTypeName.VARCHAR);
      
      return builder.build();
    }
    
    @Override
    public @Nullable String getTableComment() {
      return TableCommentDefinitions.getEconTableComment("employment_statistics");
    }
    
    @Override
    public @Nullable String getColumnComment(String columnName) {
      return TableCommentDefinitions.getEconColumnComment("employment_statistics", columnName);
    }
    
    @Override
    public Statistic getStatistic() {
      return Statistics.of(1000000D, null);
    }
    
    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }
    
    @Override
    public boolean isRolledUp(String column) {
      return false;
    }
    
    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
        @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }
  
  /**
   * Inflation Metrics table implementation.
   */
  private static class InflationMetricsTable implements CommentableTable {
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      
      builder.add("date", SqlTypeName.DATE)
          .add("index_type", SqlTypeName.VARCHAR)
          .add("item_code", SqlTypeName.VARCHAR)
          .add("item_name", SqlTypeName.VARCHAR)
          .add("index_value", SqlTypeName.DECIMAL)
          .add("percent_change_month", SqlTypeName.DECIMAL)
          .add("percent_change_year", SqlTypeName.DECIMAL)
          .add("area_code", SqlTypeName.VARCHAR)
          .add("area_name", SqlTypeName.VARCHAR)
          .add("seasonally_adjusted", SqlTypeName.BOOLEAN);
      
      return builder.build();
    }
    
    @Override
    public @Nullable String getTableComment() {
      return "Consumer Price Index (CPI) and Producer Price Index (PPI) data tracking "
          + "inflation across different categories of goods and services. Includes urban, regional, "
          + "and sector-specific inflation rates.";
    }
    
    @Override
    public @Nullable String getColumnComment(String columnName) {
      switch (columnName.toLowerCase()) {
        case "date": return "Observation date for the index value";
        case "index_type": return "Type of price index (CPI-U, CPI-W, PPI, etc.)";
        case "item_code": return "BLS item code for specific good/service category";
        case "item_name": return "Description of item or category (e.g., 'All items', 'Food', 'Energy')";
        case "index_value": return "Index value (base period = 100)";
        case "percent_change_month": return "Percent change from previous month";
        case "percent_change_year": return "Year-over-year percent change (inflation rate)";
        case "area_code": return "Geographic area code (U.S., regions, or metro areas)";
        case "area_name": return "Geographic area name";
        case "seasonally_adjusted": return "Whether data is seasonally adjusted";
        default: return null;
      }
    }
    
    @Override
    public Statistic getStatistic() {
      return Statistics.of(500000D, null);
    }
    
    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }
    
    @Override
    public boolean isRolledUp(String column) {
      return false;
    }
    
    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
        @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }
  
  /**
   * Wage Growth table implementation.
   */
  private static class WageGrowthTable implements CommentableTable {
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      
      builder.add("date", SqlTypeName.DATE)
          .add("series_id", SqlTypeName.VARCHAR)
          .add("industry_code", SqlTypeName.VARCHAR)
          .add("industry_name", SqlTypeName.VARCHAR)
          .add("occupation_code", SqlTypeName.VARCHAR)
          .add("occupation_name", SqlTypeName.VARCHAR)
          .add("average_hourly_earnings", SqlTypeName.DECIMAL)
          .add("average_weekly_earnings", SqlTypeName.DECIMAL)
          .add("employment_cost_index", SqlTypeName.DECIMAL)
          .add("percent_change_year", SqlTypeName.DECIMAL);
      
      return builder.build();
    }
    
    @Override
    public @Nullable String getTableComment() {
      return "Average hourly earnings, weekly earnings, and employment cost index by "
          + "industry and occupation. Tracks wage growth trends and labor cost pressures.";
    }
    
    @Override
    public @Nullable String getColumnComment(String columnName) {
      switch (columnName.toLowerCase()) {
        case "date": return "Observation date";
        case "series_id": return "BLS series identifier";
        case "industry_code": return "NAICS industry code";
        case "industry_name": return "Industry description";
        case "occupation_code": return "SOC occupation code";
        case "occupation_name": return "Occupation description";
        case "average_hourly_earnings": return "Average hourly earnings in dollars";
        case "average_weekly_earnings": return "Average weekly earnings in dollars";
        case "employment_cost_index": return "Employment cost index (base = 100)";
        case "percent_change_year": return "Year-over-year percent change in earnings";
        default: return null;
      }
    }
    
    @Override
    public Statistic getStatistic() {
      return Statistics.of(250000D, null);
    }
    
    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }
    
    @Override
    public boolean isRolledUp(String column) {
      return false;
    }
    
    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
        @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }
  
  /**
   * Regional Employment table implementation.
   */
  private static class RegionalEmploymentTable implements CommentableTable {
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      
      builder.add("date", SqlTypeName.DATE)
          .add("area_code", SqlTypeName.VARCHAR)
          .add("area_name", SqlTypeName.VARCHAR)
          .add("area_type", SqlTypeName.VARCHAR)
          .add("state_code", SqlTypeName.VARCHAR)
          .add("unemployment_rate", SqlTypeName.DECIMAL)
          .add("employment_level", SqlTypeName.BIGINT)
          .add("labor_force", SqlTypeName.BIGINT)
          .add("participation_rate", SqlTypeName.DECIMAL)
          .add("employment_population_ratio", SqlTypeName.DECIMAL);
      
      return builder.build();
    }
    
    @Override
    public @Nullable String getTableComment() {
      return "State and metropolitan area employment statistics including "
          + "unemployment rates, job growth, and labor force participation by geographic region.";
    }
    
    @Override
    public @Nullable String getColumnComment(String columnName) {
      switch (columnName.toLowerCase()) {
        case "date": return "Observation date";
        case "area_code": return "Geographic area code (FIPS or MSA code)";
        case "area_name": return "Geographic area name";
        case "area_type": return "Type of area (state, MSA, county)";
        case "state_code": return "Two-letter state code";
        case "unemployment_rate": return "Unemployment rate as percentage";
        case "employment_level": return "Number of employed persons";
        case "labor_force": return "Total labor force size";
        case "participation_rate": return "Labor force participation rate as percentage";
        case "employment_population_ratio": return "Employment to population ratio";
        default: return null;
      }
    }
    
    @Override
    public Statistic getStatistic() {
      return Statistics.of(100000D, null);
    }
    
    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }
    
    @Override
    public boolean isRolledUp(String column) {
      return false;
    }
    
    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
        @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }
  
  /**
   * Treasury Yields table implementation.
   */
  private static class TreasuryYieldsTable implements CommentableTable {
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      
      builder.add("date", SqlTypeName.DATE)
          .add("maturity_months", SqlTypeName.INTEGER)
          .add("maturity_label", SqlTypeName.VARCHAR)
          .add("yield_percent", SqlTypeName.DECIMAL)
          .add("yield_type", SqlTypeName.VARCHAR)
          .add("source", SqlTypeName.VARCHAR);
      
      return builder.build();
    }
    
    @Override
    public @Nullable String getTableComment() {
      return "Daily U.S. Treasury yield curve rates from 1-month to 30-year maturities. "
          + "Includes nominal yields, TIPS yields, and yield curve shape indicators. "
          + "Source: U.S. Treasury Direct API.";
    }
    
    @Override
    public @Nullable String getColumnComment(String columnName) {
      switch (columnName.toLowerCase()) {
        case "date": return "Trading date for the yield observation";
        case "maturity_months": return "Maturity period in months (1, 3, 6, 12, 24, 36, 60, 84, 120, 240, 360)";
        case "maturity_label": return "Human-readable maturity label (e.g., '3M', '2Y', '10Y', '30Y')";
        case "yield_percent": return "Yield rate as annual percentage";
        case "yield_type": return "Type of yield (Treasury Bill, Note, Bond, TIPS)";
        case "source": return "Data source (Treasury Direct)";
        default: return null;
      }
    }
    
    @Override
    public Statistic getStatistic() {
      return Statistics.of(50000D, null);
    }
    
    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }
    
    @Override
    public boolean isRolledUp(String column) {
      return false;
    }
    
    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
        @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }
  
  /**
   * Federal Debt table implementation.
   */
  private static class FederalDebtTable implements CommentableTable {
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      
      builder.add("date", SqlTypeName.DATE)
          .add("debt_type", SqlTypeName.VARCHAR)
          .add("amount_billions", SqlTypeName.DECIMAL)
          .add("percent_of_gdp", SqlTypeName.DECIMAL)
          .add("holder_category", SqlTypeName.VARCHAR)
          .add("debt_held_by_public", SqlTypeName.DECIMAL)
          .add("intragovernmental_holdings", SqlTypeName.DECIMAL);
      
      return builder.build();
    }
    
    @Override
    public @Nullable String getTableComment() {
      return "U.S. federal debt statistics including total public debt, debt held by public, "
          + "and intragovernmental holdings. Tracks debt levels, composition, and trends. "
          + "Source: Treasury Fiscal Data API.";
    }
    
    @Override
    public @Nullable String getColumnComment(String columnName) {
      switch (columnName.toLowerCase()) {
        case "date": return "Date of debt observation";
        case "debt_type": return "Type of debt (Total, Marketable, Non-marketable)";
        case "amount_billions": return "Debt amount in billions of dollars";
        case "percent_of_gdp": return "Debt as percentage of GDP (when available)";
        case "holder_category": return "Category of debt holder (Public, Foreign, Federal Reserve, etc.)";
        case "debt_held_by_public": return "Portion of debt held by public in billions";
        case "intragovernmental_holdings": return "Debt held by government accounts in billions";
        default: return null;
      }
    }
    
    @Override
    public Statistic getStatistic() {
      return Statistics.of(10000D, null);
    }
    
    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }
    
    @Override
    public boolean isRolledUp(String column) {
      return false;
    }
    
    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
        @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }
  
  /**
   * World Indicators table implementation.
   */
  private static class WorldIndicatorsTable implements CommentableTable {
    
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      
      builder.add("country_code", SqlTypeName.VARCHAR)
          .add("country_name", SqlTypeName.VARCHAR)
          .add("indicator_code", SqlTypeName.VARCHAR)
          .add("indicator_name", SqlTypeName.VARCHAR)
          .add("year", SqlTypeName.INTEGER)
          .add("value", SqlTypeName.DECIMAL)
          .add("unit", SqlTypeName.VARCHAR)
          .add("scale", SqlTypeName.VARCHAR);
      
      return builder.build();
    }
    
    @Override
    public @Nullable String getTableComment() {
      return "International economic indicators from World Bank for major economies. "
          + "Includes GDP, inflation, unemployment, government debt, and population statistics. "
          + "Enables comparison of U.S. economic performance with global peers.";
    }
    
    @Override
    public @Nullable String getColumnComment(String columnName) {
      switch (columnName.toLowerCase()) {
        case "country_code": return "ISO 3-letter country code (e.g., USA, CHN, JPN)";
        case "country_name": return "Full country name";
        case "indicator_code": return "World Bank indicator code (e.g., NY.GDP.MKTP.CD for GDP)";
        case "indicator_name": return "Human-readable indicator description";
        case "year": return "Year of observation";
        case "value": return "Indicator value";
        case "unit": return "Unit of measurement (USD, percent, etc.)";
        case "scale": return "Scale factor if applicable";
        default: return null;
      }
    }
    
    @Override
    public Statistic getStatistic() {
      return Statistics.of(100000D, null);
    }
    
    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }
    
    @Override
    public boolean isRolledUp(String column) {
      return false;
    }
    
    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
        @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
      return false;
    }
  }
}