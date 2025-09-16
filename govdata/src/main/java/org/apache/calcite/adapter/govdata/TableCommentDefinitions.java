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
package org.apache.calcite.adapter.govdata;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Comprehensive business definition comments for government data tables and columns.
 *
 * <p>This class provides standardized business definitions for SEC and GEO 
 * government data tables that are exposed through JDBC metadata operations.
 * Comments explain the business meaning and usage of each table and column.
 */
public final class TableCommentDefinitions {
  
  private TableCommentDefinitions() {
    // Utility class
  }

  // =========================== SEC SCHEMA COMMENTS ===========================
  
  /** SEC schema table comments */
  private static final Map<String, String> SEC_TABLE_COMMENTS = new HashMap<>();
  
  /** SEC schema column comments */
  private static final Map<String, Map<String, String>> SEC_COLUMN_COMMENTS = new HashMap<>();
  
  static {
    // financial_line_items table
    SEC_TABLE_COMMENTS.put("financial_line_items", 
        "Financial statement line items extracted from SEC XBRL filings. Each row represents a single "
        + "financial concept (e.g., Revenue, Assets, Liabilities) with its value for a specific reporting "
        + "period and context. Data is sourced from 10-K annual and 10-Q quarterly filings.");
    
    Map<String, String> financialLineItemsCols = new HashMap<>();
    financialLineItemsCols.put("cik", "Central Index Key - SEC's unique 10-digit identifier for registered entities");
    financialLineItemsCols.put("filing_type", "Type of SEC filing (10-K for annual reports, 10-Q for quarterly reports)");
    financialLineItemsCols.put("year", "Fiscal year of the filing (YYYY format)");
    financialLineItemsCols.put("filing_date", "Date when the filing was submitted to SEC (YYYY-MM-DD format)");
    financialLineItemsCols.put("concept", "XBRL concept name representing the financial metric (e.g., 'Revenues', 'Assets', 'NetIncomeLoss')");
    financialLineItemsCols.put("context_ref", "Reference to the XBRL context defining the reporting entity, period, and dimensions");
    financialLineItemsCols.put("value", "Numeric value of the financial concept (typically in USD)");
    financialLineItemsCols.put("label", "Human-readable label for the concept from XBRL taxonomy");
    financialLineItemsCols.put("units", "Unit of measurement (typically 'USD' for monetary values, 'shares' for share counts)");
    financialLineItemsCols.put("decimals", "Number of decimal places for rounding the value (-3 means thousands, -6 means millions)");
    financialLineItemsCols.put("accession_number", "Unique SEC document identifier (NNNNNNNNNN-NN-NNNNNN format)");
    financialLineItemsCols.put("period_start", "Start date of the reporting period (YYYY-MM-DD format)");
    financialLineItemsCols.put("period_end", "End date of the reporting period (YYYY-MM-DD format)");
    SEC_COLUMN_COMMENTS.put("financial_line_items", financialLineItemsCols);
    
    // filing_metadata table
    SEC_TABLE_COMMENTS.put("filing_metadata", 
        "Core filing information and company metadata for SEC submissions. Contains one row per filing "
        + "with essential details about the submitting entity, filing type, dates, and document references. "
        + "This table serves as the master record for linking detailed financial data.");
    
    Map<String, String> filingMetadataCols = new HashMap<>();
    filingMetadataCols.put("cik", "Central Index Key - SEC's unique 10-digit identifier for the filing entity");
    filingMetadataCols.put("accession_number", "Unique SEC document identifier in NNNNNNNNNN-NN-NNNNNN format");
    filingMetadataCols.put("filing_type", "Type of SEC filing (10-K, 10-Q, 8-K, etc.)");
    filingMetadataCols.put("filing_date", "Date when the filing was submitted to SEC (YYYY-MM-DD format)");
    filingMetadataCols.put("company_name", "Legal name of the reporting entity as registered with SEC");
    filingMetadataCols.put("fiscal_year", "Company's fiscal year for this filing (YYYY format)");
    filingMetadataCols.put("fiscal_year_end", "Company's fiscal year end date (MM-DD format, e.g., '12-31')");
    filingMetadataCols.put("period_of_report", "End date of the reporting period covered by this filing (YYYY-MM-DD format)");
    filingMetadataCols.put("acceptance_datetime", "Date and time when SEC accepted the filing (YYYY-MM-DD HH:MM:SS format)");
    filingMetadataCols.put("primary_document", "Filename of the primary filing document (e.g., 'form10k.htm')");
    filingMetadataCols.put("file_size", "Size of the filing in bytes");
    filingMetadataCols.put("state_of_incorporation", "Two-letter state code where the entity is incorporated (e.g., 'DE', 'CA')");
    SEC_COLUMN_COMMENTS.put("filing_metadata", filingMetadataCols);
    
    // insider_transactions table  
    SEC_TABLE_COMMENTS.put("insider_transactions",
        "Insider trading transactions reported on SEC Forms 3, 4, and 5. Tracks stock purchases, sales, "
        + "and other equity transactions by company officers, directors, and 10% shareholders. Each row "
        + "represents a single transaction with details about the insider, transaction type, and ownership changes.");
    
    Map<String, String> insiderTransactionsCols = new HashMap<>();
    insiderTransactionsCols.put("cik", "CIK of the company (issuer) whose securities were traded");
    insiderTransactionsCols.put("filing_type", "Type of insider form (3 for initial, 4 for changes, 5 for annual)");
    insiderTransactionsCols.put("year", "Year of the filing (YYYY format)");
    insiderTransactionsCols.put("filing_date", "Date when the insider form was filed with SEC (YYYY-MM-DD format)");
    insiderTransactionsCols.put("transaction_id", "Unique identifier for this transaction within the filing");
    insiderTransactionsCols.put("insider_cik", "CIK of the insider (officer/director) making the transaction");
    insiderTransactionsCols.put("insider_name", "Full name of the insider (officer, director, or beneficial owner)");
    insiderTransactionsCols.put("insider_title", "Title or position of the insider (CEO, CFO, Director, etc.)");
    insiderTransactionsCols.put("transaction_date", "Date when the transaction occurred (YYYY-MM-DD format)");
    insiderTransactionsCols.put("transaction_code", "SEC transaction code (P=Purchase, S=Sale, A=Grant, etc.)");
    insiderTransactionsCols.put("security_title", "Type of security traded (Common Stock, Stock Option, etc.)");
    insiderTransactionsCols.put("shares_transacted", "Number of shares or units involved in the transaction");
    insiderTransactionsCols.put("price_per_share", "Price per share for the transaction (in USD)");
    insiderTransactionsCols.put("total_value", "Total dollar value of the transaction (shares * price)");
    insiderTransactionsCols.put("shares_owned_following", "Total shares owned by insider after this transaction");
    insiderTransactionsCols.put("ownership_type", "Direct (D) or Indirect (I) ownership of the securities");
    SEC_COLUMN_COMMENTS.put("insider_transactions", insiderTransactionsCols);
    
    // footnotes table
    SEC_TABLE_COMMENTS.put("footnotes",
        "Financial statement footnotes and disclosures extracted from SEC filings. Contains the textual "
        + "explanations, accounting policies, and supplementary information that accompany financial statements. "
        + "Each row represents a single footnote or disclosure section.");
    
    Map<String, String> footnotesCols = new HashMap<>(); 
    footnotesCols.put("cik", "Central Index Key of the filing entity");
    footnotesCols.put("filing_type", "Type of SEC filing containing this footnote");
    footnotesCols.put("year", "Fiscal year of the filing");
    footnotesCols.put("accession_number", "SEC document identifier for the filing");
    footnotesCols.put("footnote_id", "Unique identifier for this footnote within the filing");
    footnotesCols.put("footnote_title", "Title or heading of the footnote section");
    footnotesCols.put("footnote_text", "Full text content of the footnote");
    footnotesCols.put("footnote_type", "Category of footnote (accounting policy, subsequent events, etc.)");
    footnotesCols.put("table_reference", "Financial statement table that this footnote relates to");
    footnotesCols.put("sequence_number", "Order of this footnote within the filing");
    SEC_COLUMN_COMMENTS.put("footnotes", footnotesCols);
  }
  
  // =========================== GEO SCHEMA COMMENTS ===========================
  
  /** GEO schema table comments */
  private static final Map<String, String> GEO_TABLE_COMMENTS = new HashMap<>();
  
  /** GEO schema column comments */
  private static final Map<String, Map<String, String>> GEO_COLUMN_COMMENTS = new HashMap<>();
  
  static {
    // tiger_states table
    GEO_TABLE_COMMENTS.put("tiger_states",
        "U.S. state boundaries and metadata from Census TIGER/Line shapefiles. Includes geographic "
        + "and demographic attributes for all 50 states, District of Columbia, Puerto Rico, and other "
        + "U.S. territories. Updated annually by the U.S. Census Bureau.");
    
    Map<String, String> tigerStatesCols = new HashMap<>();
    tigerStatesCols.put("state_fips", "2-digit Federal Information Processing Standards (FIPS) code for the state");
    tigerStatesCols.put("state_code", "2-letter USPS state abbreviation (e.g., 'CA', 'NY', 'TX')");
    tigerStatesCols.put("state_name", "Full official state name (e.g., 'California', 'New York', 'Texas')");
    tigerStatesCols.put("state_abbr", "2-letter state abbreviation (same as state_code)");
    tigerStatesCols.put("region", "Census region (1=Northeast, 2=Midwest, 3=South, 4=West)");
    tigerStatesCols.put("division", "Census division within region (more granular than region)");
    tigerStatesCols.put("land_area", "Land area in square meters (excludes water bodies)");
    tigerStatesCols.put("water_area", "Water area in square meters (lakes, rivers, coastal waters)");
    tigerStatesCols.put("total_area", "Total area in square meters (land + water)");
    tigerStatesCols.put("latitude", "Geographic center latitude in decimal degrees");
    tigerStatesCols.put("longitude", "Geographic center longitude in decimal degrees");
    GEO_COLUMN_COMMENTS.put("tiger_states", tigerStatesCols);
    
    // tiger_counties table
    GEO_TABLE_COMMENTS.put("tiger_counties", 
        "U.S. county boundaries from Census TIGER/Line shapefiles. Counties are the primary legal "
        + "divisions of states and serve as the basis for many statistical and administrative functions. "
        + "Includes parishes (Louisiana), boroughs (Alaska), and independent cities.");
    
    Map<String, String> tigerCountiesCols = new HashMap<>();
    tigerCountiesCols.put("state_fips", "2-digit FIPS code of the state containing this county");
    tigerCountiesCols.put("county_fips", "5-digit FIPS code for the county (state + 3-digit county code)");
    tigerCountiesCols.put("county_name", "Official county name (e.g., 'Los Angeles', 'Cook', 'Harris')");
    tigerCountiesCols.put("county_type", "Type of county subdivision (County, Parish, Borough, Independent City)");
    tigerCountiesCols.put("state_name", "Name of the state containing this county");
    tigerCountiesCols.put("land_area", "Land area in square meters");
    tigerCountiesCols.put("water_area", "Water area in square meters");
    tigerCountiesCols.put("total_area", "Total area in square meters (land + water)");
    tigerCountiesCols.put("latitude", "Geographic center latitude in decimal degrees");
    tigerCountiesCols.put("longitude", "Geographic center longitude in decimal degrees"); 
    tigerCountiesCols.put("population", "Total population from most recent decennial census");
    GEO_COLUMN_COMMENTS.put("tiger_counties", tigerCountiesCols);
    
    // census_places table
    GEO_TABLE_COMMENTS.put("census_places",
        "Census designated places including incorporated cities, towns, villages, and census designated "
        + "places (CDPs). These are statistical geographic entities used by the Census Bureau for "
        + "data collection and tabulation. Includes population and housing unit counts.");
    
    Map<String, String> censusPlacesCols = new HashMap<>();
    censusPlacesCols.put("place_fips", "5-digit FIPS place code within the state");
    censusPlacesCols.put("place_name", "Official name of the place (city, town, village, or CDP)");
    censusPlacesCols.put("state_fips", "2-digit FIPS code of the state containing this place");
    censusPlacesCols.put("state_code", "2-letter state abbreviation");
    censusPlacesCols.put("state_name", "Full state name");
    censusPlacesCols.put("place_type", "Type of place (city, town, village, CDP, borough, etc.)");
    censusPlacesCols.put("county_fips", "5-digit FIPS code of the primary county containing this place");
    censusPlacesCols.put("population", "Total population from most recent decennial census");
    censusPlacesCols.put("housing_units", "Total number of housing units from most recent census");
    censusPlacesCols.put("land_area", "Land area in square meters");
    censusPlacesCols.put("water_area", "Water area in square meters");
    censusPlacesCols.put("latitude", "Geographic center latitude in decimal degrees");
    censusPlacesCols.put("longitude", "Geographic center longitude in decimal degrees");
    GEO_COLUMN_COMMENTS.put("census_places", censusPlacesCols);
    
    // hud_zip_county table
    GEO_TABLE_COMMENTS.put("hud_zip_county",
        "HUD USPS ZIP code to county crosswalk mapping. Maps 5-digit ZIP codes to counties with "
        + "allocation ratios for residential, business, and other address types. Essential for "
        + "geographic analysis of ZIP-based data. Updated quarterly by HUD.");
    
    Map<String, String> hudZipCountyCols = new HashMap<>();
    hudZipCountyCols.put("zip", "5-digit USPS ZIP code");
    hudZipCountyCols.put("county_fips", "5-digit FIPS county code that contains addresses in this ZIP");
    hudZipCountyCols.put("state_fips", "2-digit FIPS code of the state");
    hudZipCountyCols.put("state_code", "2-letter state abbreviation");
    hudZipCountyCols.put("county_name", "Official county name");
    hudZipCountyCols.put("res_ratio", "Ratio of residential addresses in this ZIP-county combination (0.0 to 1.0)");
    hudZipCountyCols.put("bus_ratio", "Ratio of business addresses in this ZIP-county combination (0.0 to 1.0)");
    hudZipCountyCols.put("oth_ratio", "Ratio of other address types in this ZIP-county combination (0.0 to 1.0)");
    hudZipCountyCols.put("tot_ratio", "Total ratio for this ZIP-county combination (sum of res + bus + oth)");
    hudZipCountyCols.put("usps_zip_pref_city", "USPS preferred city name for this ZIP code");
    hudZipCountyCols.put("usps_zip_pref_state", "USPS preferred state abbreviation for this ZIP code");
    GEO_COLUMN_COMMENTS.put("hud_zip_county", hudZipCountyCols);
    
    // tiger_zctas table
    GEO_TABLE_COMMENTS.put("tiger_zctas",
        "ZIP Code Tabulation Areas (ZCTAs) from Census TIGER/Line files. Statistical entities "
        + "that represent ZIP Code service areas for statistical analysis. More stable than actual "
        + "ZIP codes and better suited for census data linkage. Include population and housing counts.");
    
    Map<String, String> tigerZctasCols = new HashMap<>();
    tigerZctasCols.put("zcta5", "5-digit ZIP Code Tabulation Area code");
    tigerZctasCols.put("zcta5_name", "ZCTA name (usually 'ZCTA5 XXXXX')");
    tigerZctasCols.put("state_fips", "Primary state FIPS code for this ZCTA");
    tigerZctasCols.put("state_code", "Primary state 2-letter abbreviation");
    tigerZctasCols.put("land_area", "Land area in square meters");
    tigerZctasCols.put("water_area", "Water area in square meters");
    tigerZctasCols.put("total_area", "Total area in square meters (land + water)");
    tigerZctasCols.put("intpt_lat", "Internal point latitude (geographic center)");
    tigerZctasCols.put("intpt_lon", "Internal point longitude (geographic center)");
    tigerZctasCols.put("population", "Total population from most recent decennial census");
    tigerZctasCols.put("housing_units", "Total housing units from most recent census");
    tigerZctasCols.put("aland_sqmi", "Land area in square miles");
    tigerZctasCols.put("awater_sqmi", "Water area in square miles");
    GEO_COLUMN_COMMENTS.put("tiger_zctas", tigerZctasCols);
    
    // tiger_census_tracts table
    GEO_TABLE_COMMENTS.put("tiger_census_tracts",
        "Census tracts from TIGER/Line files. Small statistical subdivisions of counties with "
        + "populations typically between 1,200 and 8,000 people. Primary geography for detailed "
        + "demographic analysis and American Community Survey (ACS) data tabulation.");
    
    Map<String, String> tigerTractsCols = new HashMap<>();
    tigerTractsCols.put("tract_geoid", "11-digit census tract Geographic Identifier (GEOID)");
    tigerTractsCols.put("state_fips", "2-digit state FIPS code");
    tigerTractsCols.put("county_fips", "3-digit county FIPS code within state");
    tigerTractsCols.put("tract_code", "6-digit tract code within county");
    tigerTractsCols.put("tract_name", "Tract name (usually numeric identifier)");
    tigerTractsCols.put("namelsad", "Name and Legal/Statistical Area Description");
    tigerTractsCols.put("mtfcc", "MAF/TIGER Feature Class Code");
    tigerTractsCols.put("funcstat", "Functional status of the tract");
    tigerTractsCols.put("land_area", "Land area in square meters");
    tigerTractsCols.put("water_area", "Water area in square meters");
    tigerTractsCols.put("intpt_lat", "Internal point latitude");
    tigerTractsCols.put("intpt_lon", "Internal point longitude");
    tigerTractsCols.put("population", "Total population");
    tigerTractsCols.put("housing_units", "Total housing units");
    tigerTractsCols.put("aland_sqmi", "Land area in square miles");
    tigerTractsCols.put("awater_sqmi", "Water area in square miles");
    GEO_COLUMN_COMMENTS.put("tiger_census_tracts", tigerTractsCols);
    
    // tiger_block_groups table
    GEO_TABLE_COMMENTS.put("tiger_block_groups",
        "Census block groups from TIGER/Line files. Statistical divisions of census tracts, "
        + "typically containing 600-3,000 people. Smallest geography for American Community Survey "
        + "(ACS) sample data and essential for neighborhood-level demographic analysis.");
    
    Map<String, String> tigerBlockGroupsCols = new HashMap<>();
    tigerBlockGroupsCols.put("bg_geoid", "12-digit block group Geographic Identifier");
    tigerBlockGroupsCols.put("state_fips", "2-digit state FIPS code");
    tigerBlockGroupsCols.put("county_fips", "3-digit county FIPS code");
    tigerBlockGroupsCols.put("tract_code", "6-digit census tract code");
    tigerBlockGroupsCols.put("blkgrp", "1-digit block group number within tract");
    tigerBlockGroupsCols.put("namelsad", "Name and Legal/Statistical Area Description");
    tigerBlockGroupsCols.put("mtfcc", "MAF/TIGER Feature Class Code");
    tigerBlockGroupsCols.put("funcstat", "Functional status of the block group");
    tigerBlockGroupsCols.put("land_area", "Land area in square meters");
    tigerBlockGroupsCols.put("water_area", "Water area in square meters");
    tigerBlockGroupsCols.put("intpt_lat", "Internal point latitude");
    tigerBlockGroupsCols.put("intpt_lon", "Internal point longitude");
    tigerBlockGroupsCols.put("population", "Total population");
    tigerBlockGroupsCols.put("housing_units", "Total housing units");
    tigerBlockGroupsCols.put("aland_sqmi", "Land area in square miles");
    tigerBlockGroupsCols.put("awater_sqmi", "Water area in square miles");
    GEO_COLUMN_COMMENTS.put("tiger_block_groups", tigerBlockGroupsCols);
    
    // tiger_cbsa table
    GEO_TABLE_COMMENTS.put("tiger_cbsa",
        "Core Based Statistical Areas (CBSAs) from TIGER/Line files. Geographic entities associated "
        + "with urban cores of 10,000+ population. Includes Metropolitan Statistical Areas (50,000+) "
        + "and Micropolitan Statistical Areas (10,000-49,999). Essential for urban/regional analysis.");
    
    Map<String, String> tigerCbsaCols = new HashMap<>();
    tigerCbsaCols.put("cbsa_code", "5-digit Core Based Statistical Area code");
    tigerCbsaCols.put("cbsa_name", "CBSA name (e.g., 'New York-Newark-Jersey City, NY-NJ-PA')");
    tigerCbsaCols.put("namelsad", "Name and Legal/Statistical Area Description");
    tigerCbsaCols.put("lsad", "Legal/Statistical Area Description code");
    tigerCbsaCols.put("memi", "Metropolitan/Micropolitan indicator");
    tigerCbsaCols.put("mtfcc", "MAF/TIGER Feature Class Code");
    tigerCbsaCols.put("land_area", "Land area in square meters");
    tigerCbsaCols.put("water_area", "Water area in square meters");
    tigerCbsaCols.put("intpt_lat", "Internal point latitude");
    tigerCbsaCols.put("intpt_lon", "Internal point longitude");
    tigerCbsaCols.put("cbsa_type", "Metropolitan or Micropolitan designation");
    tigerCbsaCols.put("population", "Total population");
    tigerCbsaCols.put("aland_sqmi", "Land area in square miles");
    tigerCbsaCols.put("awater_sqmi", "Water area in square miles");
    GEO_COLUMN_COMMENTS.put("tiger_cbsa", tigerCbsaCols);
    
    // hud_zip_cbsa_div table
    GEO_TABLE_COMMENTS.put("hud_zip_cbsa_div",
        "HUD crosswalk mapping ZIP codes to CBSA Divisions. CBSA Divisions are Metropolitan "
        + "Divisions within large Metropolitan Statistical Areas, enabling sub-metropolitan analysis "
        + "for major urban areas like New York, Los Angeles, Chicago.");
    
    Map<String, String> hudZipCbsaDivCols = new HashMap<>();
    hudZipCbsaDivCols.put("zip", "5-digit ZIP code");
    hudZipCbsaDivCols.put("cbsadiv", "CBSA Division code");
    hudZipCbsaDivCols.put("cbsadiv_name", "CBSA Division name");
    hudZipCbsaDivCols.put("cbsa", "Parent CBSA code");
    hudZipCbsaDivCols.put("cbsa_name", "Parent CBSA name");
    hudZipCbsaDivCols.put("res_ratio", "Proportion of residential addresses in this geographic relationship");
    hudZipCbsaDivCols.put("bus_ratio", "Proportion of business addresses in this geographic relationship");
    hudZipCbsaDivCols.put("oth_ratio", "Proportion of other addresses in this geographic relationship");
    hudZipCbsaDivCols.put("tot_ratio", "Total proportion of all addresses in this geographic relationship");
    hudZipCbsaDivCols.put("usps_city", "USPS-assigned city name for the ZIP code");
    hudZipCbsaDivCols.put("state_code", "2-letter state abbreviation");
    GEO_COLUMN_COMMENTS.put("hud_zip_cbsa_div", hudZipCbsaDivCols);
    
    // hud_zip_congressional table
    GEO_TABLE_COMMENTS.put("hud_zip_congressional",
        "HUD crosswalk mapping ZIP codes to Congressional Districts. Essential for political "
        + "analysis, constituent services, and connecting corporate/demographic data to "
        + "political representation. Updated for 119th Congress district boundaries.");
    
    Map<String, String> hudZipCongressionalCols = new HashMap<>();
    hudZipCongressionalCols.put("zip", "5-digit ZIP code");
    hudZipCongressionalCols.put("cd", "Congressional District number within state");
    hudZipCongressionalCols.put("cd_name", "Congressional District name");
    hudZipCongressionalCols.put("state_cd", "State-Congressional District code (SSDD format)");
    hudZipCongressionalCols.put("res_ratio", "Proportion of residential addresses in this district");
    hudZipCongressionalCols.put("bus_ratio", "Proportion of business addresses in this district");
    hudZipCongressionalCols.put("oth_ratio", "Proportion of other addresses in this district");
    hudZipCongressionalCols.put("tot_ratio", "Total proportion of all addresses in this district");
    hudZipCongressionalCols.put("usps_city", "USPS-assigned city name for the ZIP code");
    hudZipCongressionalCols.put("state_code", "2-letter state abbreviation");
    hudZipCongressionalCols.put("state_name", "Full state name");
    GEO_COLUMN_COMMENTS.put("hud_zip_congressional", hudZipCongressionalCols);
  }
  
  // =========================== ECON SCHEMA COMMENTS ===========================
  
  /** ECON schema table comments */
  private static final Map<String, String> ECON_TABLE_COMMENTS = new HashMap<>();
  
  /** ECON schema column comments */
  private static final Map<String, Map<String, String>> ECON_COLUMN_COMMENTS = new HashMap<>();
  
  static {
    // employment_statistics table
    ECON_TABLE_COMMENTS.put("employment_statistics",
        "U.S. employment and unemployment statistics from the Bureau of Labor Statistics (BLS). "
        + "Contains monthly data on unemployment rates, labor force participation, job openings, "
        + "and employment levels by sector. Essential for economic analysis and labor market trends.");
    
    Map<String, String> employmentStatsCols = new HashMap<>();
    employmentStatsCols.put("date", "Observation date (first day of month for monthly data)");
    employmentStatsCols.put("series_id", "BLS series identifier (e.g., 'LNS14000000' for unemployment rate)");
    employmentStatsCols.put("series_name", "Human-readable description of the data series");
    employmentStatsCols.put("value", "Metric value (percentage for rates, thousands for employment counts)");
    employmentStatsCols.put("unit", "Unit of measurement (percent, thousands, index)");
    employmentStatsCols.put("seasonally_adjusted", "Whether data is seasonally adjusted (true/false)");
    employmentStatsCols.put("percent_change_month", "Percent change from previous month");
    employmentStatsCols.put("percent_change_year", "Year-over-year percent change");
    employmentStatsCols.put("category", "Major category (Employment, Unemployment, Labor Force)");
    employmentStatsCols.put("subcategory", "Detailed subcategory (Manufacturing, Services, etc.)");
    ECON_COLUMN_COMMENTS.put("employment_statistics", employmentStatsCols);
    
    // inflation_metrics table
    ECON_TABLE_COMMENTS.put("inflation_metrics",
        "Consumer and Producer Price Index data from BLS tracking inflation across different "
        + "categories of goods and services. Includes CPI-U for urban consumers, CPI-W for wage earners, "
        + "and PPI for producer prices. Critical for understanding inflation trends and purchasing power.");
    
    Map<String, String> inflationMetricsCols = new HashMap<>();
    inflationMetricsCols.put("date", "Observation date for the index value");
    inflationMetricsCols.put("index_type", "Type of price index (CPI-U, CPI-W, PPI, etc.)");
    inflationMetricsCols.put("item_code", "BLS item code for specific good/service category");
    inflationMetricsCols.put("item_name", "Description of item or category (All items, Food, Energy, etc.)");
    inflationMetricsCols.put("index_value", "Index value with base period = 100");
    inflationMetricsCols.put("percent_change_month", "Month-over-month percent change");
    inflationMetricsCols.put("percent_change_year", "Year-over-year percent change (inflation rate)");
    inflationMetricsCols.put("area_code", "Geographic area code (U.S., regions, or metro areas)");
    inflationMetricsCols.put("area_name", "Geographic area name");
    inflationMetricsCols.put("seasonally_adjusted", "Whether data is seasonally adjusted");
    ECON_COLUMN_COMMENTS.put("inflation_metrics", inflationMetricsCols);
    
    // wage_growth table
    ECON_TABLE_COMMENTS.put("wage_growth",
        "Average earnings and compensation data by industry and occupation from BLS. "
        + "Tracks hourly and weekly earnings, employment cost index, and wage growth trends. "
        + "Essential for understanding labor cost pressures and income trends across sectors.");
    
    Map<String, String> wageGrowthCols = new HashMap<>();
    wageGrowthCols.put("date", "Observation date");
    wageGrowthCols.put("series_id", "BLS series identifier for wage data");
    wageGrowthCols.put("industry_code", "NAICS industry classification code");
    wageGrowthCols.put("industry_name", "Industry description");
    wageGrowthCols.put("occupation_code", "SOC occupation classification code");
    wageGrowthCols.put("occupation_name", "Occupation description");
    wageGrowthCols.put("average_hourly_earnings", "Average hourly earnings in dollars");
    wageGrowthCols.put("average_weekly_earnings", "Average weekly earnings in dollars");
    wageGrowthCols.put("employment_cost_index", "Employment cost index with base = 100");
    wageGrowthCols.put("percent_change_year", "Year-over-year percent change in earnings");
    ECON_COLUMN_COMMENTS.put("wage_growth", wageGrowthCols);
    
    // regional_employment table
    ECON_TABLE_COMMENTS.put("regional_employment",
        "State and metropolitan area employment statistics from BLS Local Area Unemployment Statistics "
        + "(LAUS). Provides unemployment rates, employment levels, and labor force participation by "
        + "geographic region. Critical for regional economic analysis and geographic comparisons.");
    
    Map<String, String> regionalEmploymentCols = new HashMap<>();
    regionalEmploymentCols.put("date", "Observation date");
    regionalEmploymentCols.put("area_code", "Geographic area code (FIPS or MSA code)");
    regionalEmploymentCols.put("area_name", "Geographic area name");
    regionalEmploymentCols.put("area_type", "Type of area (state, MSA, county)");
    regionalEmploymentCols.put("state_code", "Two-letter state abbreviation");
    regionalEmploymentCols.put("unemployment_rate", "Unemployment rate as percentage");
    regionalEmploymentCols.put("employment_level", "Number of employed persons");
    regionalEmploymentCols.put("labor_force", "Total labor force size");
    regionalEmploymentCols.put("participation_rate", "Labor force participation rate as percentage");
    regionalEmploymentCols.put("employment_population_ratio", "Employment to population ratio");
    ECON_COLUMN_COMMENTS.put("regional_employment", regionalEmploymentCols);
    
    // treasury_yields table
    ECON_TABLE_COMMENTS.put("treasury_yields",
        "Daily U.S. Treasury yield curve rates from Treasury Direct API. Provides yields for "
        + "various maturities from 1-month to 30-year securities. Essential benchmark for risk-free "
        + "rates, corporate bond pricing, and monetary policy analysis.");
    
    Map<String, String> treasuryYieldsCols = new HashMap<>();
    treasuryYieldsCols.put("date", "Observation date (YYYY-MM-DD)");
    treasuryYieldsCols.put("maturity_months", "Maturity period in months (1, 3, 6, 12, 24, 60, 120, 360)");
    treasuryYieldsCols.put("maturity_label", "Human-readable maturity (1M, 3M, 2Y, 10Y, 30Y)");
    treasuryYieldsCols.put("yield_percent", "Yield rate as annual percentage");
    treasuryYieldsCols.put("yield_type", "Type of yield (nominal, real/TIPS, average)");
    treasuryYieldsCols.put("source", "Data source (Treasury Direct)");
    ECON_COLUMN_COMMENTS.put("treasury_yields", treasuryYieldsCols);
    
    // federal_debt table
    ECON_TABLE_COMMENTS.put("federal_debt",
        "U.S. federal debt statistics from Treasury Fiscal Data API. Tracks daily debt levels "
        + "including debt held by public, intragovernmental holdings, and total outstanding debt. "
        + "Critical for fiscal policy analysis and understanding government financing needs.");
    
    Map<String, String> federalDebtCols = new HashMap<>();
    federalDebtCols.put("date", "Observation date");
    federalDebtCols.put("debt_type", "Type of debt measurement (Total, Held by Public, Intragovernmental)");
    federalDebtCols.put("amount_billions", "Debt amount in billions of dollars");
    federalDebtCols.put("percent_of_gdp", "Debt as percentage of GDP (when available)");
    federalDebtCols.put("holder_category", "Category of debt holder");
    federalDebtCols.put("debt_held_by_public", "Portion held by public in billions");
    federalDebtCols.put("intragovernmental_holdings", "Portion held by government accounts in billions");
    ECON_COLUMN_COMMENTS.put("federal_debt", federalDebtCols);
    
    // world_indicators table
    ECON_TABLE_COMMENTS.put("world_indicators",
        "International economic indicators from World Bank for major economies. Includes GDP, "
        + "inflation, unemployment, population, and government debt for G20 countries. Enables "
        + "international comparisons and global economic analysis.");
    
    Map<String, String> worldIndicatorsCols = new HashMap<>();
    worldIndicatorsCols.put("country_code", "ISO 3-letter country code (USA, CHN, DEU, etc.)");
    worldIndicatorsCols.put("country_name", "Full country name");
    worldIndicatorsCols.put("indicator_code", "World Bank indicator code (e.g., NY.GDP.MKTP.CD)");
    worldIndicatorsCols.put("indicator_name", "Indicator description");
    worldIndicatorsCols.put("year", "Observation year");
    worldIndicatorsCols.put("value", "Indicator value");
    worldIndicatorsCols.put("unit", "Unit of measurement (USD, percent, persons)");
    worldIndicatorsCols.put("scale", "Scale factor if applicable");
    ECON_COLUMN_COMMENTS.put("world_indicators", worldIndicatorsCols);
    
    // fred_indicators table
    ECON_TABLE_COMMENTS.put("fred_indicators",
        "Federal Reserve Economic Data (FRED) time series covering 800,000+ economic indicators. "
        + "Includes interest rates, monetary aggregates, exchange rates, commodity prices, and "
        + "economic activity measures. Primary source for U.S. economic time series data.");
    
    Map<String, String> fredIndicatorsCols = new HashMap<>();
    fredIndicatorsCols.put("series_id", "FRED series identifier (DFF, GDP, UNRATE, etc.)");
    fredIndicatorsCols.put("series_name", "Human-readable series description");
    fredIndicatorsCols.put("date", "Observation date");
    fredIndicatorsCols.put("value", "Observation value");
    fredIndicatorsCols.put("units", "Unit of measurement");
    fredIndicatorsCols.put("frequency", "Data frequency (D=Daily, W=Weekly, M=Monthly, Q=Quarterly, A=Annual)");
    fredIndicatorsCols.put("seasonal_adjustment", "Seasonal adjustment method if applicable");
    fredIndicatorsCols.put("last_updated", "Timestamp when series was last updated");
    ECON_COLUMN_COMMENTS.put("fred_indicators", fredIndicatorsCols);
    
    // gdp_components table
    ECON_TABLE_COMMENTS.put("gdp_components",
        "Detailed GDP components from Bureau of Economic Analysis (BEA) NIPA tables. Breaks down "
        + "GDP into personal consumption, investment, government spending, and net exports. Provides "
        + "granular view of economic activity drivers and sectoral contributions to growth.");
    
    Map<String, String> gdpComponentsCols = new HashMap<>();
    gdpComponentsCols.put("table_id", "BEA NIPA table identifier");
    gdpComponentsCols.put("line_number", "Line number within NIPA table");
    gdpComponentsCols.put("line_description", "Component description (e.g., Personal Consumption, Fixed Investment)");
    gdpComponentsCols.put("series_code", "BEA series code");
    gdpComponentsCols.put("year", "Observation year");
    gdpComponentsCols.put("value", "Component value in billions of dollars");
    gdpComponentsCols.put("units", "Unit of measurement (typically billions of dollars)");
    gdpComponentsCols.put("frequency", "Data frequency (A=Annual, Q=Quarterly, M=Monthly)");
    ECON_COLUMN_COMMENTS.put("gdp_components", gdpComponentsCols);
    
    // regional_income table
    ECON_TABLE_COMMENTS.put("regional_income",
        "State and regional personal income statistics from BEA Regional Economic Accounts. "
        + "Includes total personal income, per capita income, and population by state. Essential "
        + "for understanding regional economic disparities and income trends across states.");
    
    Map<String, String> regionalIncomeCols = new HashMap<>();
    regionalIncomeCols.put("geo_fips", "Geographic FIPS code");
    regionalIncomeCols.put("geo_name", "Geographic area name (state or region)");
    regionalIncomeCols.put("metric", "Metric type (Total Income, Per Capita Income, Population)");
    regionalIncomeCols.put("line_code", "BEA line code");
    regionalIncomeCols.put("line_description", "Detailed description of the metric");
    regionalIncomeCols.put("year", "Observation year");
    regionalIncomeCols.put("value", "Metric value (dollars or persons)");
    regionalIncomeCols.put("units", "Unit of measurement");
    ECON_COLUMN_COMMENTS.put("regional_income", regionalIncomeCols);
    
    // trade_statistics table
    ECON_TABLE_COMMENTS.put("trade_statistics",
        "Detailed U.S. export and import statistics from BEA NIPA Table T40205B. Provides "
        + "comprehensive breakdown of goods and services trade by category including foods, "
        + "industrial supplies, capital goods, automotive, and consumer goods. Includes calculated "
        + "trade balances for matching export/import pairs. Essential for trade policy analysis.");
    
    Map<String, String> tradeStatsCols = new HashMap<>();
    tradeStatsCols.put("table_id", "BEA NIPA table identifier (T40205B)");
    tradeStatsCols.put("line_number", "Line number within the NIPA table");
    tradeStatsCols.put("line_description", "Detailed description of trade category");
    tradeStatsCols.put("series_code", "BEA series code for this trade component");
    tradeStatsCols.put("year", "Observation year");
    tradeStatsCols.put("value", "Trade value in billions of dollars");
    tradeStatsCols.put("units", "Unit of measurement (billions of dollars)");
    tradeStatsCols.put("frequency", "Data frequency (A=Annual)");
    tradeStatsCols.put("trade_type", "Type of trade flow (Exports, Imports, or Other)");
    tradeStatsCols.put("category", "Parsed trade category (Goods, Services, Food, Capital Goods, etc.)");
    tradeStatsCols.put("trade_balance", "Calculated trade balance (exports minus imports) for category");
    ECON_COLUMN_COMMENTS.put("trade_statistics", tradeStatsCols);
    
    // ita_data table
    ECON_TABLE_COMMENTS.put("ita_data",
        "International Transactions Accounts (ITA) from BEA providing comprehensive balance "
        + "of payments statistics. Includes trade balance, current account balance, capital "
        + "account flows, and primary/secondary income balances. Critical for understanding "
        + "international financial flows and the U.S. position in global markets.");
    
    Map<String, String> itaDataCols = new HashMap<>();
    itaDataCols.put("indicator", "ITA indicator code (e.g., BalGds, BalCurrAcct, BalCapAcct)");
    itaDataCols.put("indicator_description", "Human-readable description of the indicator");
    itaDataCols.put("area_or_country", "Geographic scope (typically 'AllCountries' for aggregates)");
    itaDataCols.put("frequency", "Data frequency (A=Annual, Q=Quarterly)");
    itaDataCols.put("year", "Observation year");
    itaDataCols.put("value", "Balance value in millions of USD (negative = deficit)");
    itaDataCols.put("units", "Unit of measurement (USD Millions)");
    itaDataCols.put("time_series_id", "BEA time series identifier");
    itaDataCols.put("time_series_description", "Detailed time series description");
    ECON_COLUMN_COMMENTS.put("ita_data", itaDataCols);
    
    // industry_gdp table
    ECON_TABLE_COMMENTS.put("industry_gdp",
        "GDP by Industry data from BEA showing value added by NAICS industry sectors. "
        + "Provides comprehensive breakdown of economic output by industry including "
        + "agriculture, mining, manufacturing, services, and government sectors. Available "
        + "at both annual and quarterly frequencies for detailed sectoral analysis.");
    
    Map<String, String> industryGdpCols = new HashMap<>();
    industryGdpCols.put("table_id", "BEA GDPbyIndustry table identifier");
    industryGdpCols.put("frequency", "Data frequency (A=Annual, Q=Quarterly)");
    industryGdpCols.put("year", "Observation year");
    industryGdpCols.put("quarter", "Quarter for quarterly data (Q1-Q4) or year for annual");
    industryGdpCols.put("industry_code", "NAICS industry classification code");
    industryGdpCols.put("industry_description", "Industry sector description");
    industryGdpCols.put("value", "Value added by industry in billions of dollars");
    industryGdpCols.put("units", "Unit of measurement (billions of dollars)");
    industryGdpCols.put("note_ref", "Reference to explanatory notes if applicable");
    ECON_COLUMN_COMMENTS.put("industry_gdp", industryGdpCols);
  }
  
  // =========================== PUBLIC API METHODS ===========================
  
  /**
   * Gets the business definition comment for a SEC schema table.
   * 
   * @param tableName name of the table (case-insensitive)
   * @return table comment or null if not found
   */
  public static @Nullable String getSecTableComment(String tableName) {
    return SEC_TABLE_COMMENTS.get(tableName.toLowerCase());
  }
  
  /**
   * Gets the business definition comment for a SEC schema column.
   * 
   * @param tableName name of the table (case-insensitive)
   * @param columnName name of the column (case-insensitive)  
   * @return column comment or null if not found
   */
  public static @Nullable String getSecColumnComment(String tableName, String columnName) {
    Map<String, String> tableColumns = SEC_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return tableColumns != null ? tableColumns.get(columnName.toLowerCase()) : null;
  }
  
  /**
   * Gets all column comments for a SEC schema table.
   * 
   * @param tableName name of the table (case-insensitive)
   * @return map of column names to comments, or empty map if table not found
   */
  public static Map<String, String> getSecColumnComments(String tableName) {
    Map<String, String> comments = SEC_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return comments != null ? new HashMap<>(comments) : new HashMap<>();
  }
  
  /**
   * Gets the business definition comment for a GEO schema table.
   * 
   * @param tableName name of the table (case-insensitive)
   * @return table comment or null if not found
   */
  public static @Nullable String getGeoTableComment(String tableName) {
    return GEO_TABLE_COMMENTS.get(tableName.toLowerCase());
  }
  
  /**
   * Gets the business definition comment for a GEO schema column.
   * 
   * @param tableName name of the table (case-insensitive) 
   * @param columnName name of the column (case-insensitive)
   * @return column comment or null if not found
   */
  public static @Nullable String getGeoColumnComment(String tableName, String columnName) {
    Map<String, String> tableColumns = GEO_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return tableColumns != null ? tableColumns.get(columnName.toLowerCase()) : null;
  }
  
  /**
   * Gets all column comments for a GEO schema table.
   * 
   * @param tableName name of the table (case-insensitive)
   * @return map of column names to comments, or empty map if table not found
   */
  public static Map<String, String> getGeoColumnComments(String tableName) {
    Map<String, String> comments = GEO_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return comments != null ? new HashMap<>(comments) : new HashMap<>();
  }
  
  /**
   * Gets the business definition comment for an ECON schema table.
   * 
   * @param tableName name of the table (case-insensitive)
   * @return table comment or null if not found
   */
  public static @Nullable String getEconTableComment(String tableName) {
    return ECON_TABLE_COMMENTS.get(tableName.toLowerCase());
  }
  
  /**
   * Gets the business definition comment for an ECON schema column.
   * 
   * @param tableName name of the table (case-insensitive)
   * @param columnName name of the column (case-insensitive)
   * @return column comment or null if not found
   */
  public static @Nullable String getEconColumnComment(String tableName, String columnName) {
    Map<String, String> tableColumns = ECON_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return tableColumns != null ? tableColumns.get(columnName.toLowerCase()) : null;
  }
  
  /**
   * Gets all column comments for an ECON schema table.
   * 
   * @param tableName name of the table (case-insensitive)
   * @return map of column names to comments, or empty map if table not found
   */
  public static Map<String, String> getEconColumnComments(String tableName) {
    Map<String, String> comments = ECON_COLUMN_COMMENTS.get(tableName.toLowerCase());
    return comments != null ? new HashMap<>(comments) : new HashMap<>();
  }
}