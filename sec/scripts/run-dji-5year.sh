#!/bin/bash

echo "=================================================================================="
echo "TESTING DJI 5-YEAR SEC FILINGS DOWNLOAD"
echo "=================================================================================="
echo
echo "This will download SEC filings for all 30 Dow Jones Industrial Average companies"
echo "for the years 2020-2024. This may take several minutes due to SEC rate limits."
echo
echo "Data will be stored in: /Volumes/T9/sec-data/dji-5year"
echo "=================================================================================="
echo

cd /Users/kennethstott/calcite

# Build the SEC adapter
echo "Building SEC adapter..."
./gradlew :sec:build -x test

echo
echo "Running DJI 5-year download test..."
echo

# The test uses the model from /sec/src/main/resources/dji-5year-model.json
./gradlew :sec:test --tests "org.apache.calcite.adapter.sec.DJI5YearTest.testDJI5YearFilings" -PincludeTags=integration --console=plain