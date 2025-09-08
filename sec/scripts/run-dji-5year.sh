#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
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
