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
echo "================================================================================"
echo "REAL DOW 30 SEC EDGAR DOWNLOAD"
echo "Downloading all filings for 30 DJI companies for the last 10 years"
echo "================================================================================"
echo

# Set data directory
export SEC_DATA_DIR="/Volumes/T9/sec-data/dji-5year"
mkdir -p $SEC_DATA_DIR

echo "Data directory: $SEC_DATA_DIR"
echo

# Run the comprehensive DJI test with extended timeout
./gradlew :sec:test \
  --tests "org.apache.calcite.adapter.sec.Dow30SecAdapterTest.testRealDow30Download" \
  --info \
  --console=plain \
  -Dsec.data.home=$SEC_DATA_DIR \
  -Dsec.download.real=true \
  -Dsec.download.years=10 \
  -Dtest.timeout=7200
