#!/bin/bash
echo "=================================================================================="
echo "TESTING DJI WIKIPEDIA SCRAPER DIRECTLY"
echo "=================================================================================="

cd /Users/kennethstott/calcite/sec

# Compile the test
javac -cp "build/classes/java/main:../build/libs/*:../file/build/libs/*:../core/build/libs/*:../linq4j/build/libs/*" TestRealDJIScraper.java

# Run the test with all dependencies  
java -cp ".:build/classes/java/main:../build/libs/*:../file/build/libs/*:../core/build/libs/*:../linq4j/build/libs/*:../avatica/build/libs/*" \
     -Dlog4j.configuration=file:src/test/resources/log4j.properties \
     TestRealDJIScraper