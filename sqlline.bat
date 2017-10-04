@echo off
:: sqlline.bat - Windows script to launch SQL shell
::
:: Licensed to the Apache Software Foundation (ASF) under one or more
:: contributor license agreements.  See the NOTICE file distributed with
:: this work for additional information regarding copyright ownership.
:: The ASF licenses this file to you under the Apache License, Version 2.0
:: (the "License"); you may not use this file except in compliance with
:: the License.  You may obtain a copy of the License at
::
:: http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing, software
:: distributed under the License is distributed on an "AS IS" BASIS,
:: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
:: See the License for the specific language governing permissions and
:: limitations under the License.
::
:: Example:
:: > sqlline.bat
:: sqlline> !connect jdbc:calcite: admin admin 

if "%1" == "" (
  :: By default assume users want to use ES5
  set EXT_CP="elasticsearch5\target\dependencies\*"
  goto run
)
if "%1" == "es2" (
  set EXT_CP="elasticsearch2\target\dependencies\*"
  goto run
)
if "%1" == "es5" (
  set EXT_CP="elasticsearch5\target\dependencies\*"
  goto run
)

echo "Invalid argument: %1"
exit 1

:run
shift
:: Copy dependency jars on first call. (To force jar refresh, remove target\dependencies)
if not exist target\dependencies (call mvn -B dependency:copy-dependencies -DoverWriteReleases=false -DoverWriteSnapshots=false -DoverWriteIfNewer=true -DoutputDirectory=target\dependencies)

java -Xmx1G -cp ".\target\dependencies\*;core\target\dependencies\*;cassandra\target\dependencies\*;druid\target\dependencies\*;elasticsearch2\target\dependencies\*;elasticsearch5\target\dependencies\*;file\target\dependencies\*;mongodb\target\dependencies\*;spark\target\dependencies\*;splunk\target\dependencies\*;%EXT_CP%" sqlline.SqlLine --verbose=true %1 %2 %3 %4 %5 %6 %7 %8 %9

:: End sqlline.bat
