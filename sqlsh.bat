@echo off
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

:: sqlline.bat - Windows script to launch SQL shell
:: Example:
:: > sqlline.bat
:: sqlline> !connect jdbc:calcite: admin admin

:: Copy dependency jars on first call.
:: To force jar refresh, remove core\target\dependencies)
if not exist build\libs\sqllineClasspath.jar (call gradlew buildSqllineClasspath)

java -Xmx1G -cp build\libs\sqllineClasspath.jar org.apache.calcite.adapter.os.SqlShell %*

:: End sqlline.bat
