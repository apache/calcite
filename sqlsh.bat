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

:: sqlsh.bat - Windows script to launch SQL shell
:: Example:
:: > sqlsh.bat select * from du order by 1 limit 3

:: The script updates the classpath on each execution,
:: You might add CACHE_SQLLINE_CLASSPATH environment variable to cache it
:: To build classpath jar manually use gradlew buildSqllineClasspath
set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set CP=%DIRNAME%\build\libs\sqllineClasspath.jar

if not defined CACHE_SQLLINE_CLASSPATH (
  if exist "%CP%" del "%CP%"
)
if not exist "%CP%" (call "%DIRNAME%\gradlew" --console plain -q :buildSqllineClasspath)
:: Running sqlsh is explicitly for OS adapter, so we can set the allowed classes to avoid security exception when loading the model file.
set JAVA_OPTS=-Djavax.xml.parsers.DocumentBuilderFactory=com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl -Dcalcite.model.classes.allowed=org.apache.calcite.adapter.os. %JAVA_OPTS%

java -Xmx1g -cp "%CP%" %JAVA_OPTS% org.apache.calcite.adapter.os.SqlShell %*
