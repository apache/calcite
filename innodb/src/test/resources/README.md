<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# How to generate binary files for test

There are binary files (*.ibd) for test in `innodb/src/test/resources/data`, they are innodb data files generated by MySQL server.  Unit tests are based on reading the binary files to check `innodb-adapter` functionality. Because it is too hard to generate those files manually, so we have to include the binary files in the source code.

These binary files are:

```
innodb/src/test/resources/data/DEPT.ibd
innodb/src/test/resources/data/EMP.ibd
innodb/src/test/resources/data/test_types.ibd
```

You can recreate `DEPT.ibd` and `EMP.ibd` by the following steps.
1. Make sure a MySQL server is running, follow the [Prerequisites](#Prerequisites). Note that MySQL version should be 5.7, 8.0 or higher.
2. Run `mysql -u<username> -p<password> -h<hostname> -P<port> <dbname> < innodb/src/test/resources/scott.sql`.
3. Copy `DEPT.ibd` and `EMP.ibd` from MySQL data directory (for example, `/usr/local/mysql/data/<dbname>`).

You can recreate `test_types.ibd` by the following steps.
1. Make sure a MySQL server is running, follow the [Prerequisites](#Prerequisites). Note that MySQL version should be 5.7, please do not use 8.0 since there is a limitation in `innodb-java-reader`, which does not support TEXT/BLOB yet (will be supported in the future plan).
2. Run `mysql -u<username> -p<password> -h<hostname> -P<port> <dbname> < innodb/src/test/resources/data_types.sql`.
3. Copy `test_types.ibd` from MySQL data directory (for example, `/usr/local/mysql/data/<dbname>`).

# Prerequisites
* `innodb_file_per_table` should set to `ON`, `innodb_file_per_table` is enabled by default in MySQL 5.6 and higher.
* Page size should set to `16K` which is also the default value.
