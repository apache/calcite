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
[![Travis Build Status](https://travis-ci.com/yunpengn/calcite.svg?branch=master)](https://travis-ci.com/yunpengn/calcite)
[![AppVeyor Build status](https://ci.appveyor.com/api/projects/status/qo30vjfl2rwsapnx?svg=true)](https://ci.appveyor.com/project/yunpengn/calcite)

# Apache Calcite

This is a forked version of the [Apache Calcite](http://calcite.apache.org) framework, with enhancements on outer join reorderability. _We do NOT guarantee compatibility with its upstream version._

This [repository](https://github.com/yunpengn/calcite) is currently maintained by **[Yunpeng Niu](https://github.com/yunpengn)**.

## Development Environment Setup

- Install the latest version of [IntelliJ IDEA](https://www.jetbrains.com/idea/) by [JetBrains](https://www.jetbrains.com/).
- Clone the repository by `git clone git@github.com:yunpengn/calcite.git`.
- Navigate to the cloned folder by `cd calcite/`.
- Import all dependencies by `./mvnw -DskipTests clean install`.
    - This step may take a long time and need stable Internet connection. Please be patient.
- Open the IDE and import the project.
- Start coding!

## Licence

[Apache Licence 2.0](LICENSE)
