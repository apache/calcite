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

# Calcite native connector for Tableau

A connector that allows Tableau to connect to Apache Calcite via
JDBC. It uses the
[Tableau Connector SDK](https://github.com/tableau/connector-plugin-sdk)
to build what looks like a native named data source in Tableau.

The parts of the connector are:
 * `connector-dialog.tcd` - The connection dialog UI for Tableau.
 * `connectionBuilder.js` - The script that creates the JDBC
   connection URL from the properties passed from the connection
   dialog.
 * `connectionRequired.js` - Indicates which properties are
   required to make a connection
 * `connectionResolver.tdr` - Indicates to Tableau the other
   files to be used in the connector
 * `dialect.tdd` - File providing the mapping from Tableau's AST to
   Calcite's SQL dialect
 * `manifest.xml` - The top-level file giving driver metadata

To run the connector in development, see
https://tableau.github.io/connector-plugin-sdk/docs/run-taco.

To test the connector in development, see
https://tableau.github.io/connector-plugin-sdk/docs/tdvt.

To package the driver for shipment, see
https://tableau.github.io/connector-plugin-sdk/docs/package-sign.

To manually sign the file, you can package the file as unsigned and
then run:
```
jarsigner -keystore NONE -storetype PKCS11 -tsa <tsaUrl> -providerClass sun.security.pkcs11.SunPKCS11 -providerArg <dir>/pkcs11.cfg packaged-connector/calcite.taco 'Certificate for Digital Signature' -certchain fullchain.pem
```

Additional information, including samples, can be found at https://github.com/tableau/connector-plugin-sdk

Based on
[Tableau's sample Postgres JDBC connector](https://github.com/tableau/connector-plugin-sdk/tree/master/samples/plugins/postgres_jdbc)
and
[Dremio's Tableau connector](https://github.com/dremio-hub/tableau-connector).
