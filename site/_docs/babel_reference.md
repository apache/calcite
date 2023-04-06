---
layout: docs
title: SQL language (Babel)
permalink: /docs/babel_reference.html
---
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

<style>
.container {
  width: 400px;
  height: 26px;
}
.gray {
  width: 60px;
  height: 26px;
  background: gray;
  float: left;
}
.r15 {
  width: 40px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 10px;
}
.r12 {
  width: 10px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 10px;
}
.r13 {
  width: 20px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 10px;
}
.r2 {
  width: 2px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 20px;
}
.r24 {
  width: 20px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 20px;
}
.r35 {
  width: 20px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 30px;
}
</style>

The page describes SQL statements and expressions that are recognized by Calcite's Babel SQL parser,
in addition to those recognized by [Calcite's default SQL parser](reference.html).

## Grammar

SQL grammar in [BNF](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_Form)-like
form. This grammar covers only the modified or added statements in Calcite's babel SQL parser, referring to
[Calcite's default SQL grammar](reference.html).

{% highlight sql %}
statement:
      setStatement
  |   resetStatement
  |   begin
  |   commit
  |   rollback
  |   discard
  |   explain
  |   describe
  |   insert
  |   update
  |   merge
  |   delete
  |   query

begin:
      BEGIN [ { WORK | TRANSACTION } ] [ transactionMode [, transactionMode]* ]

commit:
      COMMIT [ { WORK | TRANSACTION } ] [ AND [ NO ] CHAIN ]

rollback:
      ROLLBACK [ { WORK | TRANSACTION } ] [ AND [ NO ] CHAIN ]

discard:
      DISCARD { ALL | PLANS | SEQUENCES | TEMPORARY | TEMP }

setStatement:
      [ ALTER { SYSTEM | SESSION } ] SET [ { SESSION | LOCAL } ]
      {
          identifier { TO | = } { expression [, expression]*  | DEFAULT }
      |   TIME ZONE { expression | 'value' | LOCAL | DEFAULT }
      |   (SCHEMA | NAMES | SEED) expression
      |   TRANSACTION transactionMode [, transactionMode]*
      |   TRANSACTION SNAPSHOT expression
      |   SESSION CHARACTERISTICS AS TRANSACTION transactionMode [, transactionMode]*
      |   SESSION AUTHORIZATION expression
      |   ROLE { NONE | expression }
      }
}

resetStatement:
      RESET { ALL | SESSION AUTHORIZATION | TIME ZONE | TRANSACTION ISOLATION LEVEL | identifier }

transactionMode:
      ISOLATION LEVEL { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }
  |   READ WRITE
  |   READ ONLY
  |   [ NOT ] DEFERRABLE
{% endhighlight %}
