---
layout: news_item
title: "Splitting Avatica from Calcite"
date: "2016-03-03 23:57:33 -0500"
author: elserj
categories: [milestones]
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

This marks the separation of Avatica from its previous location as a sub-module
of Apache Calcite's Maven build.

This separation is not to remove Avatica from
the governance of the Apache Calcite project, but to allow for even more rapid
releases from both the Avatica and Calcite projects. We can confidently make new
releases of each without having to worry about the current state of development
features in the other.
