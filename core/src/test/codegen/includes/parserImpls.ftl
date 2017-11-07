<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->


SqlAlter SqlUploadJarNode(Span s, String scope) :
{
    SqlNode jarPath;
    final List<SqlNode> jarPathsList;
}
{
    <UPLOAD> <JAR>
    jarPath = StringLiteral() {
        jarPathsList = startList(jarPath);
    }
    (
        <COMMA> jarPath = StringLiteral() {
            jarPathsList.add(jarPath);
        }
    )*
    {
        return new SqlUploadJarNode(s.end(this), scope, jarPathsList);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SqlIdentifier id;
    final SqlNodeList columnList;
}
{
    <TABLE> id = CompoundIdentifier() columnList = ExtendList() {
        return new SqlCreateTable(s.end(columnList), id, columnList);
    }
}

SqlNode SqlDescribeSpacePower() :
{
}
{
    <DESCRIBE> <SPACE> <POWER> {
        return null;
    }
}

// End parserImpls.ftl
