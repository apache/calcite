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



// /** Parses the infix "::" cast operator used in PostgreSQL. */
// void InfixCast(List<Object> list, ExprContext exprContext, Span s) :
// {
//     final SqlDataTypeSpec dt;
// }
// {
//     <INFIX_CAST> {
//         checkNonQueryExpression(exprContext);
//     }
//     dt = DataType() {
//         list.add(
//             new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.INFIX_CAST,
//                 s.pos()));
//         list.add(dt);
//     }
// }


/** Parses the NULL-safe "<=>" equal operator used in MySQL. */
// void NullSafeEqual(List<Object> list, ExprContext exprContext, Span s):
// {
// }
// {
//     <NULL_SAFE_EQUAL> {
//         checkNonQueryExpression(exprContext);
//         list.add(new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.NULL_SAFE_EQUAL, getPos()));
//     }
//     Expression2b(ExprContext.ACCEPT_SUB_QUERY, list)
// }
