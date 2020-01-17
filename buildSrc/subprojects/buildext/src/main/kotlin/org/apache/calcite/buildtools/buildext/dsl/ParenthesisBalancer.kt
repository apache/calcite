/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.calcite.buildtools.buildext.dsl

import java.util.function.Function

private const val SINGLE_LINE_COMMENT = "//.*+"
private const val MULTILINE_COMMENT = "/[*](?>\\\\[*]|[*][^/]|[^*])*+[*]/"
private const val STRING_LITERAL = "\"(?>\\\\.|[^\"])*+\""
private const val CHAR_LITERAL = "'(?>\\\\.|[^'])'"

private const val KEYWORDS = "\\b(?>for|if|return|switch|try|while)\\b"
private const val KEYWORD_BLOCK = "$KEYWORDS *\\("
private const val WHITESPACE = "(?:(?!$KEYWORDS|[(),\"'/]).)++"

// Below Regex matches one token at a time
// That is it breaks if (!canCastFrom(/*comment*/callBinding, throwOnFailure into the following sequence
// "if (", "!canCastFrom", "(", "/*comment*/", "callBinding", ",", " throwOnFailure"
// This enables to skip strings, comments, and capture the position of commas and parenthesis

private val tokenizer =
    Regex("(?>$SINGLE_LINE_COMMENT|$MULTILINE_COMMENT|$STRING_LITERAL|$CHAR_LITERAL|$KEYWORD_BLOCK|$WHITESPACE|.)")
private val looksLikeJavadoc = Regex("^ +\\* ")

// Note: if you change the logic, please remember to update the value in
// build.gradle.kts / bumpThisNumberIfACustomStepChanges
// Otherwise Autostyle would assume the files are up to date
object ParenthesisBalancer : Function<String, String> {
    override fun apply(v: String): String = v.lines().map { line ->
        if ('(' !in line || looksLikeJavadoc.containsMatchIn(line)) {
            return@map line
        }
        var balance = 0
        var seenOpen = false
        var commaSplit = 0
        var lastOpen = 0
        for (m in tokenizer.findAll(line)) {
            val range = m.range
            if (range.last - range.first > 1) {
                // parenthesis always take one char, so ignore long matches
                continue
            }
            val c = line[range.first]
            if (c == '(') {
                seenOpen = true
                if (balance == 0) {
                    lastOpen = range.first + 1
                }
                balance += 1
                continue
            } else if (!seenOpen) {
                continue
            }
            if (c == ',' && balance == 0) {
                commaSplit = range.first + 1
            }
            if (c == ')') {
                balance -= 1
            }
        }
        if (balance <= 1) {
            line
        } else {
            val indent = line.indexOfFirst { it != ' ' }
            val res = if (commaSplit == 0) {
                // f1(1,f2(2,...  pattern
                //    ^-- lastOpen, commaSplit=0 (no split)
                // It is split right after ('
                line.substring(0, lastOpen) + "\n" + " ".repeat(indent + 4) +
                        line.substring(lastOpen)
            } else {
                // f1(1), f2(2,...  pattern
                //       ^   ^-- lastOpen
                //        '-- commaSplit
                // It is split twice: right after the comma, and after (
                line.substring(0, commaSplit) +
                        "\n" + " ".repeat(indent) +
                        line.substring(commaSplit, lastOpen).trimStart(' ') +
                        "\n" + " ".repeat(indent + 4) + line.substring(lastOpen)
            }
            // println("---\n$line\n->\n$res")
            res
        }
    }.joinToString("\n")
}
