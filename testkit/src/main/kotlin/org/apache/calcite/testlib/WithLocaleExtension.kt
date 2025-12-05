/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.testlib

import org.apache.calcite.test.Unsafe
import org.apache.calcite.testlib.annotations.WithLocale
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ExtensionContext.Namespace
import org.junit.platform.commons.support.AnnotationSupport
import java.util.Locale

/**
 * Enables to set a locale per test class or test method by adding [WithLex] annotation.
 *
 * It might be useful in case the test depends on decimal separators (1,000 vs 1 000),
 * translated messages, and so on.
 */
class WithLocaleExtension : BeforeAllCallback, AfterAllCallback, BeforeEachCallback {
    companion object {
        private val NAMESPACE = Namespace.create(WithLocaleExtension::class)
        private const val DEFAULT_LOCALE = "localeBeforeClass"
        private const val CLASS_LOCALE = "classLocale"
    }

    private val ExtensionContext.store: ExtensionContext.Store get() = getStore(NAMESPACE)

    override fun beforeAll(context: ExtensionContext) {
        val defaultLocale = Locale.getDefault()
        context.store.put(DEFAULT_LOCALE, defaultLocale)
        // Save the value of WithLocale if it is present at the class level
        context.element
            .flatMap { AnnotationSupport.findAnnotation(it, WithLocale::class.java) }
            .map { Locale.Builder().setLanguageTag(it.country + '-' + it.country +
                    if (it.variant.isBlank()) "" else '-' + it.variant).build() }
            .orElseGet { defaultLocale }
            .let { context.store.put(CLASS_LOCALE, it) }
    }

    override fun afterAll(context: ExtensionContext) {
        // Restore the original Locale
        context.store.get(DEFAULT_LOCALE, Locale::class.java)?.let {
            Unsafe.setDefaultLocale(it)
        }
    }

    override fun beforeEach(context: ExtensionContext) {
        // Set locale based on
        context.element
            .flatMap { AnnotationSupport.findAnnotation(it, WithLocale::class.java) }
            .map { Locale.Builder().setLanguageTag(it.country + '-' + it.country +
                    if (it.variant.isBlank()) "" else '-' + it.variant).build() }
            .orElseGet { context.store.get(CLASS_LOCALE, Locale::class.java) }
            ?.let { Unsafe.setDefaultLocale(it) }
    }
}
