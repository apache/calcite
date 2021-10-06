package buildlogic

import org.gradle.api.artifacts.Configuration
import org.gradle.api.attributes.Category
import org.gradle.api.attributes.Usage
import org.gradle.api.model.ObjectFactory
import org.gradle.kotlin.dsl.named

fun Configuration.javaLibrary(objects: ObjectFactory, usage: String) =
    attributes {
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(usage))
    }

fun Configuration.javaLibraryApi(objects: ObjectFactory) =
    javaLibrary(objects, Usage.JAVA_API)

fun Configuration.javaLibraryRuntime(objects: ObjectFactory) =
    javaLibrary(objects, Usage.JAVA_RUNTIME)
