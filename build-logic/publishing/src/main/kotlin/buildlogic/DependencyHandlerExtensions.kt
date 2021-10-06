package buildlogic

import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.api.artifacts.component.ProjectComponentIdentifier
import org.gradle.api.artifacts.dsl.DependencyHandler
import org.gradle.api.artifacts.result.ResolvedVariantResult
import org.gradle.api.attributes.Category
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.project

/**
 * Converts the resolved result back to a dependency notation, so it can be resolved again.
 * Note: the conversion does not support "classifiers"
 */
fun DependencyHandler.reconstruct(variant: ResolvedVariantResult): Dependency {
    val category = variant.attributes.run {
        keySet().firstOrNull { it.name == Category.CATEGORY_ATTRIBUTE.name }?.let { getAttribute(it) }
    }

    val id = variant.owner
    return when (id) {
        is ProjectComponentIdentifier -> project(id.projectPath)
        is ModuleComponentIdentifier -> create(id.group, id.module, id.version)
        else -> throw IllegalArgumentException("Can't convert $id to dependency")
    }.let {
        when (category) {
            Category.REGULAR_PLATFORM -> platform(it)
            Category.ENFORCED_PLATFORM -> enforcedPlatform(it)
            Category.LIBRARY -> it
            else -> throw IllegalStateException("Unexpected dependency type $category for id $id")
        }
    }
}
