package buildlogic

import org.apache.tools.ant.filters.FixCrLfFilter
import org.gradle.api.file.CopySpec
import org.gradle.kotlin.dsl.filter

fun CopySpec.filterEolSimple(eol: String) {
    filteringCharset = "UTF-8"
    filter(
        FixCrLfFilter::class, mapOf(
            "eol" to FixCrLfFilter.CrLf.newInstance(eol),
            "fixlast" to true,
            "ctrlz" to FixCrLfFilter.AddAsisRemove.newInstance("asis")
        )
    )
}
