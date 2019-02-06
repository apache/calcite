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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link RelMetadataQuery#getTableReferences} for the
 * standard logical algebra.
 *
 * <p>The goal of this provider is to return all tables used by a given
 * expression identified uniquely by a {@link RelTableRef}.
 *
 * <p>Each unique identifier {@link RelTableRef} of a table will equal to the
 * identifier obtained running {@link RelMdExpressionLineage} over the same plan
 * node for an expression that refers to the same table.
 *
 * <p>If tables cannot be obtained, we return null.
 */
public class RelMdTableReferences
    implements MetadataHandler<BuiltInMetadata.TableReferences> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.TABLE_REFERENCES.method, new RelMdTableReferences());

  //~ Constructors -----------------------------------------------------------

  protected RelMdTableReferences() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.TableReferences> getDef() {
    return BuiltInMetadata.TableReferences.DEF;
  }

  // Catch-all rule when none of the others apply.
  public Set<RelTableRef> getTableReferences(RelNode rel, RelMetadataQuery mq) {
    return null;
  }

  public Set<RelTableRef> getTableReferences(HepRelVertex rel, RelMetadataQuery mq) {
    return mq.getTableReferences(rel.getCurrentRel());
  }

  public Set<RelTableRef> getTableReferences(RelSubset rel, RelMetadataQuery mq) {
    return mq.getTableReferences(Util.first(rel.getBest(), rel.getOriginal()));
  }

  /**
   * TableScan table reference.
   */
  public Set<RelTableRef> getTableReferences(TableScan rel, RelMetadataQuery mq) {
    return ImmutableSet.of(RelTableRef.of(rel.getTable(), 0));
  }

  /**
   * Table references from Aggregate.
   */
  public Set<RelTableRef> getTableReferences(Aggregate rel, RelMetadataQuery mq) {
    return mq.getTableReferences(rel.getInput());
  }

  /**
   * Table references from Join.
   */
  public Set<RelTableRef> getTableReferences(Join rel, RelMetadataQuery mq) {
    final RelNode leftInput = rel.getLeft();
    final RelNode rightInput = rel.getRight();
    final Set<RelTableRef> result = new HashSet<>();

    // Gather table references, left input references remain unchanged
    final Multimap<List<String>, RelTableRef> leftQualifiedNamesToRefs = HashMultimap.create();
    final Set<RelTableRef> leftTableRefs = mq.getTableReferences(leftInput);
    if (leftTableRefs == null) {
      // We could not infer the table refs from left input
      return null;
    }
    for (RelTableRef leftRef : leftTableRefs) {
      assert !result.contains(leftRef);
      result.add(leftRef);
      leftQualifiedNamesToRefs.put(leftRef.getQualifiedName(), leftRef);
    }

    // Gather table references, right input references might need to be
    // updated if there are table names clashes with left input
    final Set<RelTableRef> rightTableRefs = mq.getTableReferences(rightInput);
    if (rightTableRefs == null) {
      // We could not infer the table refs from right input
      return null;
    }
    for (RelTableRef rightRef : rightTableRefs) {
      int shift = 0;
      Collection<RelTableRef> lRefs = leftQualifiedNamesToRefs.get(rightRef.getQualifiedName());
      if (lRefs != null) {
        shift = lRefs.size();
      }
      RelTableRef shiftTableRef = RelTableRef.of(
          rightRef.getTable(), shift + rightRef.getEntityNumber());
      assert !result.contains(shiftTableRef);
      result.add(shiftTableRef);
    }

    // Return result
    return result;
  }

  /**
   * Table references from {@link Union}.
   *
   * <p>For Union operator, we might be able to extract multiple table
   * references.
   */
  public Set<RelTableRef> getTableReferences(Union rel, RelMetadataQuery mq) {
    final Set<RelTableRef> result = new HashSet<>();

    // Infer column origin expressions for given references
    final Multimap<List<String>, RelTableRef> qualifiedNamesToRefs = HashMultimap.create();
    for (RelNode input : rel.getInputs()) {
      final Map<RelTableRef, RelTableRef> currentTablesMapping = new HashMap<>();
      final Set<RelTableRef> inputTableRefs = mq.getTableReferences(input);
      if (inputTableRefs == null) {
        // We could not infer the table refs from input
        return null;
      }
      for (RelTableRef tableRef : inputTableRefs) {
        int shift = 0;
        Collection<RelTableRef> lRefs = qualifiedNamesToRefs.get(
            tableRef.getQualifiedName());
        if (lRefs != null) {
          shift = lRefs.size();
        }
        RelTableRef shiftTableRef = RelTableRef.of(
            tableRef.getTable(), shift + tableRef.getEntityNumber());
        assert !result.contains(shiftTableRef);
        result.add(shiftTableRef);
        currentTablesMapping.put(tableRef, shiftTableRef);
      }
      // Add to existing qualified names
      for (RelTableRef newRef : currentTablesMapping.values()) {
        qualifiedNamesToRefs.put(newRef.getQualifiedName(), newRef);
      }
    }

    // Return result
    return result;
  }

  /**
   * Table references from Project.
   */
  public Set<RelTableRef> getTableReferences(Project rel, final RelMetadataQuery mq) {
    return mq.getTableReferences(rel.getInput());
  }

  /**
   * Table references from Filter.
   */
  public Set<RelTableRef> getTableReferences(Filter rel, RelMetadataQuery mq) {
    return mq.getTableReferences(rel.getInput());
  }

  /**
   * Table references from Sort.
   */
  public Set<RelTableRef> getTableReferences(Sort rel, RelMetadataQuery mq) {
    return mq.getTableReferences(rel.getInput());
  }

  /**
   * Table references from Exchange.
   */
  public Set<RelTableRef> getTableReferences(Exchange rel, RelMetadataQuery mq) {
    return mq.getTableReferences(rel.getInput());
  }
}

// End RelMdTableReferences.java
