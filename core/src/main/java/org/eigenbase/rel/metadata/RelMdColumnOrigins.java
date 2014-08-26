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
package org.eigenbase.rel.metadata;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

import net.hydromatic.optiq.BuiltinMethod;

/**
 * RelMdColumnOrigins supplies a default implementation of {@link
 * RelMetadataQuery#getColumnOrigins} for the standard logical algebra.
 */
public class RelMdColumnOrigins {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltinMethod.COLUMN_ORIGIN.method, new RelMdColumnOrigins());

  //~ Constructors -----------------------------------------------------------

  private RelMdColumnOrigins() {}

  //~ Methods ----------------------------------------------------------------

  public Set<RelColumnOrigin> getColumnOrigins(
      AggregateRelBase rel,
      int iOutputColumn) {
    if (iOutputColumn < rel.getGroupCount()) {
      // Group columns pass through directly.
      return invokeGetColumnOrigins(
          rel.getChild(),
          iOutputColumn);
    }

    // Aggregate columns are derived from input columns
    AggregateCall call =
        rel.getAggCallList().get(iOutputColumn - rel.getGroupCount());

    Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();
    for (Integer iInput : call.getArgList()) {
      Set<RelColumnOrigin> inputSet =
          invokeGetColumnOrigins(
              rel.getChild(), iInput);
      inputSet = createDerivedColumnOrigins(inputSet);
      if (inputSet != null) {
        set.addAll(inputSet);
      }
    }
    return set;
  }

  public Set<RelColumnOrigin> getColumnOrigins(
      JoinRelBase rel,
      int iOutputColumn) {
    int nLeftColumns = rel.getLeft().getRowType().getFieldList().size();
    Set<RelColumnOrigin> set;
    boolean derived = false;
    if (iOutputColumn < nLeftColumns) {
      set =
          invokeGetColumnOrigins(
              rel.getLeft(),
              iOutputColumn);
      if (rel.getJoinType().generatesNullsOnLeft()) {
        derived = true;
      }
    } else {
      set =
          invokeGetColumnOrigins(
              rel.getRight(),
              iOutputColumn - nLeftColumns);
      if (rel.getJoinType().generatesNullsOnRight()) {
        derived = true;
      }
    }
    if (derived) {
      // nulls are generated due to outer join; that counts
      // as derivation
      set = createDerivedColumnOrigins(set);
    }
    return set;
  }

  public Set<RelColumnOrigin> getColumnOrigins(
      SetOpRel rel,
      int iOutputColumn) {
    Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();
    for (RelNode input : rel.getInputs()) {
      Set<RelColumnOrigin> inputSet =
          invokeGetColumnOrigins(
              input,
              iOutputColumn);
      if (inputSet == null) {
        return null;
      }
      set.addAll(inputSet);
    }
    return set;
  }

  public Set<RelColumnOrigin> getColumnOrigins(
      ProjectRelBase rel,
      int iOutputColumn) {
    final RelNode child = rel.getChild();
    RexNode rexNode = rel.getProjects().get(iOutputColumn);

    if (rexNode instanceof RexInputRef) {
      // Direct reference:  no derivation added.
      RexInputRef inputRef = (RexInputRef) rexNode;
      return invokeGetColumnOrigins(
          child,
          inputRef.getIndex());
    }

    // Anything else is a derivation, possibly from multiple
    // columns.
    final Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();
    RexVisitor visitor =
        new RexVisitorImpl<Void>(true) {
          public Void visitInputRef(RexInputRef inputRef) {
            Set<RelColumnOrigin> inputSet =
                invokeGetColumnOrigins(
                    child,
                    inputRef.getIndex());
            if (inputSet != null) {
              set.addAll(inputSet);
            }
            return null;
          }
        };
    rexNode.accept(visitor);

    return createDerivedColumnOrigins(set);
  }

  public Set<RelColumnOrigin> getColumnOrigins(
      FilterRelBase rel,
      int iOutputColumn) {
    return invokeGetColumnOrigins(
        rel.getChild(),
        iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(
      SortRel rel,
      int iOutputColumn) {
    return invokeGetColumnOrigins(
        rel.getChild(),
        iOutputColumn);
  }

  public Set<RelColumnOrigin> getColumnOrigins(
      TableFunctionRelBase rel,
      int iOutputColumn) {
    Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();
    Set<RelColumnMapping> mappings = rel.getColumnMappings();
    if (mappings == null) {
      if (rel.getInputs().size() > 0) {
        // This is a non-leaf transformation:  say we don't
        // know about origins, because there are probably
        // columns below.
        return null;
      } else {
        // This is a leaf transformation: say there are fer sure no
        // column origins.
        return set;
      }
    }
    for (RelColumnMapping mapping : mappings) {
      if (mapping.iOutputColumn != iOutputColumn) {
        continue;
      }
      Set<RelColumnOrigin> origins =
          invokeGetColumnOrigins(
              rel.getInputs().get(mapping.iInputRel),
              mapping.iInputColumn);
      if (origins == null) {
        return null;
      }
      if (mapping.derived) {
        origins = createDerivedColumnOrigins(origins);
      }
      set.addAll(origins);
    }
    return set;
  }

  // Catch-all rule when none of the others apply.
  public Set<RelColumnOrigin> getColumnOrigins(
      RelNode rel,
      int iOutputColumn) {
    // NOTE jvs 28-Mar-2006: We may get this wrong for a physical table
    // expression which supports projections.  In that case,
    // it's up to the plugin writer to override with the
    // correct information.

    if (rel.getInputs().size() > 0) {
      // No generic logic available for non-leaf rels.
      return null;
    }

    Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();

    RelOptTable table = rel.getTable();
    if (table == null) {
      // Somebody is making column values up out of thin air, like a
      // VALUES clause, so we return an empty set.
      return set;
    }

    // Detect the case where a physical table expression is performing
    // projection, and say we don't know instead of making any assumptions.
    // (Theoretically we could try to map the projection using column
    // names.)  This detection assumes the table expression doesn't handle
    // rename as well.
    if (table.getRowType() != rel.getRowType()) {
      return null;
    }

    set.add(new RelColumnOrigin(table, iOutputColumn, false));
    return set;
  }

  protected Set<RelColumnOrigin> invokeGetColumnOrigins(
      RelNode rel,
      int iOutputColumn) {
    return RelMetadataQuery.getColumnOrigins(rel, iOutputColumn);
  }

  private Set<RelColumnOrigin> createDerivedColumnOrigins(
      Set<RelColumnOrigin> inputSet) {
    if (inputSet == null) {
      return null;
    }
    Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();
    for (RelColumnOrigin rco : inputSet) {
      RelColumnOrigin derived =
          new RelColumnOrigin(
              rco.getOriginTable(),
              rco.getOriginColumnOrdinal(),
              true);
      set.add(derived);
    }
    return set;
  }
}

// End RelMdColumnOrigins.java
