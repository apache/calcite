/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;

import net.hydromatic.linq4j.Ord;


/**
 * <code>ProjectRelBase</code> is an abstract base class for implementations of
 * {@link ProjectRel}.
 *
 * @version $Id$
 * @author jhyde
 * @since March, 2004
 */
public abstract class ProjectRelBase
    extends SingleRel
{
    //~ Instance fields --------------------------------------------------------

    protected RexNode [] exps;

    /**
     * Values defined in {@link Flags}.
     */
    protected int flags;

    private final List<RelCollation> collationList;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Project.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits traits of this rel
     * @param child input relational expression
     * @param exps set of expressions for the input columns
     * @param rowType output row type
     * @param flags values as in {@link Flags}
     * @param collationList List of sort keys
     */
    protected ProjectRelBase(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RexNode [] exps,
        RelDataType rowType,
        int flags,
        final List<RelCollation> collationList)
    {
        super(cluster, traits, child);
        assert rowType != null;
        assert collationList != null;
        this.exps = exps;
        this.rowType = rowType;
        this.flags = flags;
        this.collationList =
            collationList.isEmpty() ? Collections.<RelCollation>emptyList()
            : collationList;
        assert isValid(true);
    }

    //~ Methods ----------------------------------------------------------------

    public List<RelCollation> getCollationList()
    {
        return collationList;
    }

    public boolean isBoxed()
    {
        return (flags & Flags.Boxed) == Flags.Boxed;
    }

    // override AbstractRelNode
    public RexNode [] getChildExps()
    {
        return getProjectExps();
    }

    /**
     * Returns the project expressions.
     */
    public RexNode [] getProjectExps()
    {
        return exps;
    }

    public List<RexNode> getProjectExpList() {
        return Arrays.asList(exps);
    }

    public int getFlags()
    {
        return flags;
    }

    public boolean isValid(boolean fail)
    {
        if (!super.isValid(fail)) {
            assert !fail;
            return false;
        }
        if (!RexUtil.compatibleTypes(
                exps,
                getRowType(),
                true))
        {
            assert !fail;
            return false;
        }
        RexChecker checker =
            new RexChecker(
                getChild().getRowType(), fail);
        for (RexNode exp : exps) {
            exp.accept(checker);
        }
        if (checker.getFailureCount() > 0) {
            assert !fail;
            return false;
        }
        if (!isBoxed()) {
            if (exps.length != 1) {
                assert !fail;
                return false;
            }
        }
        if (collationList == null) {
            assert !fail;
            return false;
        }
        return true;
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = RelMetadataQuery.getRowCount(getChild());
        double dCpu = dRows * exps.length;
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }

    public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
        super.explainTerms(pw);
        for (Ord<RelDataTypeField> field : Ord.zip(rowType.getFields())) {
            String fieldName = field.e.getName();
            if (fieldName == null) {
                fieldName = "field#" + field.i;
            }
            pw.item(fieldName, exps[field.i]);
        }

        // If we're generating a digest, include the rowtype. If two projects
        // differ in return type, we don't want to regard them as equivalent,
        // otherwise we will try to put rels of different types into the same
        // planner equivalence set.
        if ((pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
            && false)
        {
            pw.item("type", rowType);
        }

        return pw;
    }

    //~ Inner Interfaces -------------------------------------------------------

    public interface Flags
    {
        int AnonFields = 2;

        /**
         * Whether the resulting row is to be a synthetic class whose fields are
         * the aliases of the fields. <code>boxed</code> must be true unless
         * there is only one field: <code>select {dept.deptno} from dept</code>
         * is boxed, <code>select dept.deptno from dept</code> is not.
         */
        int Boxed = 1;
        int None = 0;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Visitor which walks over a program and checks validity.
     */
    private static class Checker
        extends RexVisitorImpl<Boolean>
    {
        private final boolean fail;
        private final RelDataType inputRowType;
        int failCount = 0;

        /**
         * Creates a Checker.
         *
         * @param inputRowType Input row type to expressions
         * @param fail Whether to throw if checker finds an error
         */
        private Checker(RelDataType inputRowType, boolean fail)
        {
            super(true);
            this.fail = fail;
            this.inputRowType = inputRowType;
        }

        public Boolean visitInputRef(RexInputRef inputRef)
        {
            final int index = inputRef.getIndex();
            final RelDataTypeField [] fields = inputRowType.getFields();
            if ((index < 0) || (index >= fields.length)) {
                assert !fail;
                ++failCount;
                return false;
            }
            if (!RelOptUtil.eq(
                    "inputRef",
                    inputRef.getType(),
                    "underlying field",
                    fields[index].getType(),
                    fail))
            {
                assert !fail;
                ++failCount;
                return false;
            }
            return true;
        }

        public Boolean visitLocalRef(RexLocalRef localRef)
        {
            assert !fail : "localRef invalid in project";
            ++failCount;
            return false;
        }

        public Boolean visitFieldAccess(RexFieldAccess fieldAccess)
        {
            super.visitFieldAccess(fieldAccess);
            final RelDataType refType =
                fieldAccess.getReferenceExpr().getType();
            assert refType.isStruct();
            final RelDataTypeField field = fieldAccess.getField();
            final int index = field.getIndex();
            if ((index < 0) || (index > refType.getFields().length)) {
                assert !fail;
                ++failCount;
                return false;
            }
            final RelDataTypeField typeField = refType.getFields()[index];
            if (!RelOptUtil.eq(
                    "type1",
                    typeField.getType(),
                    "type2",
                    fieldAccess.getType(),
                    fail))
            {
                assert !fail;
                ++failCount;
                return false;
            }
            return true;
        }
    }
}

// End ProjectRelBase.java
