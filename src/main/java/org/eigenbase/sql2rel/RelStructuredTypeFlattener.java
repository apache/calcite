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
package org.eigenbase.sql2rel;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.util.mapping.Mappings;

import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.function.IntegerFunction1;


// TODO jvs 10-Feb-2005:  factor out generic rewrite helper, with the
// ability to map between old and new rels and field ordinals.  Also,
// for now need to prohibit queries which return UDT instances.

/**
 * RelStructuredTypeFlattener removes all structured types from a tree of
 * relational expressions. Because it must operate globally on the tree, it is
 * implemented as an explicit self-contained rewrite operation instead of via
 * normal optimizer rules. This approach has the benefit that real optimizer and
 * codegen rules never have to deal with structured types.
 *
 * <p>As an example, suppose we have a structured type <code>ST(A1 smallint, A2
 * bigint)</code>, a table <code>T(c1 ST, c2 double)</code>, and a query <code>
 * select t.c2, t.c1.a2 from t</code>. After SqlToRelConverter executes, the
 * unflattened tree looks like:
 *
 * <pre><code>
 * ProjectRel(C2=[$1], A2=[$0.A2])
 *   TableAccessRel(table=[T])
 *</code></pre>
 *
 * After flattening, the resulting tree looks like
 *
 * <pre><code>
 * ProjectRel(C2=[$3], A2=[$2])
 *   FtrsIndexScanRel(table=[T], index=[clustered])
 *</code></pre>
 *
 * The index scan produces a flattened row type <code>(boolean, smallint,
 * bigint, double)</code> (the boolean is a null indicator for c1), and the
 * projection picks out the desired attributes (omitting <code>$0</code> and
 * <code>$1</code> altogether). After optimization, the projection might be
 * pushed down into the index scan, resulting in a final tree like
 *
 * <pre><code>
 * FtrsIndexScanRel(table=[T], index=[clustered], projection=[3, 2])
 *</code></pre>
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RelStructuredTypeFlattener
    implements ReflectiveVisitor
{
    //~ Instance fields --------------------------------------------------------

    private final RexBuilder rexBuilder;
    private final RewriteRelVisitor visitor;

    private Map<RelNode, RelNode> oldToNewRelMap;
    private RelNode currentRel;
    private int iRestructureInput;
    private RelDataType flattenedRootType;
    boolean restructured;
    private final RelOptTable.ToRelContext toRelContext;

    //~ Constructors -----------------------------------------------------------

    public RelStructuredTypeFlattener(RexBuilder rexBuilder)
    {
        this.rexBuilder = rexBuilder;
        visitor = new RewriteRelVisitor();
        toRelContext = null; // TODO:
    }

    //~ Methods ----------------------------------------------------------------

    public void updateRelInMap(
        Map<RelNode, SortedSet<CorrelatorRel.Correlation>> mapRefRelToCorVar)
    {
        Set<RelNode> oldRefRelSet = new HashSet<RelNode>();
        oldRefRelSet.addAll(mapRefRelToCorVar.keySet());
        for (RelNode rel : oldRefRelSet) {
            if (oldToNewRelMap.containsKey(rel)) {
                SortedSet<CorrelatorRel.Correlation> corVarSet =
                    new TreeSet<CorrelatorRel.Correlation>();
                corVarSet.addAll(mapRefRelToCorVar.get(rel));
                mapRefRelToCorVar.remove(rel);
                mapRefRelToCorVar.put(oldToNewRelMap.get(rel), corVarSet);
            }
        }
    }

    public void updateRelInMap(
        SortedMap<CorrelatorRel.Correlation, CorrelatorRel> mapCorVarToCorRel)
    {
        for (CorrelatorRel.Correlation corVar : mapCorVarToCorRel.keySet()) {
            CorrelatorRel oldRel = mapCorVarToCorRel.get(corVar);
            if (oldToNewRelMap.containsKey(oldRel)) {
                RelNode newRel = oldToNewRelMap.get(oldRel);
                assert (newRel instanceof CorrelatorRel);
                mapCorVarToCorRel.put(corVar, (CorrelatorRel) newRel);
            }
        }
    }

    public RelNode rewrite(RelNode root, boolean restructure)
    {
        // Perform flattening.
        oldToNewRelMap = new HashMap<RelNode, RelNode>();
        visitor.visit(root, 0, null);
        RelNode flattened = getNewForOldRel(root);
        flattenedRootType = flattened.getRowType();

        // If requested, add an additional projection which puts
        // everything back into structured form for return to the
        // client.
        restructured = false;
        List<RexNode> structuringExps = null;
        if (restructure) {
            iRestructureInput = 0;
            structuringExps = restructureFields(root.getRowType());
        }
        if (restructured) {
            // REVIEW jvs 23-Mar-2005:  How do we make sure that this
            // implementation stays in Java?  Fennel can't handle
            // structured types.
            return CalcRel.createProject(
                flattened,
                structuringExps,
                root.getRowType().getFieldNames());
        } else {
            return flattened;
        }
    }

    private List<RexNode> restructureFields(RelDataType structuredType) {
        List<RexNode> structuringExps = new ArrayList<RexNode>();
        for (RelDataTypeField field : structuredType.getFieldList()) {
            // TODO:  row
            if (field.getType().getSqlTypeName() == SqlTypeName.STRUCTURED) {
                restructured = true;
                structuringExps.add(restructure(field.getType()));
            } else {
                structuringExps.add(
                    new RexInputRef(
                        iRestructureInput,
                        field.getType()));
                ++iRestructureInput;
            }
        }
        return structuringExps;
    }

    private RexNode restructure(
        RelDataType structuredType)
    {
        // Access null indicator for entire structure.
        RexInputRef nullIndicator =
            new RexInputRef(
                iRestructureInput,
                flattenedRootType.getFields()[iRestructureInput].getType());
        ++iRestructureInput;

        // Use NEW to put flattened data back together into a structure.
        List<RexNode> inputExprs = restructureFields(structuredType);
        RexNode newInvocation =
            rexBuilder.makeNewInvocation(
                structuredType,
                inputExprs.toArray(new RexNode[inputExprs.size()]));

        if (!structuredType.isNullable()) {
            // Optimize away the null test.
            return newInvocation;
        }

        // Construct a CASE expression to handle the structure-level null
        // indicator.
        RexNode [] caseOperands = new RexNode[3];

        // WHEN StructuredType.Indicator IS NULL
        caseOperands[0] =
            rexBuilder.makeCall(
                SqlStdOperatorTable.isNullOperator,
                nullIndicator);

        // THEN CAST(NULL AS StructuredType)
        caseOperands[1] =
            rexBuilder.makeCast(
                structuredType,
                rexBuilder.constantNull());

        // ELSE NEW StructuredType(inputs...) END
        caseOperands[2] = newInvocation;

        return rexBuilder.makeCall(
            SqlStdOperatorTable.caseOperator,
            caseOperands);
    }

    protected void setNewForOldRel(RelNode oldRel, RelNode newRel)
    {
        oldToNewRelMap.put(oldRel, newRel);
    }

    protected RelNode getNewForOldRel(RelNode oldRel)
    {
        return oldToNewRelMap.get(oldRel);
    }

    /**
     * Maps the ordinal of a field pre-flattening to the ordinal of the
     * corresponding field post-flattening, and optionally returns its type.
     *
     * @param oldOrdinal Pre-flattening ordinal
     *
     * @return Post-flattening ordinal
     */
    protected int getNewForOldInput(int oldOrdinal)
    {
        assert (currentRel != null);
        int newOrdinal = 0;

        // determine which input rel oldOrdinal references, and adjust
        // oldOrdinal to be relative to that input rel
        RelNode oldInput = null;
        for (RelNode oldInput1 : currentRel.getInputs()) {
            RelDataType oldInputType = oldInput1.getRowType();
            int n = oldInputType.getFieldCount();
            if (oldOrdinal < n) {
                oldInput = oldInput1;
                break;
            }
            RelNode newInput = getNewForOldRel(oldInput1);
            newOrdinal += newInput.getRowType().getFieldCount();
            oldOrdinal -= n;
        }
        assert (oldInput != null);

        RelDataType oldInputType = oldInput.getRowType();
        newOrdinal += calculateFlattenedOffset(oldInputType, oldOrdinal);
        return newOrdinal;
    }

    /**
     * Returns a mapping between old and new fields.
     *
     * @param oldRel Old relational expression
     * @return Mapping between fields of old and new
     */
    private Mappings.TargetMapping getNewForOldInputMapping(RelNode oldRel) {
        final RelNode newRel = getNewForOldRel(oldRel);
        return Mappings.target(
            new Util.Function1<Integer, Integer>() {
                public Integer apply(Integer oldInput) {
                    return getNewForOldInput(oldInput);
                }
            },
            oldRel.getRowType().getFieldCount(),
            newRel.getRowType().getFieldCount());
    }

    private int calculateFlattenedOffset(RelDataType rowType, int ordinal)
    {
        int offset = 0;
        if (SqlTypeUtil.needsNullIndicator(rowType)) {
            // skip null indicator
            ++offset;
        }
        RelDataTypeField [] oldFields = rowType.getFields();
        for (int i = 0; i < ordinal; ++i) {
            RelDataType oldFieldType = oldFields[i].getType();
            if (oldFieldType.isStruct()) {
                // TODO jvs 10-Feb-2005:  this isn't terribly efficient;
                // keep a mapping somewhere
                RelDataType flattened =
                    SqlTypeUtil.flattenRecordType(
                        rexBuilder.getTypeFactory(),
                        oldFieldType,
                        null);
                final RelDataTypeField [] fields = flattened.getFields();
                offset += fields.length;
            } else {
                ++offset;
            }
        }
        return offset;
    }

    protected RexNode flattenFieldAccesses(RexNode exp)
    {
        RewriteRexShuttle shuttle = new RewriteRexShuttle();
        return exp.accept(shuttle);
    }

    public void rewriteRel(TableModificationRel rel)
    {
        TableModificationRel newRel =
            new TableModificationRel(
                rel.getCluster(),
                rel.getTable(),
                rel.getCatalogReader(),
                getNewForOldRel(rel.getChild()),
                rel.getOperation(),
                rel.getUpdateColumnList(),
                true);
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(AggregateRel rel)
    {
        RelDataType inputType = rel.getChild().getRowType();
        for (RelDataTypeField field : inputType.getFieldList()) {
            if (field.getType().isStruct()) {
                // TODO jvs 10-Feb-2005
                throw Util.needToImplement("aggregation on structured types");
            }
        }

        rewriteGeneric(rel);
    }

    public void rewriteRel(SortRel rel)
    {
        RelCollation oldCollation = rel.getCollation();
        final RelNode oldChild = rel.getChild();
        final RelNode newChild = getNewForOldRel(oldChild);
        final Mappings.TargetMapping mapping =
            getNewForOldInputMapping(oldChild);

        // validate
        for (RelFieldCollation field : oldCollation.getFieldCollations()) {
            int oldInput = field.getFieldIndex();
            RelDataType sortFieldType =
                oldChild.getRowType().getFields()[oldInput].getType();
            if (sortFieldType.isStruct()) {
                // TODO jvs 10-Feb-2005
                throw Util.needToImplement("sorting on structured types");
            }
        }

        SortRel newRel =
            new SortRel(
                rel.getCluster(),
                rel.getCluster().traitSetOf(Convention.NONE),
                newChild,
                RexUtil.apply(mapping, oldCollation));
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(FilterRel rel)
    {
        RelNode newRel =
            CalcRel.createFilter(
                getNewForOldRel(rel.getChild()),
                flattenFieldAccesses(rel.getCondition()));
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(JoinRel rel)
    {
        JoinRel newRel =
            new JoinRel(
                rel.getCluster(),
                getNewForOldRel(rel.getLeft()),
                getNewForOldRel(rel.getRight()),
                flattenFieldAccesses(rel.getCondition()),
                rel.getJoinType(),
                rel.getVariablesStopped());
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(CorrelatorRel rel)
    {
        Iterator oldCorrelations = rel.getCorrelations().iterator();
        ArrayList<CorrelatorRel.Correlation> newCorrelations =
            new ArrayList<CorrelatorRel.Correlation>();
        while (oldCorrelations.hasNext()) {
            CorrelatorRel.Correlation c =
                (CorrelatorRel.Correlation) oldCorrelations.next();
            RelDataType corrFieldType =
                rel.getLeft().getRowType().getFields()[c.getOffset()].getType();
            if (corrFieldType.isStruct()) {
                throw Util.needToImplement("correlation on structured type");
            }
            newCorrelations.add(
                new CorrelatorRel.Correlation(
                    c.getId(),
                    getNewForOldInput(c.getOffset())));
        }
        CorrelatorRel newRel =
            new CorrelatorRel(
                rel.getCluster(),
                getNewForOldRel(rel.getLeft()),
                getNewForOldRel(rel.getRight()),
                rel.getCondition(),
                newCorrelations,
                rel.getJoinType());
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(CollectRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(UncollectRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(IntersectRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(MinusRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(UnionRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(OneRowRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(ValuesRel rel)
    {
        // NOTE jvs 30-Apr-2006:  UDT instances require invocation
        // of a constructor method, which can't be represented
        // by the tuples stored in a ValuesRel, so we don't have
        // to worry about them here.
        rewriteGeneric(rel);
    }

    public void rewriteRel(TableFunctionRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(SamplingRel rel)
    {
        rewriteGeneric(rel);
    }

    public void rewriteRel(ProjectRel rel)
    {
        final List<RexNode> flattenedExpList = new ArrayList<RexNode>();
        final List<String> flattenedFieldNameList = new ArrayList<String>();
        List<String> fieldNames = rel.getRowType().getFieldNames();
        flattenProjections(
            rel.getProjectExpList(),
            fieldNames,
            "",
            flattenedExpList,
            flattenedFieldNameList);
        final RexNode [] flattenedExps =
            (RexNode []) flattenedExpList.toArray(RexNode.EMPTY_ARRAY);
        final String [] flattenedFieldNames =
            (String []) flattenedFieldNameList.toArray(
                new String[flattenedFieldNameList.size()]);
        RelNode newRel =
            CalcRel.createProject(
                getNewForOldRel(rel.getChild()),
                flattenedExps,
                flattenedFieldNames);
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(CalcRel rel)
    {
        // Translate the child.
        final RelNode newChild = getNewForOldRel(rel.getChild());

        final RelOptCluster cluster = rel.getCluster();
        RexProgramBuilder programBuilder =
            new RexProgramBuilder(
                newChild.getRowType(),
                cluster.getRexBuilder());

        // Convert the common expressions.
        final RexProgram program = rel.getProgram();
        for (RexNode expr : program.getExprList()) {
            programBuilder.registerInput(
                flattenFieldAccesses(expr));
        }

        // Convert the projections.
        final List<RexNode> flattenedExpList = new ArrayList<RexNode>();
        final List<String> flattenedFieldNameList = new ArrayList<String>();
        List<String> fieldNames = rel.getRowType().getFieldNames();
        flattenProjections(
            program.getProjectList(),
            fieldNames,
            "",
            flattenedExpList,
            flattenedFieldNameList);

        // Register each of the new projections.
        int i = -1;
        for (RexNode flattenedExp : flattenedExpList) {
            ++i;
            programBuilder.addProject(
                flattenedExp,
                flattenedFieldNameList.get(i));
        }

        // Translate the condition.
        final RexLocalRef conditionRef = program.getCondition();
        if (conditionRef != null) {
            programBuilder.addCondition(
                new RexLocalRef(
                    getNewForOldInput(conditionRef.getIndex()),
                    conditionRef.getType()));
        }

        RexProgram newProgram = programBuilder.getProgram();

        // Create a new calc relational expression.
        CalcRel newRel =
            new CalcRel(
                cluster,
                rel.getTraitSet(),
                newChild,
                newProgram.getOutputRowType(),
                newProgram,
                Collections.<RelCollation>emptyList());
        setNewForOldRel(rel, newRel);
    }

    public void rewriteRel(SelfFlatteningRel rel)
    {
        rel.flattenRel(this);
    }

    public void rewriteGeneric(RelNode rel)
    {
        RelNode newRel = rel.copy(rel.getTraitSet(), rel.getInputs());
        List<RelNode> oldInputs = rel.getInputs();
        for (int i = 0; i < oldInputs.size(); ++i) {
            newRel.replaceInput(
                i,
                getNewForOldRel(oldInputs.get(i)));
        }
        setNewForOldRel(rel, newRel);
    }

    private void flattenProjections(
        List<? extends RexNode> exps,
        List<String> fieldNames,
        String prefix,
        List<RexNode> flattenedExps,
        List<String> flattenedFieldNames)
    {
        for (int i = 0; i < exps.size(); ++i) {
            RexNode exp = exps.get(i);
            String fieldName =
                (fieldNames == null || fieldNames.get(i) == null)
                    ? ("$" + i)
                    : fieldNames.get(i);
            if (!prefix.equals("")) {
                fieldName = prefix + "$" + fieldName;
            }
            flattenProjection(
                exp,
                fieldName,
                flattenedExps,
                flattenedFieldNames);
        }
    }

    private void flattenProjection(
        RexNode exp,
        String fieldName,
        List<RexNode> flattenedExps,
        List<String> flattenedFieldNames)
    {
        if (exp.getType().isStruct()) {
            if (exp instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) exp;
                int newOffset = getNewForOldInput(inputRef.getIndex());

                // expand to range
                RelDataType flattenedType =
                    SqlTypeUtil.flattenRecordType(
                        rexBuilder.getTypeFactory(),
                        exp.getType(),
                        null);
                List<RelDataTypeField> fieldList = flattenedType.getFieldList();
                int n = fieldList.size();
                for (int j = 0; j < n; ++j) {
                    RelDataTypeField field = fieldList.get(j);
                    flattenedExps.add(
                        new RexInputRef(
                            newOffset + j,
                            field.getType()));
                    flattenedFieldNames.add(fieldName);
                }
            } else if (isConstructor(exp) || exp.isA(RexKind.Cast)) {
                // REVIEW jvs 27-Feb-2005:  for cast, see corresponding note
                // in RewriteRexShuttle
                RexCall call = (RexCall) exp;
                if (exp.isA(RexKind.NewSpecification)) {
                    // For object constructors, prepend a FALSE null
                    // indicator.
                    flattenedExps.add(
                        rexBuilder.makeLiteral(false));
                    flattenedFieldNames.add(fieldName);
                } else if (exp.isA(RexKind.Cast)) {
                    if (RexLiteral.isNullLiteral(
                            ((RexCall) exp).getOperands()[0]))
                    {
                        // Translate CAST(NULL AS UDT) into
                        // the correct number of null fields.
                        flattenNullLiteral(
                            exp.getType(),
                            flattenedExps,
                            flattenedFieldNames);
                        return;
                    }
                }
                flattenProjections(
                    call.getOperandList(),
                    Collections.<String>nCopies(
                        call.getOperands().length, null),
                    fieldName,
                    flattenedExps,
                    flattenedFieldNames);
            } else if (exp instanceof RexCall) {
                // NOTE jvs 10-Feb-2005:  This is a lame hack to keep special
                // functions which return row types working.

                int j = 0;
                for (RelDataTypeField field : exp.getType().getFieldList()) {
                    RexNode cloneCall = exp.clone();
                    RexNode fieldAccess =
                        rexBuilder.makeFieldAccess(
                            cloneCall,
                            field.getIndex());
                    flattenedExps.add(fieldAccess);
                    flattenedFieldNames.add(fieldName + "$" + (j++));
                }
            } else {
                throw Util.needToImplement(exp);
            }
        } else {
            exp = flattenFieldAccesses(exp);
            flattenedExps.add(exp);
            flattenedFieldNames.add(fieldName);
        }
    }

    private void flattenNullLiteral(
        RelDataType type,
        List<RexNode> flattenedExps,
        List<String> flattenedFieldNames)
    {
        RelDataType flattenedType =
            SqlTypeUtil.flattenRecordType(
                rexBuilder.getTypeFactory(),
                type,
                null);
        for (RelDataTypeField field : flattenedType.getFieldList()) {
            flattenedExps.add(
                rexBuilder.makeCast(
                    field.getType(),
                    rexBuilder.constantNull()));
            flattenedFieldNames.add(field.getName());
        }
    }

    private boolean isConstructor(RexNode rexNode)
    {
        // TODO jvs 11-Feb-2005:  share code with SqlToRelConverter
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        RexCall call = (RexCall) rexNode;
        return call.getOperator().getName().equalsIgnoreCase("row")
            || (call.isA(RexKind.NewSpecification));
    }

    public void rewriteRel(TableAccessRel rel)
    {
        RelNode newRel =
            rel.getTable().toRel(toRelContext);

        setNewForOldRel(rel, newRel);
    }

    //~ Inner Interfaces -------------------------------------------------------

    public interface SelfFlatteningRel
        extends RelNode
    {
        void flattenRel(RelStructuredTypeFlattener flattener);
    }

    //~ Inner Classes ----------------------------------------------------------

    private class RewriteRelVisitor
        extends RelVisitor
    {
        private final ReflectiveVisitDispatcher<RelStructuredTypeFlattener,
            RelNode> dispatcher =
            ReflectUtil.createDispatcher(
                RelStructuredTypeFlattener.class,
                RelNode.class);

        // implement RelVisitor
        public void visit(RelNode p, int ordinal, RelNode parent)
        {
            // rewrite children first
            super.visit(p, ordinal, parent);

            currentRel = p;
            final String visitMethodName = "rewriteRel";
            boolean found =
                dispatcher.invokeVisitor(
                    RelStructuredTypeFlattener.this,
                    currentRel,
                    visitMethodName);
            currentRel = null;
            if (!found) {
                if (p.getInputs().size() == 0) {
                    // for leaves, it's usually safe to assume that
                    // no transformation is required
                    rewriteGeneric(p);
                }
            }
            if (!found) {
                throw Util.newInternal(
                    "no '" + visitMethodName + "' method found for class "
                    + p.getClass().getName());
            }
        }
    }

    private class RewriteRexShuttle
        extends RexShuttle
    {
        // override RexShuttle
        public RexNode visitInputRef(RexInputRef input)
        {
            final int oldIndex = input.getIndex();
            final int newIndex = getNewForOldInput(oldIndex);

            // FIXME: jhyde, 2005/12/3: Once indicator fields have been
            //  introduced, the new field type may be very different to the
            //  old field type. We should look at the actual flattened types,
            //  rather than trying to deduce the type from the current type.
            RelDataType fieldType = removeDistinct(input.getType());
            RexInputRef newInput = new RexInputRef(newIndex, fieldType);
            return newInput;
        }

        private RelDataType removeDistinct(RelDataType type)
        {
            if (type.getSqlTypeName() != SqlTypeName.DISTINCT) {
                return type;
            }
            return type.getFields()[0].getType();
        }

        // override RexShuttle
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess)
        {
            // walk down the field access path expression, calculating
            // the desired input number
            int iInput = 0;
            RelDataType fieldType = removeDistinct(fieldAccess.getType());

            for (;;) {
                RexNode refExp = fieldAccess.getReferenceExpr();
                int ordinal =
                    refExp.getType().getFieldOrdinal(
                        fieldAccess.getField().getName());
                iInput +=
                    calculateFlattenedOffset(
                        refExp.getType(),
                        ordinal);
                if (refExp instanceof RexInputRef) {
                    RexInputRef inputRef = (RexInputRef) refExp;
                    iInput += getNewForOldInput(inputRef.getIndex());
                    return new RexInputRef(iInput, fieldType);
                } else if (refExp instanceof RexCorrelVariable) {
                    return fieldAccess;
                } else if (refExp.isA(RexKind.Cast)) {
                    // REVIEW jvs 27-Feb-2005:  what about a cast between
                    // different user-defined types (once supported)?
                    RexCall cast = (RexCall) refExp;
                    refExp = cast.getOperands()[0];
                }
                if (refExp.isA(RexKind.NewSpecification)) {
                    return
                        ((RexCall) refExp).getOperands()[fieldAccess.getField()
                            .getIndex()];
                }
                if (!(refExp instanceof RexFieldAccess)) {
                    throw Util.needToImplement(refExp);
                }
                fieldAccess = (RexFieldAccess) refExp;
            }
        }

        // override RexShuttle
        public RexNode visitCall(RexCall rexCall)
        {
            if (rexCall.isA(RexKind.Cast)) {
                RexNode input = rexCall.getOperands()[0].accept(this);
                RelDataType targetType = removeDistinct(rexCall.getType());
                return rexBuilder.makeCast(
                    targetType,
                    input);
            }
            if (!rexCall.isA(RexKind.Comparison)) {
                return super.visitCall(rexCall);
            }
            RexNode lhs = rexCall.getOperands()[0];
            if (!lhs.getType().isStruct()) {
                // NOTE jvs 9-Mar-2005:  Calls like IS NULL operate
                // on the representative null indicator.  Since it comes
                // first, we don't have to do any special translation.
                return super.visitCall(rexCall);
            }

            // NOTE jvs 22-Mar-2005:  Likewise, the null indicator takes
            // care of comparison null semantics without any special casing.
            return flattenComparison(
                rexBuilder,
                rexCall.getOperator(),
                rexCall.getOperandList());
        }

        private RexNode flattenComparison(
            RexBuilder rexBuilder,
            SqlOperator op,
            List<RexNode> exprs)
        {
            List<RexNode> flattenedExps = new ArrayList<RexNode>();
            flattenProjections(
                exprs,
                null,
                "",
                flattenedExps,
                new ArrayList<String>());
            int n = flattenedExps.size() / 2;
            boolean negate = false;
            if (op.getKind() == SqlKind.NOT_EQUALS) {
                negate = true;
                op = SqlStdOperatorTable.equalsOperator;
            }
            if ((n > 1) && op.getKind() != SqlKind.EQUALS) {
                throw Util.needToImplement(
                    "inequality comparison for row types");
            }
            RexNode conjunction = null;
            for (int i = 0; i < n; ++i) {
                RexNode comparison =
                    rexBuilder.makeCall(
                        op,
                        flattenedExps.get(i),
                        flattenedExps.get(i + n));
                if (conjunction == null) {
                    conjunction = comparison;
                } else {
                    conjunction =
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.andOperator,
                            conjunction,
                            comparison);
                }
            }
            if (negate) {
                return rexBuilder.makeCall(
                    SqlStdOperatorTable.notOperator,
                    conjunction);
            } else {
                return conjunction;
            }
        }
    }
}

// End RelStructuredTypeFlattener.java
