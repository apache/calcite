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
package org.eigenbase.oj.stmt;

import java.io.*;

import java.lang.reflect.*;

import java.util.*;
import java.util.logging.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.javac.*;
import org.eigenbase.oj.rel.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.runtime.*;
import org.eigenbase.runtime.Iterable;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * <code>OJPreparingStmt</code> is an abstract base for classes which implement
 * the process of preparing and executing SQL expressions by generating OpenJava
 * code.
 */
public abstract class OJPreparingStmt
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String connectionVariable = "connection";
    private static final Logger tracer = EigenbaseTrace.getStatementTracer();

    //~ Instance fields --------------------------------------------------------

    protected String queryString = null;
    protected Environment env;

    /**
     * CallingConvention via which results should be returned by execution.
     */
    private CallingConvention resultCallingConvention;

    protected JavaCompiler javaCompiler;
    protected final CatalogReader catalogReader;

    protected EigenbaseTimingTracer timingTracer;

    /**
     * True if the statement contains java RelNodes
     */
    protected boolean containsJava;

    protected java.util.List<java.util.List<String>> fieldOrigins;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a statement.
     *
     * @param catalogReader Catalog reader
     */
    public OJPreparingStmt(CatalogReader catalogReader)
    {
        this.catalogReader = catalogReader;
        this.resultCallingConvention = CallingConvention.RESULT_SET;
        this.containsJava = true;
    }

    //~ Methods ----------------------------------------------------------------

    public void setResultCallingConvention(
        CallingConvention resultCallingConvention)
    {
        this.resultCallingConvention = resultCallingConvention;
    }

    protected BoundMethod compileAndBind(
        ClassDeclaration decl,
        ParseTree parseTree,
        Argument [] arguments)
    {
        BoundMethod thunk = compile(decl, env, parseTree, arguments);
        Object [] args = new Object[thunk.parameterNames.length];
        for (int i = 0; i < thunk.parameterNames.length; i++) {
            String parameterName = thunk.parameterNames[i];
            Argument argument = null;
            for (int j = 0; j < arguments.length; j++) {
                if (arguments[j].name.equals(parameterName)) {
                    argument = arguments[j];
                    break;
                }
            }
            if (argument == null) {
                throw Util.newInternal(
                    "variable '" + parameterName
                    + "' not found");
            }
            args[i] = argument.value;
        }
        thunk.args = args;
        return thunk;
    }

    protected void initSub()
    {
    }

    public ClassDeclaration init(Argument [] arguments)
    {
        env = OJSystem.env;

        String packageName = getTempPackageName();
        String className = getTempClassName();
        env = new FileEnvironment(env, packageName, className);
        ClassDeclaration decl =
            new ClassDeclaration(
                new ModifierList(ModifierList.PUBLIC),
                className,
                null,
                null,
                new MemberDeclarationList());
        OJClass clazz = new OJClass(env, null, decl);
        env.record(
            clazz.getName(),
            clazz);
        env = new ClosedEnvironment(clazz.getEnvironment());

        initSub();

        OJUtil.threadDeclarers.set(clazz);
        if ((arguments != null) && (arguments.length > 0)) {
            for (int i = 0; i < arguments.length; i++) {
                final Argument argument = arguments[i];
                if (argument.value instanceof Enumeration) {
                    argument.value =
                        new EnumerationIterator((Enumeration) argument.value);
                    argument.clazz = argument.value.getClass();
                }
                if ((argument.value instanceof Iterator)
                    && !(argument.value instanceof Iterable))
                {
                    argument.value =
                        new BufferedIterator((Iterator) argument.value);
                    argument.clazz = argument.value.getClass();
                }
                // If the argument's type is a private class, change its
                // type to the nearest base class which is public. Otherwise
                // the generated code won't compile.
                argument.clazz =
                    visibleBaseClass(argument.clazz, packageName);
                bindArgument(argument);
            }
        }
        return decl;
    }

    protected void bindArgument(Argument arg)
    {
        env.bindVariable(
            arg.getName(),
            arg.getType());
    }

    public PreparedResult prepareSql(
        SqlNode sqlQuery,
        Class runtimeContextClass,
        SqlValidator validator,
        boolean needsValidation)
    {
        return prepareSql(
            sqlQuery,
            sqlQuery,
            runtimeContextClass,
            validator,
            needsValidation);
    }

    /**
     * Prepares a statement for execution, starting from a parse tree and using
     * a user-supplied validator.
     */
    public PreparedResult prepareSql(
        SqlNode sqlQuery,
        SqlNode sqlNodeOriginal,
        Class runtimeContextClass,
        SqlValidator validator,
        boolean needsValidation)
    {
        queryString = sqlQuery.toString();

        final Argument [] arguments = {
            new Argument(
                connectionVariable,
                runtimeContextClass,
                null)
        };
        ClassDeclaration decl = init(arguments);

        SqlToRelConverter sqlToRelConverter =
            getSqlToRelConverter(validator, catalogReader);

        SqlExplain sqlExplain = null;
        if (sqlQuery.getKind() == SqlKind.EXPLAIN) {
            // dig out the underlying SQL statement
            sqlExplain = (SqlExplain) sqlQuery;
            sqlQuery = sqlExplain.getExplicandum();
            sqlToRelConverter.setIsExplain(sqlExplain.getDynamicParamCount());
        }

        RelNode rootRel =
            sqlToRelConverter.convertQuery(sqlQuery, needsValidation, true);

        if (timingTracer != null) {
            timingTracer.traceTime("end sql2rel");
        }

        final RelDataType resultType = validator.getValidatedNodeType(sqlQuery);
        fieldOrigins = validator.getFieldOrigins(sqlQuery);
        assert fieldOrigins.size() == resultType.getFieldCount();

        // Display logical plans before view expansion, plugging in physical
        // storage and decorrelation
        if (sqlExplain != null) {
            SqlExplain.Depth explainDepth = sqlExplain.getDepth();
            boolean explainAsXml = sqlExplain.isXml();
            SqlExplainLevel detailLevel = sqlExplain.getDetailLevel();
            switch (explainDepth) {
            case Type:
                return new PreparedExplanation(
                    resultType,
                    null,
                    explainAsXml,
                    detailLevel);
            case Logical:
                return new PreparedExplanation(
                    null,
                    rootRel,
                    explainAsXml,
                    detailLevel);
            default:
            }
        }

        // Structured type flattening, view expansion, and plugging in physical
        // storage.
        rootRel = flattenTypes(rootRel, true);

        // Subquery decorrelation.
        rootRel = decorrelate(sqlQuery, rootRel);

        // Trim unused fields.
        rootRel = trimUnusedFields(rootRel);

        // Display physical plan after decorrelation.
        if (sqlExplain != null) {
            SqlExplain.Depth explainDepth = sqlExplain.getDepth();
            boolean explainAsXml = sqlExplain.isXml();
            SqlExplainLevel detailLevel = sqlExplain.getDetailLevel();
            switch (explainDepth) {
            case Physical:
            default:
                rootRel =
                    optimize(
                        rootRel.getRowType(),
                        rootRel);
                return new PreparedExplanation(
                    null,
                    rootRel,
                    explainAsXml,
                    detailLevel);
            }
        }

        rootRel = optimize(resultType, rootRel);
        containsJava = treeContainsJava(rootRel);

        if (timingTracer != null) {
            timingTracer.traceTime("end optimization");
        }

        // For transformation from DML -> DML, use result of rewrite
        // (e.g. UPDATE -> MERGE).  For anything else (e.g. CALL -> SELECT),
        // use original kind.
        SqlKind kind = sqlQuery.getKind();
        if (!kind.belongsTo(SqlKind.DML)) {
            kind = sqlNodeOriginal.getKind();
        }
        return implement(
            resultType,
            rootRel,
            kind,
            decl,
            arguments);
    }

    /**
     * Optimizes a query plan.
     *
     *
     * @param logicalRowType logical row type of relational expression (before
     * struct fields are flattened, or field names are renamed for uniqueness)
     * @param rootRel root of a relational expression
     *
     * @return an equivalent optimized relational expression
     */
    protected RelNode optimize(
        RelDataType logicalRowType,
        RelNode rootRel)
    {
        final RelOptPlanner planner = rootRel.getCluster().getPlanner();

        // Allow each rel to register its own rules.
        RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                planner.registerClass(node);
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(rootRel);

        planner.setRoot(rootRel);

        RelTraitSet desiredTraits = getDesiredRootTraitSet(rootRel);

        final RelNode rootRel2 = planner.changeTraits(rootRel, desiredTraits);
        assert rootRel2 != null;

        planner.setRoot(rootRel2);
        final RelOptPlanner planner2 = planner.chooseDelegate();
        final RelNode rootRel3 = planner2.findBestExp();
        assert rootRel3 != null : "could not implement exp";
        return rootRel3;
    }

    protected RelTraitSet getDesiredRootTraitSet(RelNode rootRel)
    {
        // Make sure non-CallingConvention traits, if any, are preserved
        return rootRel.getTraitSet()
            .replace(resultCallingConvention);
    }

    /**
     * Determines if the RelNode tree contains Java RelNodes. Also, if the row
     * contains an interval type, then effectively, the tree is treated as
     * containing Java, since we currently cannot read raw interval columns.
     *
     * @param rootRel root of the RelNode tree
     *
     * @return true if the tree contains Java RelNodes or returns an interval
     * type
     */
    protected boolean treeContainsJava(RelNode rootRel)
    {
        JavaRelFinder javaFinder = new JavaRelFinder();
        javaFinder.go(rootRel);
        if (javaFinder.containsJavaRel()) {
            return true;
        }
        RelDataTypeField [] fields = rootRel.getRowType().getFields();
        for (int i = 0; i < fields.length; i++) {
            if (SqlTypeUtil.isInterval(fields[i].getType())) {
                return true;
            }
        }
        return false;
    }

    public void halfImplement(
        RelNode rootRel,
        ClassDeclaration decl)
    {
        Class runtimeContextClass = null;
        final Argument [] arguments = {
            new Argument(
                connectionVariable,
                runtimeContextClass,
                null)
        };
        assert containsJava;
        javaCompiler = createCompiler();
        JavaRelImplementor relImplementor =
            (JavaRelImplementor)
            getRelImplementor(rootRel.getCluster().getRexBuilder());
        Expression expr = relImplementor.implementRoot((JavaRel) rootRel);
        BoundMethod boundMethod;
        boundMethod = compileAndBind(decl, expr, arguments);
        Util.discard(boundMethod);
    }

    /**
     * Implements a physical query plan.
     *
     * @param rowType original rowtype returned by query validator
     * @param rootRel root of the relational expression.
     * @param sqlKind SqlKind of the original statement.
     * @param decl ClassDeclaration of the generated result.
     * @param args argument list of the generated result.
     * @return an executable plan, a {@link PreparedExecution}.
     */
    protected PreparedExecution implement(
        RelDataType rowType,
        RelNode rootRel,
        SqlKind sqlKind,
        ClassDeclaration decl,
        Argument[] args)
    {
        BoundMethod boundMethod;
        ParseTree parseTree;
        RelDataType resultType = rootRel.getRowType();
        boolean isDml = sqlKind.belongsTo(SqlKind.DML);
        if (containsJava) {
            javaCompiler = createCompiler();
            JavaRelImplementor relImplementor =
                (JavaRelImplementor)
                getRelImplementor(rootRel.getCluster().getRexBuilder());
            Expression expr = relImplementor.implementRoot((JavaRel) rootRel);

            if (timingTracer != null) {
                timingTracer.traceTime("end codegen");
            }

            parseTree = expr;
            boundMethod = compileAndBind(decl, parseTree, args);

            if (timingTracer != null) {
                timingTracer.traceTime("end compilation");
            }
        } else {
            boundMethod = null;
            parseTree = null;

            // Need to create a new result rowtype where the field names of
            // the row originate from the original projection list. E.g,
            // if you have a query like:
            //      select col1 as x, col2 as x from tab;
            // the field names of the resulting tuples should be "x" and "x",
            // rather than "x" and "x0".
            //
            // This only needs to be done for non-DML statements.  For DML, the
            // resultant row is a single rowcount column, so we don't want to
            // change the field name in those cases.
            if (!isDml) {
                assert (rowType.getFieldCount() == resultType.getFieldCount());
                String [] fieldNames = RelOptUtil.getFieldNames(rowType);
                RelDataType [] types = RelOptUtil.getFieldTypes(resultType);
                resultType =
                    rootRel.getCluster().getTypeFactory().createStructType(
                        types,
                        fieldNames);
            }

            // strip off the topmost, special converter RelNode, now that we no
            // longer need it
            rootRel = rootRel.getInput(0);
        }

        final PreparedExecution plan =
            new PreparedExecution(
                parseTree,
                rootRel,
                resultType,
                isDml,
                mapTableModOp(isDml, sqlKind),
                boundMethod);
        return plan;
    }

    protected TableModificationRel.Operation mapTableModOp(
        boolean isDml, SqlKind sqlKind)
    {
        if (!isDml) {
            return null;
        }
        switch (sqlKind) {
        case INSERT:
            return TableModificationRel.Operation.INSERT;
        case DELETE:
            return TableModificationRel.Operation.DELETE;
        case MERGE:
            return TableModificationRel.Operation.MERGE;
        case UPDATE:
            return TableModificationRel.Operation.UPDATE;
        default:
            return null;
        }
    }

    /**
     * Prepares a statement for execution, starting from a relational expression
     * (ie a logical or a physical query plan).
     *
     * @param rowType Row type
     * @param rootRel root of the relational expression.
     * @param sqlKind SqlKind for the relational expression: only
     * SqlKind.Explain and SqlKind.Dml are special cases.
     * @param needOpt true for a logical query plan (still needs to be
     * optimized), false for a physical plan.
     * @param decl openjava ClassDeclaration for the code generated to implement
     * the statement.
     * @param args openjava argument list for the generated code.
     */
    public PreparedResult prepareSql(
        RelDataType rowType,
        RelNode rootRel,
        SqlKind sqlKind,
        boolean needOpt,
        ClassDeclaration decl,
        Argument [] args)
    {
        if (needOpt) {
            rootRel = flattenTypes(rootRel, true);
            rootRel =
                optimize(
                    rootRel.getRowType(),
                    rootRel);
        }
        return implement(rowType, rootRel, sqlKind, decl, args);
    }

    /**
     * Protected method to allow subclasses to override construction of
     * SqlToRelConverter.
     */
    protected abstract SqlToRelConverter getSqlToRelConverter(
        SqlValidator validator,
        CatalogReader catalogReader);

    /**
     * Protected method to allow subclasses to override construction of
     * RelImplementor.
     */
    protected abstract RelImplementor getRelImplementor(RexBuilder rexBuilder);

    protected abstract String getClassRoot();

    protected abstract String getCompilerClassName();

    protected abstract String getJavaRoot();

    protected abstract String getTempPackageName();

    protected abstract String getTempMethodName();

    protected abstract String getTempClassName();

    protected abstract boolean shouldAlwaysWriteJavaFile();

    protected abstract boolean shouldSetConnectionInfo();

    public abstract RelNode flattenTypes(
        RelNode rootRel,
        boolean restructure);

    protected abstract RelNode decorrelate(
        SqlNode query,
        RelNode rootRel);

    /**
     * Walks over a tree of relational expressions, replacing each
     * {@link RelNode} with a 'slimmed down' relational expression that projects
     * only the columns required by its consumer.
     *
     * @param rootRel Relational expression that is at the root of the tree
     * @return Trimmed relational expression
     */
    protected abstract RelNode trimUnusedFields(RelNode rootRel);

    protected JavaCompiler createCompiler()
    {
        String compilerClassName = getCompilerClassName();
        try {
            Class compilerClass = Class.forName(compilerClassName);
            return (JavaCompiler) compilerClass.newInstance();
        } catch (ClassNotFoundException e) {
            throw Util.newInternal(e, "while instantiating compiler");
        } catch (InstantiationException e) {
            throw Util.newInternal(e, "while instantiating compiler");
        } catch (IllegalAccessException e) {
            throw Util.newInternal(e, "while instantiating compiler");
        } catch (ClassCastException e) {
            throw Util.newInternal(e, "while instantiating compiler");
        }
    }

    /**
     * Returns the lowest ancestor of <code>clazz</code> which is visible from
     * <code>fromPackage</code>&#46;<code>fromClazz</code>.
     */
    private static Class visibleBaseClass(
        Class clazz,
        String fromPackageName)
    {
        for (Class c = clazz; c != null; c = c.getSuperclass()) {
            int modifiers = c.getModifiers();
            if (Modifier.isPublic(modifiers)) {
                return c;
            }
            Package pakkage = c.getPackage();
            if (pakkage == null) {
                pakkage = Object.class.getPackage();
            }
            if (!Modifier.isPrivate(modifiers)
                && pakkage.getName().equals(fromPackageName))
            {
                return c;
            }
        }
        return java.lang.Object.class;
    }

    protected void addDecl(
        openjava.ptree.Statement statement,
        ExpressionList exprList)
    {
        return;
    }

    private BoundMethod compile(
        ClassDeclaration decl,
        Environment env,
        ParseTree parseTree,
        Argument [] arguments)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.log(
                Level.FINE,
                "source " + parseTree.toFlattenString());
            tracer.log(
                Level.FINE,
                "decl " + decl.toFlattenString());
            tracer.log(
                Level.FINE,
                "Before compile, parse tree",
                new Object[] { parseTree });
        }

        // NOTE jvs 14-Jan-2004:  DynamicJava doesn't correctly handle
        // the FINAL modifier on parameters.  So I made the codegen
        // for the method body copy the parameter to a final local
        // variable instead.  The only side-effect is that the parameter
        // names in the method signature is different.
        // TODO jvs 18-Oct-2006:  get rid of this since we tossed DynamicJava
        // long ago

        // form parameter list
        String [] parameterNames = new String[arguments.length];
        String [] javaParameterNames = new String[arguments.length];
        Class [] parameterTypes = new Class[arguments.length];
        OJClass [] parameterOjTypes = new OJClass[arguments.length];
        ExpressionList returnDeclList = new ExpressionList();
        for (int i = 0; i < arguments.length; i++) {
            parameterNames[i] = arguments[i].name;
            javaParameterNames[i] = arguments[i].name + "_p";
            parameterTypes[i] = arguments[i].clazz;
            parameterOjTypes[i] = OJClass.forClass(arguments[i].clazz);
        }

        // form the body of the method, and figure out the return type
        OJClass returnType = OJUtil.clazzVoid;
        StatementList statementList = new StatementList();
        for (int i = 0; i < arguments.length; ++i) {
            statementList.add(
                new VariableDeclaration(
                    new ModifierList(ModifierList.FINAL),
                    TypeName.forOJClass(parameterOjTypes[i]),
                    arguments[i].name,
                    new FieldAccess(javaParameterNames[i])));
        }
        if (parseTree instanceof Expression) {
            Expression expression = (Expression) parseTree;
            returnType = OJUtil.getType(env, expression);
            if (!returnType.isPrimitive()) {
                returnType = OJUtil.clazzObject;
            }
            openjava.ptree.Statement statement;
            if (returnType == OJSystem.VOID) {
                statement = new ExpressionStatement(expression);
            } else {
                statement = new ReturnStatement(expression);
            }
            statementList.add(statement);
            returnDeclList = null;
        } else if (parseTree instanceof openjava.ptree.Statement) {
            openjava.ptree.Statement statement =
                (openjava.ptree.Statement) parseTree;
            statementList.add(statement);
            addDecl(statement, returnDeclList);
        } else if (parseTree instanceof StatementList) {
            StatementList newList = (StatementList) parseTree;
            for (int i = 0, count = newList.size(); i < count; i++) {
                addDecl(
                    newList.get(i),
                    returnDeclList);
            }
            statementList.addAll(newList);
        } else {
            throw Util.newInternal("cannot handle a " + parseTree.getClass());
        }
        if (returnDeclList != null) {
            statementList.add(
                new ReturnStatement(
                    new ArrayAllocationExpression(
                        OJClass.forClass(VarDecl.class),
                        new ExpressionList(null),
                        new ArrayInitializer(returnDeclList))));
            returnType = OJClass.arrayOf(OJClass.forClass(VarDecl.class));
        }
        OJSyntheticClass.addMethod(
            decl,
            statementList,
            getTempMethodName(),
            javaParameterNames,
            parameterOjTypes,
            returnType);
        String packageName = getTempPackageName();
        CompilationUnit compUnit =
            new CompilationUnit(
                packageName,
                new String[0],
                new ClassDeclarationList(decl));

        if (queryString != null) {
            // use single line comments to avoid issues with */ in literals
            queryString = queryString.replaceAll("\n", "\n// ");

            // have to escape backslashes, because Java thinks
            // backslash-u means Unicode escape (LDB-141)
            queryString = queryString.replaceAll("\\\\", "\\\\\\\\");
            compUnit.setComment(
                "// " + queryString + "\n");
        }
        String s = compUnit.toString();
        String className = decl.getName();
        packageName = compUnit.getPackage(); // e.g. "abc.def", or null
        return compile(
            packageName,
            className,
            s,
            parameterTypes,
            parameterNames);
    }

    private BoundMethod compile(
        String packageName,
        String className,
        String s,
        Class [] parameterTypes,
        String [] parameterNames)
    {
        try {
            Class clazz = compileClass(packageName, className, s);
            Object o = clazz.newInstance();
            Method method =
                clazz.getDeclaredMethod(
                    getTempMethodName(), parameterTypes);
            return new BoundMethod(o, method, parameterNames);
        } catch (InstantiationException e) {
            throw Util.newInternal(e);
        } catch (IllegalAccessException e) {
            throw Util.newInternal(e);
        } catch (NoSuchMethodException e) {
            throw Util.newInternal(e);
        }
    }

    /**
     * Compile a single class with the given source in the given package.
     *
     * @param packageName package name, if null the className must be fully
     * qualified
     * @param className simple class name unless packageName is null
     * @param source source code for the class
     *
     * @return a Class based on source
     */
    protected Class compileClass(
        String packageName,
        String className,
        String source)
    {
        JavaCompilerArgs args = javaCompiler.getArgs();
        args.clear();
        String initialArgs =
            SaffronProperties.instance().javaCompilerArgs.get();
        if (initialArgs != null) {
            args.setString(initialArgs);
        }
        String fullClassName;
        if (packageName == null) {
            fullClassName = className;
        } else {
            fullClassName = packageName + "." + className;
        }
        String javaFileName =
            Util.replace(fullClassName, ".", Util.fileSeparator) + ".java";
        File javaRoot = new File(getJavaRoot());
        File javaFile = new File(javaRoot, javaFileName);

        boolean writeJavaFile = shouldAlwaysWriteJavaFile();
        javaCompiler.getArgs().setDestdir(javaRoot.getAbsolutePath());
        javaCompiler.getArgs().setFullClassName(fullClassName);
        if (javaCompiler.getArgs().supportsSetSource()) {
            javaCompiler.getArgs().setSource(
                source,
                javaFile.toString());
        } else {
            writeJavaFile = true;
            args.addFile(javaFile.toString());
        }

        if (writeJavaFile) {
            try {
                javaFile.getParentFile().mkdirs(); // make any necessary parent
                                                   // directories
                final boolean print =
                    SaffronProperties.instance().printBeforeCompile.get();
                if (print) {
                    System.out.println(
                        "Compiling " + javaFile + ", class "
                        + fullClassName);
                }
                FileWriter fw = new FileWriter(javaFile);
                fw.write(source);
                fw.close();
            } catch (java.io.IOException e2) {
                throw Util.newInternal(
                    e2,
                    "while writing java file '" + javaFile + "'");
            }
        }

        EigenbaseTrace.getDynamicHandler().get().apply(javaFile, source);

        javaCompiler.compile();
        try {
            return Class.forName(
                fullClassName,
                true,
                javaCompiler.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw Util.newInternal(e);
        }
    }

    /**
     * Returns a relational expression which is to be substituted for an access
     * to a SQL view.
     *
     * @param rowType Row type of the view
     * @param queryString Body of the view
     *
     * @return Relational expression
     */
    public RelNode expandView(RelDataType rowType, String queryString)
    {
        throw new UnsupportedOperationException();
    }


    //~ Inner Classes ----------------------------------------------------------

    /**
     * An <code>Argument</code> supplies a name/value pair to a statement. The
     * class of the argument is usually superfluous, but is necessary when the
     * value is a primitive type (such as <code>int</code>, as opposed to {@link
     * Integer}), or is a superclass of the object's runtime type.
     */
    public static class Argument
    {
        Class clazz;
        Object value;
        String name;

        /**
         * Creates an argument.
         */
        public Argument(
            String name,
            Class clazz,
            Object value)
        {
            this.name = name;
            this.clazz = clazz;
            this.value = value;
        }

        /**
         * Creates an argument whose type is the runtime type of <code>
         * value</code>.
         */
        public Argument(
            String name,
            Object value)
        {
            this.name = name;
            this.clazz = value.getClass();
            this.value = value;
        }

        /**
         * Creates an <code>int</code> argument.
         */
        public Argument(
            String name,
            int value)
        {
            this(
                name,
                java.lang.Integer.TYPE,
                new Integer(value));
        }

        public OJClass getType()
        {
            return OJClass.forClass(clazz);
        }

        public Object getValue()
        {
            return value;
        }

        public String getName()
        {
            return name;
        }
    }

    /**
     * Walks a {@link RelNode} tree and determines if it contains any {@link
     * JavaRel}s.
     *
     * @author Zelaine Fong
     */
    public static class JavaRelFinder
        extends RelVisitor
    {
        private boolean javaRelFound;

        public JavaRelFinder()
        {
            javaRelFound = false;
        }

        public void visit(
            RelNode node,
            int ordinal,
            RelNode parent)
        {
            if (node instanceof JavaRel) {
                javaRelFound = true;
            }
            node.childrenAccept(this);
        }

        public boolean containsJavaRel()
        {
            return javaRelFound;
        }
    }

    public interface CatalogReader
        extends RelOptSchema, SqlValidatorCatalogReader
    {
        PreparingTable getTableForMember(String[] names);

        PreparingTable getTable(String[] names);
    }

    public interface PreparingTable
        extends RelOptTable, SqlValidatorTable
    {
    }
}

// End OJPreparingStmt.java
