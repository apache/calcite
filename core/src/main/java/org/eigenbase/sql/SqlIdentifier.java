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
package org.eigenbase.sql;

import java.util.*;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;

/**
 * A <code>SqlIdentifier</code> is an identifier, possibly compound.
 */
public class SqlIdentifier
    extends SqlNode
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Array of the components of this compound identifier.
     *
     * <p>It's convenient to have this member public, and it's convenient to
     * have this member not-final, but it's a shame it's public and not-final.
     * If you assign to this member, please use {@link #setNames(String[],
     * SqlParserPos[])}. And yes, we'd like to make identifiers immutable one
     * day.
     */
    public String [] names;

    /**
     * This identifier's collation (if any).
     */
    SqlCollation collation;

    /**
     * A list of the positions of the components of compound identifiers.
     */
    private SqlParserPos [] componentPositions;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a compound identifier, for example <code>foo.bar</code>.
     *
     * @param names Parts of the identifier, length &gt;= 1
     */
    public SqlIdentifier(
        String [] names,
        SqlCollation collation,
        SqlParserPos pos,
        SqlParserPos [] componentPositions)
    {
        super(pos);
        this.names = names;
        this.collation = collation;
        this.componentPositions = componentPositions;
        for (String name : names) {
            assert name != null;
        }
    }

    public SqlIdentifier(
        String [] names,
        SqlParserPos pos)
    {
        this(names, null, pos, null);
    }

    /**
     * Creates a simple identifier, for example <code>foo</code>, with a
     * collation.
     */
    public SqlIdentifier(
        String name,
        SqlCollation collation,
        SqlParserPos pos)
    {
        this(
            new String[] { name },
            collation,
            pos,
            null);
    }

    /**
     * Creates a simple identifier, for example <code>foo</code>.
     */
    public SqlIdentifier(
        String name,
        SqlParserPos pos)
    {
        this(
            new String[] { name },
            null,
            pos,
            null);
    }

    //~ Methods ----------------------------------------------------------------

    public SqlKind getKind()
    {
        return SqlKind.IDENTIFIER;
    }

    public SqlNode clone(SqlParserPos pos)
    {
        return new SqlIdentifier(
            names.clone(),
            collation,
            pos,
            componentPositions);
    }

    public String toString()
    {
        // Short-circuit for common case.
        if (names.length == 1) {
            return names[0];
        }
        StringBuilder buf = new StringBuilder(names[0]);
        for (int i = 1; i < names.length; i++) {
            buf.append('.');
            buf.append(names[i]);
        }
        return buf.toString();
    }

    /**
     * Modifies the components of this identifier and their positions.
     *
     * @param names Names of components
     * @param poses Positions of components
     *
     * @deprecated Identifiers should be immutable
     */
    public void setNames(String [] names, SqlParserPos [] poses)
    {
        this.names = names;
        this.componentPositions = poses;
    }

    /**
     * Returns the position of the <code>i</code>th component of a compound
     * identifier, or the position of the whole identifier if that information
     * is not present.
     *
     * @param i Ordinal of component.
     *
     * @return Position of i'th component
     */
    public SqlParserPos getComponentParserPosition(int i)
    {
        assert (i >= 0) && (i < names.length);
        return (componentPositions == null) ? getParserPosition()
            : componentPositions[i];
    }

    /**
     * Copies names and components from another identifier. Does not modify the
     * cross-component parser position.
     *
     * @param other identifier from which to copy
     */
    public void assignNamesFrom(SqlIdentifier other)
    {
        setNames(other.names, other.componentPositions);
    }

    /**
     * Creates an identifier which contains only the <code>ordinal</code>th
     * component of this compound identifier. It will have the correct {@link
     * SqlParserPos}, provided that detailed position information is available.
     */
    public SqlIdentifier getComponent(int ordinal)
    {
        return new SqlIdentifier(
            names[ordinal],
            getComponentParserPosition(ordinal));
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        final SqlWriter.Frame frame =
            writer.startList(SqlWriter.FrameTypeEnum.Identifier);
        for (String name : names) {
            writer.sep(".");
            if (name.equals("*")) {
                writer.print(name);
            } else {
                writer.identifier(name);
            }
        }

        if (null != collation) {
            collation.unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
    }

    public void validate(SqlValidator validator, SqlValidatorScope scope)
    {
        validator.validateIdentifier(this, scope);
    }

    public void validateExpr(SqlValidator validator, SqlValidatorScope scope)
    {
        // First check for builtin functions which don't have parentheses,
        // like "LOCALTIME".
        SqlCall call =
            SqlUtil.makeCall(
                validator.getOperatorTable(),
                this);
        if (call != null) {
            validator.validateCall(call, scope);
            return;
        }

        validator.validateIdentifier(this, scope);
    }

    public boolean equalsDeep(SqlNode node, boolean fail)
    {
        if (!(node instanceof SqlIdentifier)) {
            assert !fail : this + "!=" + node;
            return false;
        }
        SqlIdentifier that = (SqlIdentifier) node;
        if (this.names.length != that.names.length) {
            assert !fail : this + "!=" + node;
            return false;
        }
        for (int i = 0; i < names.length; i++) {
            if (!this.names[i].equals(that.names[i])) {
                assert !fail : this + "!=" + node;
                return false;
            }
        }
        return true;
    }

    public <R> R accept(SqlVisitor<R> visitor)
    {
        return visitor.visit(this);
    }

    public SqlCollation getCollation()
    {
        return collation;
    }

    public String getSimple()
    {
        assert (names.length == 1);
        return names[0];
    }

    /**
     * Returns whether this identifier is a star, such as "*" or "foo.bar.*".
     */
    public boolean isStar()
    {
        return names[names.length - 1].equals("*");
    }

    /**
     * Returns whether this is a simple identifier. "FOO" is simple; "*",
     * "FOO.*" and "FOO.BAR" are not.
     */
    public boolean isSimple()
    {
        return (names.length == 1) && !names[0].equals("*");
    }

    public SqlMonotonicity getMonotonicity(SqlValidatorScope scope)
    {
        // First check for builtin functions which don't have parentheses,
        // like "LOCALTIME".
        final SqlValidator validator = scope.getValidator();
        SqlCall call =
            SqlUtil.makeCall(
                validator.getOperatorTable(),
                this);
        if (call != null) {
            return call.getMonotonicity(scope);
        }
        final SqlIdentifier fqId = scope.fullyQualify(this);
        final SqlValidatorNamespace ns =
            SqlValidatorUtil.lookup(
                scope,
                Arrays.asList(fqId.names).subList(0, fqId.names.length - 1));
        return ns.getMonotonicity(fqId.names[fqId.names.length - 1]);
    }

    public boolean equalsBaseName(String name)
    {
        return names[names.length - 1].equals(name);
    }
}

// End SqlIdentifier.java
