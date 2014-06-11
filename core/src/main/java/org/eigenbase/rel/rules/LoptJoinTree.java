/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.luciddb.optimizer;

import java.util.*;

import org.eigenbase.rel.*;


/**
 * LoptJoinTree implements a utility class used to store a JoinRel tree and the
 * factors that make up the tree. Because RelNodes can be duplicated in a query
 * when you have a self-join, factor ids are needed to distinguish between the
 * different join inputs that correspond to identical tables. The class
 * associates factor ids with a join tree, matching the order of the factor ids
 * with the order of those factors in the join tree.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LoptJoinTree
{
    //~ Instance fields --------------------------------------------------------

    private BinaryTree factorTree;
    private RelNode joinTree;
    private boolean removableSelfJoin;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a jointree consisting of a single node
     *
     * @param joinTree RelNode corresponding to the single node
     * @param factorId factor id of the node
     */
    public LoptJoinTree(RelNode joinTree, int factorId)
    {
        this.joinTree = joinTree;
        factorTree = new BinaryTree(factorId, this);
        this.removableSelfJoin = false;
    }

    /**
     * Associates the factor ids with a jointree
     *
     * @param joinTree RelNodes corresponding to the join tree
     * @param factorTree tree of the factor ids
     * @param removableSelfJoin whether the join corresponds to a removable
     * self-join
     */
    public LoptJoinTree(
        RelNode joinTree,
        BinaryTree factorTree,
        boolean removableSelfJoin)
    {
        this.joinTree = joinTree;
        this.factorTree = factorTree;
        this.removableSelfJoin = removableSelfJoin;
    }

    /**
     * Associates the factor ids with a jointree given the factors corresponding
     * to the left and right subtrees of the join
     *
     * @param joinTree RelNodes corresponding to the join tree
     * @param leftFactorTree tree of the factor ids for left subtree
     * @param rightFactorTree tree of the factor ids for the right subtree
     */
    public LoptJoinTree(
        RelNode joinTree,
        BinaryTree leftFactorTree,
        BinaryTree rightFactorTree)
    {
        this(joinTree, leftFactorTree, rightFactorTree, false);
    }

    /**
     * Associates the factor ids with a jointree given the factors corresponding
     * to the left and right subtrees of the join. Also indicates whether the
     * join is a removable self-join.
     *
     * @param joinTree RelNodes corresponding to the join tree
     * @param leftFactorTree tree of the factor ids for left subtree
     * @param rightFactorTree tree of the factor ids for the right subtree
     * @param removableSelfJoin true if the join is a removable self-join
     */
    public LoptJoinTree(
        RelNode joinTree,
        BinaryTree leftFactorTree,
        BinaryTree rightFactorTree,
        boolean removableSelfJoin)
    {
        factorTree = new BinaryTree(leftFactorTree, rightFactorTree, this);
        this.joinTree = joinTree;
        this.removableSelfJoin = removableSelfJoin;
    }

    //~ Methods ----------------------------------------------------------------

    public RelNode getJoinTree()
    {
        return joinTree;
    }

    public LoptJoinTree getLeft()
    {
        return new LoptJoinTree(
            ((JoinRel) joinTree).getLeft(),
            factorTree.getLeft(),
            factorTree.getLeft().getParent().isRemovableSelfJoin());
    }

    public LoptJoinTree getRight()
    {
        return new LoptJoinTree(
            ((JoinRel) joinTree).getRight(),
            factorTree.getRight(),
            factorTree.getRight().getParent().isRemovableSelfJoin());
    }

    public BinaryTree getFactorTree()
    {
        return factorTree;
    }

    public void getTreeOrder(List<Integer> treeOrder)
    {
        factorTree.getTreeOrder(treeOrder);
    }

    public boolean isRemovableSelfJoin()
    {
        return removableSelfJoin;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Simple binary tree class that stores an id in the leaf nodes and keeps
     * track of the parent LoptJoinTree object associated with the binary tree.
     */
    protected class BinaryTree
    {
        private int id;
        private BinaryTree left;
        private BinaryTree right;
        private LoptJoinTree parent;

        public BinaryTree(int rootId, LoptJoinTree parent)
        {
            this.id = rootId;
            this.left = null;
            this.right = null;
            this.parent = parent;
        }

        public BinaryTree(
            BinaryTree left,
            BinaryTree right,
            LoptJoinTree parent)
        {
            this.left = left;
            this.right = right;
            this.parent = parent;
        }

        public BinaryTree getLeft()
        {
            return left;
        }

        public BinaryTree getRight()
        {
            return right;
        }

        public LoptJoinTree getParent()
        {
            return parent;
        }

        /**
         * @return the id associated with a leaf node in a binary tree
         */
        public int getId()
        {
            assert ((left == null) && (right == null));
            return id;
        }

        public void getTreeOrder(List<Integer> treeOrder)
        {
            if ((left == null) || (right == null)) {
                treeOrder.add(id);
            } else {
                left.getTreeOrder(treeOrder);
                right.getTreeOrder(treeOrder);
            }
        }
    }
}

// End LoptJoinTree.java
