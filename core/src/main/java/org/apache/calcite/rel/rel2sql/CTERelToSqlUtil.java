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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.plan.CTEDefinationTrait;
import org.apache.calcite.plan.CTEDefinationTraitDef;
import org.apache.calcite.plan.CTEScopeTrait;
import org.apache.calcite.plan.CTEScopeTraitDef;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWithItem;

import java.util.List;

/**
 * Class to identify whether Rel has CTE trait or not.
 */
public class CTERelToSqlUtil {

  private CTERelToSqlUtil() {
  }

  public static boolean isCteScopeTrait(RelTraitSet relTraitSet) {
    boolean isCteScopeTrait = false;
    RelTrait relTrait = relTraitSet.getTrait(CTEScopeTraitDef.instance);
    if (relTrait != null && relTrait instanceof CTEScopeTrait) {
      if (((CTEScopeTrait) relTrait).isCTEScope()) {
        isCteScopeTrait = true;
      }
    }
    return isCteScopeTrait;
  }

  public static boolean isCteDefinationTrait(RelTraitSet relTraitSet) {
    boolean isCteDefinationTrait = false;
    RelTrait relTrait = relTraitSet.getTrait(CTEDefinationTraitDef.instance);
    if (relTrait != null && relTrait instanceof CTEDefinationTrait) {
      if (((CTEDefinationTrait) relTrait).isCTEDefination()) {
        isCteDefinationTrait = true;
      }
    }
    return isCteDefinationTrait;
  }

  /**
   * This Method fetches and add sqlNodes from sqlSelect node.
   */
  public static List<SqlNode> fetchSqlWithItemNodes(SqlNode sqlSelect, List<SqlNode> sqlNodes) {
    SqlNode sqlNode = ((SqlSelect) sqlSelect).getFrom();
    fetchSqlWithItems(sqlNode, sqlNodes);
    return sqlNodes;
  }

  /**
   * This method fetches sqlWithItem nodes and add to sqlNodes list.
   */
  public static void fetchSqlWithItems(SqlNode sqlNode, List<SqlNode> sqlNodes) {
    if (sqlNode instanceof SqlSelect) {
      fetchSqlWithItemNodes(sqlNode, sqlNodes);
    } else if (sqlNode instanceof SqlBasicCall) {
      fetchfromSqlBasicCall(sqlNode, sqlNodes);
    } else if (sqlNode instanceof SqlJoin) {
      SqlNode leftNode = ((SqlJoin) sqlNode).getLeft();
      SqlNode rightNode = ((SqlJoin) sqlNode).getRight();

      fetchSqlWithItems(leftNode, sqlNodes);
      fetchSqlWithItems(rightNode, sqlNodes);
    } else if (sqlNode instanceof SqlWithItem) {
      fetchFromSqlWithItemNode(sqlNode, sqlNodes);
    }
  }

  /**
   * This method fetches sqlNodes and add to sqlNodes list.
   */
  public static void fetchfromSqlBasicCall(SqlNode sqlNode, List<SqlNode> sqlNodes) {
    SqlNode sqlNodeOPerand = ((SqlBasicCall) sqlNode).operand(0);
    if (sqlNodeOPerand instanceof SqlSelect) {
      fetchSqlWithItemNodes(sqlNodeOPerand, sqlNodes);
    } else if (sqlNodeOPerand instanceof SqlWithItem) {
      fetchFromSqlWithItemNode(sqlNodeOPerand, sqlNodes);
    }
  }

  /**
   * This method fetches sqlNodes from SqlNode having sqlWithItem node and add it to sqlNodes list.
   */
  public static void fetchFromSqlWithItemNode(SqlNode sqlWithItem, List<SqlNode> sqlNodes) {
    fetchSqlWithItems(((SqlWithItem) sqlWithItem).query, sqlNodes);
    updateSqlNode(((SqlWithItem) sqlWithItem).query);
    addSqlWithItemNode((SqlWithItem) sqlWithItem, sqlNodes);

  }

  /**
   * This method fetches sqlNodes from SqlWithItem node and add it to sqlNodes list.
   */
  public static void addSqlWithItemNode(SqlWithItem sqlWithItem, List<SqlNode> sqlNodes) {
    if (sqlNodes.isEmpty()) {
      sqlNodes.add(sqlWithItem);
    } else {
      boolean status = false;
      for (SqlNode sqlWithItemNode : sqlNodes) {
        if (((SqlWithItem) sqlWithItemNode).name.toString()
            .equalsIgnoreCase(sqlWithItem.name.toString())) {
          status = true;
          break;
        }
      }
      if (!status) {
        sqlNodes.add(sqlWithItem);
      }
    }
  }

  /**
   * This method updates SqlNode and add SqlIdentifier object in the place of nested nodes.
   */
  public static void updateSqlNode(SqlNode sqlNode) {
    if (sqlNode != null) {
      SqlSelect sqlSelect = (SqlSelect) sqlNode;
      if (sqlSelect.getFrom() instanceof SqlJoin) {
        SqlNode leftNode = ((SqlJoin) sqlSelect.getFrom()).getLeft();
        SqlNode rightNode = ((SqlJoin) sqlSelect.getFrom()).getRight();

        //Update Left Node
        updateNode(leftNode);
        // Update Right Node
        updateNode(rightNode);

        //if left and right is join again need recursive call
      } else if (sqlSelect.getFrom() instanceof SqlBasicCall) {
        updateNode(sqlSelect.getFrom());
      } else if (sqlSelect.getFrom() instanceof SqlWithItem) {
        sqlSelect.setFrom(((SqlWithItem) sqlSelect.getFrom()).name);
      }
    }
  }

  public static void updateNode(SqlNode sqlNode) {
    if (sqlNode instanceof SqlBasicCall) {
      if (((SqlBasicCall) sqlNode).operand(0) instanceof SqlSelect) {
        updateSqlNode(((SqlBasicCall) sqlNode).operand(0));
      } else if (((SqlBasicCall) sqlNode).operand(0) instanceof SqlWithItem) {
        SqlIdentifier identifier = fetchCTEIdentifier(sqlNode);
        if (identifier != null) {
          ((SqlBasicCall) sqlNode).setOperand(0, identifier);
        }
      }
    }
  }

  public static SqlIdentifier fetchCTEIdentifier(SqlNode sqlNode) {
    SqlIdentifier name = null;
    if ("As".equalsIgnoreCase((((SqlBasicCall) sqlNode).getOperator()).getName())
        && ((SqlBasicCall) sqlNode).operand(0) instanceof SqlWithItem) {
      name = ((SqlWithItem) ((SqlBasicCall) sqlNode).operand(0)).name;
    }
    return name;
  }
}
