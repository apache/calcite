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
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
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

  public static boolean isCTEScopeOrDefinitionTrait(RelTraitSet relTraitSet) {
    return isCteScopeTrait(relTraitSet) || isCteDefinationTrait(relTraitSet);
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
      fetchFromSqlBasicCall(sqlNode, sqlNodes);
    } else if (sqlNode instanceof SqlJoin) {
      SqlNode leftNode = ((SqlJoin) sqlNode).getLeft();
      SqlNode rightNode = ((SqlJoin) sqlNode).getRight();

      fetchSqlWithItems(leftNode, sqlNodes);
      fetchSqlWithItems(rightNode, sqlNodes);
    } else if (sqlNode instanceof SqlWithItem) {
      fetchFromSqlWithItemNode(sqlNode, sqlNodes);
    } else if (sqlNode instanceof SqlWith) {
      if (((SqlWith) sqlNode).withList.size() > 0) {
        fetchSqlWithItems(((SqlWith) sqlNode).withList.get(0), sqlNodes);
      }
    }
  }

  private static boolean isNestedCte(SqlNode query) {
    if (query instanceof SqlWithItem) {
      return true;
    } else if (query instanceof SqlSelect) {
      SqlNode fromNode = ((SqlSelect) query).getFrom();
      return fromNode instanceof SqlWithItem || isNestedCte(fromNode); // Recursive check
    } else if (query instanceof SqlBasicCall) {
      return ((SqlBasicCall) query).getOperandList().stream()
          .anyMatch(CTERelToSqlUtil::isNestedCte);
    }
    return false;
  }

  /**
   * This method fetches sqlNodes and add to sqlNodes list.
   */
  public static void fetchFromSqlBasicCall(SqlNode sqlNode, List<SqlNode> sqlNodes) {
    if (sqlNode instanceof SqlBasicCall) {
      SqlBasicCall basicCall = (SqlBasicCall) sqlNode;
      for (SqlNode operand : basicCall.getOperandList()) {
        processSqlNode(operand, sqlNodes);
      }
    }
  }

  private static void processSqlNode(SqlNode sqlNode, List<SqlNode> sqlNodes) {
    if (sqlNode instanceof SqlSelect) {
      fetchSqlWithItemNodes(sqlNode, sqlNodes);
    } else if (sqlNode instanceof SqlBasicCall) {
      fetchFromSqlBasicCall(sqlNode, sqlNodes);
    } else if (sqlNode instanceof SqlWithItem) {
      fetchFromSqlWithItemNode(sqlNode, sqlNodes);
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
      if (sqlNode instanceof SqlSelect) {
        SqlSelect sqlSelect = (SqlSelect) sqlNode;
        // Handle FROM clause
        SqlNode fromNode = sqlSelect.getFrom();
        processFromNode(sqlSelect, fromNode);
        if (isNestedCte(fromNode)
            &&
            fromNode instanceof SqlBasicCall
            &&
            ((SqlBasicCall) fromNode).getOperator() instanceof SqlAsOperator) {
          updateNode(fromNode);
        }
        SqlNode whereNode = sqlSelect.getWhere();
        if (whereNode instanceof SqlBasicCall) {
          updateNode(whereNode);
        }
      }
    }
  }

  private static void processFromNode(SqlNode sqlNode, SqlNode fromNode) {
    if (fromNode instanceof SqlJoin) {
      updateSqlJoinNode((SqlJoin) fromNode);
    } else if (fromNode instanceof SqlBasicCall) {
      processBasicCall((SqlBasicCall) fromNode);
    } else if (fromNode instanceof SqlWithItem) {
      SqlNode query = ((SqlWithItem) fromNode).query;

      if (isNestedCte(query)) {
        processWithItem((SqlWithItem) fromNode);
      } else {
        ((SqlSelect) sqlNode).setFrom(((SqlWithItem) fromNode).name);
      }
    }
  }

  private static void processBasicCall(SqlNode sqlNode) {
    if (sqlNode instanceof SqlBasicCall) {
      if (isNestedCte(sqlNode)) {
        SqlBasicCall basicCall = (SqlBasicCall) sqlNode;

        for (SqlNode operand : basicCall.getOperandList()) {
          if (operand instanceof SqlSelect) {
            updateSqlNode(operand);
          } else if (operand instanceof SqlWithItem) {
            processWithItem((SqlWithItem) operand);
          } else if (operand instanceof SqlBasicCall) {
            processBasicCall(operand);
          }
        }
      } else {
        updateNode(sqlNode);
      }
    }
  }

  private static void processWithItem(SqlWithItem withItem) {
    SqlNode query = withItem.query;

    if (query instanceof SqlSelect) {
      updateSqlNode(query);
    } else if (query instanceof SqlBasicCall) {
      processBasicCall(query);
    }
  }

  public static void updateSqlJoinNode(SqlJoin sqlJoin) {
    updateNodeOrJoin(sqlJoin.getLeft());
    updateNodeOrJoin(sqlJoin.getRight());
  }

  private static void updateNodeOrJoin(SqlNode node) {
    if (node instanceof SqlJoin) {
      updateSqlJoinNode((SqlJoin) node);
    } else if (node instanceof SqlBasicCall) {
      updateNode(node);
    } else {
      updateSqlNode(node);
    }
  }

  public static void updateNode(SqlNode sqlNode) {
    SqlBasicCall basicCall = (SqlBasicCall) sqlNode;
    if (basicCall.getOperator() instanceof SqlBinaryOperator) {
      for (SqlNode operand : basicCall.getOperandList()) {
        if (operand instanceof SqlBasicCall) {
          handleBasicCallOperand((SqlBasicCall) operand);
        } else if (operand instanceof SqlSelect) {
          updateSqlNode(operand);
        }
      }
    } else {
      SqlNode operand = basicCall.operand(0);
      handleOperand(sqlNode, operand);
    }
  }

  private static void handleOperand(SqlNode parentNode, SqlNode operand) {
    if (operand instanceof SqlBasicCall) {
      handleBasicCallOperand((SqlBasicCall) operand);
    } else if (operand instanceof SqlSelect) {
      updateSqlNode(operand);
    } else if (operand instanceof SqlWithItem) {
      SqlIdentifier identifier = fetchCTEIdentifier(parentNode);
      if (identifier != null) {
        ((SqlBasicCall) parentNode).setOperand(0, identifier);
      }
    }
  }

  private static void handleBasicCallOperand(SqlBasicCall basicCall) {
    basicCall.getOperandList().forEach(operand -> handleOperand(basicCall, operand));
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
