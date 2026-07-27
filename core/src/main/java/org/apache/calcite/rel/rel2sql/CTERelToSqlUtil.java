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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlUnpivot;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class to identify whether Rel has CTE trait or not.
 */
public class CTERelToSqlUtil {

  private CTERelToSqlUtil() {
  }

  public static boolean isCteScopeTrait(RelTraitSet relTraitSet) {
    RelTrait relTrait = relTraitSet.getTrait(CTEScopeTraitDef.instance);
    return relTrait instanceof CTEScopeTrait && ((CTEScopeTrait) relTrait).isCTEScope();
  }

  public static boolean isCTEScopeOrDefinitionTrait(RelTraitSet relTraitSet) {
    return isCteScopeTrait(relTraitSet) || isCteDefinationTrait(relTraitSet);
  }

  public static boolean isCteDefinationTrait(RelTraitSet relTraitSet) {
    RelTrait relTrait = relTraitSet.getTrait(CTEDefinationTraitDef.instance);
    return relTrait instanceof CTEDefinationTrait
        && ((CTEDefinationTrait) relTrait).isCTEDefination();
  }

  /**
   * This Method fetches and add sqlNodes from sqlSelect node.
   */
  public static List<SqlNode> fetchSqlWithItemNodes(SqlNode sqlSelect, List<SqlNode> sqlNodes) {
    if (sqlSelect instanceof SqlBasicCall) {
      fetchFromSqlBasicCall(sqlSelect, sqlNodes);
    } else if (sqlSelect instanceof SqlSelect && ((SqlSelect) sqlSelect).getFrom() != null) {
      fetchSqlWithItems(((SqlSelect) sqlSelect).getFrom(), sqlNodes);
    }
    if (sqlSelect instanceof SqlSelect && !((SqlSelect) sqlSelect).getSelectList().isEmpty()) {
      fetchSqlWithSelectList(((SqlSelect) sqlSelect).getSelectList(), sqlNodes);
    }
    if (sqlSelect instanceof SqlSelect && ((SqlSelect) sqlSelect).getWhere() != null) {
      fetchSqlWithSelectList(Arrays.asList(((SqlSelect) sqlSelect).getWhere()), sqlNodes);
    }
    if (sqlSelect instanceof SqlDelete && ((SqlDelete) sqlSelect).getSourceSelect() != null) {
      fetchSqlWithItems(((SqlDelete) sqlSelect).getSourceSelect(), sqlNodes);
    }
    if (sqlSelect instanceof SqlUpdate && ((SqlUpdate) sqlSelect).getSourceSelect() != null) {
      fetchSqlWithItems(((SqlUpdate) sqlSelect).getSourceSelect(), sqlNodes);
    }
    return sqlNodes;
  }

  public static void fetchSqlWithSelectList(List<SqlNode> selectItems, List<SqlNode> sqlNodes) {
    selectItems.stream().filter(item -> item instanceof SqlBasicCall)
        .forEach(item -> fetchFromSqlBasicCall(item, sqlNodes));
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
    } else if (sqlNode instanceof SqlUnpivot) {
      SqlUnpivot unpivot = (SqlUnpivot) sqlNode;
      fetchSqlWithItems(unpivot.query, sqlNodes);
    } else if (sqlNode instanceof SqlPivot) {
      SqlPivot pivot = (SqlPivot) sqlNode;
      fetchSqlWithItems(pivot.query, sqlNodes);
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
    } else if (sqlNode instanceof SqlPivot) {
      fetchFromSqlWithItemNode(((SqlPivot) sqlNode).query, sqlNodes);
    } else if (sqlNode instanceof SqlCase) {
      fetchSqlWithSelectList(((SqlCase) sqlNode).getWhenOperands(), sqlNodes);
      fetchSqlWithSelectList(((SqlCase) sqlNode).getThenOperands(), sqlNodes);
    }
  }

  /**
   * This method fetches sqlNodes from SqlNode having sqlWithItem node and add it to sqlNodes list.
   */
  public static void fetchFromSqlWithItemNode(SqlNode sqlWithItem, List<SqlNode> sqlNodes) {
    if ((sqlWithItem instanceof SqlBasicCall)
        && (((SqlBasicCall) sqlWithItem).operand(0)) instanceof SqlWithItem) {
      sqlWithItem = ((SqlBasicCall) sqlWithItem).operand(0);
    }
    fetchSqlWithItems(((SqlWithItem) sqlWithItem).query, sqlNodes);
    updateSqlNode(((SqlWithItem) sqlWithItem).query);
    addSqlWithItemNode((SqlWithItem) sqlWithItem, sqlNodes);

  }

  /**
   * This method fetches sqlNodes from SqlWithItem node and add it to sqlNodes list.
   */
  public static void addSqlWithItemNode(SqlWithItem sqlWithItem, List<SqlNode> sqlNodes) {
    if (sqlWithItem.query instanceof SqlWith) {
      SqlWith innerWith = (SqlWith) sqlWithItem.query;
      sqlNodes.removeIf(node -> {
        SqlWithItem existingNode = (SqlWithItem) node;
        for (SqlNode innerNode : innerWith.withList) {
          if (innerNode.equals(existingNode)) {
            return true;
          }
        }
        return false;
      });
    }
    for (SqlNode sqlWithItemNode : sqlNodes) {
      if (((SqlWithItem) sqlWithItemNode).name.toString()
          .equalsIgnoreCase(sqlWithItem.name.toString())) {
        return;
      }
    }
    sqlNodes.add(sqlWithItem);
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
        if (!sqlSelect.getSelectList().isEmpty()) {
          sqlSelect.getSelectList().stream().filter(item -> item instanceof SqlBasicCall)
              .forEach(CTERelToSqlUtil::updateNode);
        }
      } else if (sqlNode instanceof SqlBasicCall
          && ((SqlBasicCall) sqlNode).getOperator() instanceof SqlSetOperator) {
        SqlBasicCall setOpCall = (SqlBasicCall) sqlNode;
        for (SqlNode operand : setOpCall.getOperandList()) {
          updateSqlNode(operand);
        }
      } else if (sqlNode instanceof SqlDelete) {
        SqlDelete sqlDelete = (SqlDelete) sqlNode;
        // Handle targetTable
        SqlNode targetTable = sqlDelete.getTargetTable();
        processFromNode(sqlDelete, targetTable);
        if (isNestedCte(targetTable)
            && targetTable instanceof SqlBasicCall
            && ((SqlBasicCall) targetTable).getOperator() instanceof SqlAsOperator) {
          updateNode(targetTable);
        }
        if (sqlDelete.getCondition() != null) {
          updateNode(sqlDelete.getCondition());
        }
      } else if (sqlNode instanceof SqlUpdate) {
        SqlUpdate sqlUpdate = (SqlUpdate) sqlNode;
        // Handle targetTable
        SqlNode targetTable = sqlUpdate.getTargetTable();
        processFromNode(sqlUpdate, targetTable);
        if (sqlUpdate.getSourceSelect() != null) {
          updateSqlNode(sqlUpdate.getSourceSelect());
        }
        if (isNestedCte(targetTable)
            && targetTable instanceof SqlBasicCall
            && ((SqlBasicCall) targetTable).getOperator() instanceof SqlAsOperator) {
          updateNode(targetTable);
        }
        if (sqlUpdate.getCondition() != null) {
          updateNode(sqlUpdate.getCondition());
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
    } else if (fromNode instanceof SqlUnpivot) {
      // Replace inline CTE body in the UNPIVOT's query with just the CTE identifier
      // (or AS(identifier, alias) when the CTE reference carries a table alias).
      SqlNode resolvedQuery = resolveUnpivotCteQuery(((SqlUnpivot) fromNode).query);
      if (resolvedQuery != null) {
        SqlUnpivot unpivot = (SqlUnpivot) fromNode;
        SqlUnpivot unpivotNode =
            new SqlUnpivot(unpivot.getParserPosition(), resolvedQuery, unpivot.includeNulls,
                unpivot.measureList, unpivot.axisList, unpivot.inList);

        ((SqlSelect) sqlNode).setFrom(unpivotNode);
      }
    } else if (fromNode instanceof SqlPivot) {
      // Replace inline CTE body in the UNPIVOT's query with just the CTE identifier
      // (or AS(identifier, alias) when the CTE reference carries a table alias).
      SqlNode resolvedQuery = resolveUnpivotCteQuery(((SqlPivot) fromNode).query);
      if (resolvedQuery != null) {
        SqlPivot pivot = (SqlPivot) fromNode;
        SqlPivot pivotNode =
            new SqlPivot(pivot.getParserPosition(), resolvedQuery, pivot.aggList,
                pivot.axisList, pivot.inList);

        ((SqlSelect) sqlNode).setFrom(pivotNode);
      }
    }
  }

  /**
   * Returns the replacement query node for an UNPIVOT whose source is a CTE reference,
   * or {@code null} if no substitution is needed.
   *
   * <ul>
   *   <li>Unaliased ({@code FROM cte UNPIVOT}): query is {@link SqlWithItem}; returns
   *       the CTE {@link SqlIdentifier}.</li>
   *   <li>Aliased ({@code FROM cte alias UNPIVOT}): query is {@code AS(SqlWithItem, alias)};
   *       returns {@code AS(SqlIdentifier, alias)}.</li>
   * </ul>
   */
  private static SqlNode resolveUnpivotCteQuery(SqlNode query) {
    if (query instanceof SqlWithItem) {
      return ((SqlWithItem) query).name;
    }
    if (query instanceof SqlBasicCall
        && ((SqlBasicCall) query).operand(0) instanceof SqlWithItem) {
      SqlBasicCall asCall = (SqlBasicCall) query;
      SqlIdentifier identifier = ((SqlWithItem) asCall.operand(0)).name;
      return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, identifier, asCall.operand(1));
    }
    return null;
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
    if (basicCall.getOperator() instanceof SqlBinaryOperator
        || basicCall.getOperator().getKind() == SqlKind.BETWEEN) {
      for (SqlNode operand : basicCall.getOperandList()) {
        if (operand instanceof SqlBasicCall) {
          handleBasicCallOperand((SqlBasicCall) operand);
        } else if (operand instanceof SqlSelect) {
          updateSqlNode(operand);
        } else if (operand instanceof SqlCase) {
          ((SqlCase) operand).getWhenOperands().forEach(CTERelToSqlUtil::processBasicCall);
        }
      }
    } else {
      for (SqlNode operand : basicCall.getOperandList()) {
        handleOperand(sqlNode, operand);
      }
    }
  }

  private static void handleOperand(SqlNode parentNode, SqlNode operand) {
    if (operand instanceof SqlBasicCall) {
      handleBasicCallOperand((SqlBasicCall) operand);
    } else if (operand instanceof SqlSelect) {
      updateSqlNode(operand);
    } else if (operand instanceof SqlPivot && ((SqlPivot) operand).query instanceof SqlWithItem) {
      ((SqlPivot) ((SqlBasicCall) parentNode).getOperandList().get(0)).setOperand(0,
          ((SqlWithItem) ((SqlPivot) operand).query).name);
    } else if (operand instanceof SqlPivot && (((SqlPivot) operand).query instanceof SqlBasicCall)
        && ((SqlBasicCall) ((SqlPivot) operand).query).operand(0) instanceof SqlWithItem) {
      ((SqlPivot) ((SqlBasicCall) parentNode).getOperandList().get(0)).setOperand(0,
          ((SqlWithItem) ((SqlBasicCall) ((SqlPivot) operand).query).operand(0)).name);
    } else if (operand instanceof SqlWithItem) {
      SqlIdentifier identifier = fetchCTEIdentifier(parentNode);
      if (identifier != null) {
        ((SqlBasicCall) parentNode).setOperand(0, identifier);
      }
    } else if (operand instanceof SqlCase) {
      ((SqlCase) operand).getWhenOperands().forEach(CTERelToSqlUtil::processBasicCall);
      ((SqlCase) operand).getThenOperands().forEach(CTERelToSqlUtil::processBasicCall);
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

  public static SqlWith modifyWithNode(SqlWith sqlWith) {
    SqlNodeList withItemList = (SqlNodeList) sqlWith.getOperandList().get(0);
    SqlNodeList modifiedList = modifyWithItemList(withItemList);
    return new SqlWith(sqlWith.getParserPosition(), modifiedList, sqlWith.body);
  }

  /**
   * Rebuilds the WITH-item list, dropping redundant nested WITH items.
   *
   * <p>The rebuild only runs for non-first CTEs. When an item is rebuilt, the comments
   * captured around its CTE name (e.g. a comment before the 2nd+ CTE) are copied onto the
   * replacement {@link SqlWithItem}; otherwise those name comments would be dropped.
   */
  private static SqlNodeList modifyWithItemList(SqlNodeList modeList) {
    List<String> names = new ArrayList<>();
    List<SqlNode> modifiedList = new ArrayList<>();

    for (SqlNode node : modeList) {
      SqlWithItem withItem = (SqlWithItem) node;
      String name = withItem.name.names.get(0);
      SqlNode query = withItem.query;
      if (!names.isEmpty()) {
        SqlNode modifiedQuery = query.accept(new SqlShuttle() {
          @Override public SqlNode visit(SqlCall call) {
            switch (call.getKind()) {
            case WITH:
              return removingRedundantWithItems(call, names);
            default:
              return super.visit(call);
            }
          }
        });
        SqlWithItem updatedItem =
            new SqlWithItem(SqlParserPos.ZERO, withItem.name, withItem.columnList, modifiedQuery);
        updatedItem.setCommentList(withItem.getCommentList());
        modifiedList.add(updatedItem);
        names.add(name);
      } else {
        names.add(name);
        modifiedList.add(withItem);
      }
    }
    return new SqlNodeList(modifiedList, SqlParserPos.ZERO);
  }

  private static SqlNode removingRedundantWithItems(SqlNode sqlCall, List<String> existNames) {
    SqlWith sqlWith = (SqlWith) sqlCall;
    List<SqlNode> nodeList = new ArrayList<>();
    for (SqlNode node : (SqlNodeList) sqlWith.getOperandList().get(0)) {
      SqlWithItem item = (SqlWithItem) node;
      String itemName = item.name.names.get(0);
      boolean isExists = existNames.stream().anyMatch(n -> n.equals(itemName));
      if (!isExists) {
        nodeList.add(item);
      }
    }
    if (nodeList.isEmpty()) {
      return sqlWith.body;
    }
    return new SqlWith(SqlParserPos.ZERO, new SqlNodeList(nodeList, SqlParserPos.ZERO),
        sqlWith.body);
  }
}
