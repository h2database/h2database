package org.h2.command.query;

import org.h2.engine.SessionLocal;

import org.h2.table.TableFilter;

import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;

import java.util.*;

/** Homework 5
 * Determines the best join order by the following rules rather than considering every possible permutation.
 * The following are the two rules that need to be upheld:
 * (1) Never choose an order that would introduce a cartesian product join (joining two tables that do not
 *    have an explicit ON clause in the query)
 * (2) Choose the table with the lowest number of roles out of all of the potential next tables to add to our
 *    join order as permitted by rule 1
 *
 * To implement the rule, adjacency graph is used to keep track of all table join conditions to prevent any
 * the introduction of cartesian product joins (1). Building this graph will start from the smallest table (2) and
 * involves extraction of all join conditions.
 *
 * After building the graph, we will recursively do DFS to determine the best join sequence. This involves sorting adjacent
 * tables by row count, to ensure that we are favoring smaller tables.
 */
public class RuleBasedJoinOrderPicker {
    final SessionLocal session;
    final TableFilter[] filters; // array of Table Filter objects / tables in query

    public RuleBasedJoinOrderPicker(SessionLocal session, TableFilter[] filters) {
        this.session = session;
        this.filters = filters;
    }

    public TableFilter[] bestOrder() {
        // Implement rules here:
        Map<TableFilter, List<TableFilter>> adjacencyGraph = buildAdjacencyGraph();

        // Building graph with the smallest table as the starting point
        Optional<TableFilter> smallestTable = Arrays.stream(filters)
                .min((tableA, tableB)
                        -> Long.compare(tableA.getTable().getRowCountApproximation(session),
                        tableB.getTable().getRowCountApproximation(session))
                );

        List<TableFilter> bestJoinOrderList = new ArrayList<>();

        // Traversing built graph
        smallestTable.ifPresent(filter -> DFS(filter, adjacencyGraph, new HashSet<>(), bestJoinOrderList));

        return bestJoinOrderList.toArray(new TableFilter[0]);
    }

    /*
     * Helper function to build adjacency graph where each table is represented as a node
     * */
    private Map<TableFilter, List<TableFilter>> buildAdjacencyGraph() {
        Map<TableFilter, List<TableFilter>> adjacencyGraph = new HashMap<>();
        for (TableFilter filter : filters) {
            adjacencyGraph.put(filter, new ArrayList<>());
        }

        // To ensure each join condition is processed once
        Set<Expression> seenFullJoinConditions = new HashSet<>();
        for (TableFilter filter : filters) {
            Expression fullCondition = filter.getFullCondition();
//            System.out.println("Full condition for table" + filter + ": " + fullCondition);

            if (fullCondition == null) {
                continue;
            }

            if (!seenFullJoinConditions.contains(fullCondition)) {
                extractTableRelationships(fullCondition, adjacencyGraph);
                seenFullJoinConditions.add(fullCondition);
            }
        }
        return adjacencyGraph;
    }

    /*
    * Helper function to extract table relationships from full conditions
    * */
    private void extractTableRelationships(Expression expression, Map<TableFilter, List<TableFilter>> adjacencyGraph) {
        int lenExpression = expression.getSubexpressionCount();
//        System.out.println("expression: " + expression);

        if (expression instanceof ConditionAndOr || expression instanceof ConditionAndOrN) {
            for (int i = 0; i < lenExpression; i++) {
                extractTableRelationships(expression.getSubexpression(i), adjacencyGraph);
            }
        } else if (expression instanceof Comparison) {
            Expression left = expression.getSubexpression(0);
            Expression right = expression.getSubexpression(1);

//            System.out.println("left: " + left);
//            System.out.println("right: " + right);

            if (left instanceof ExpressionColumn && right instanceof ExpressionColumn) {
                TableFilter leftTable = ((ExpressionColumn) left).getTableFilter();
                TableFilter rightTable = ((ExpressionColumn) right).getTableFilter();

                // Add an edge when both columns are from different tables and not null
                if (leftTable != rightTable && leftTable != null && rightTable != null) {
                    if (!adjacencyGraph.get(leftTable).contains(rightTable)) {
                        adjacencyGraph.get(leftTable).add(rightTable);
                    }
                    if (!adjacencyGraph.get(rightTable).contains(leftTable)) {
                        adjacencyGraph.get(rightTable).add(leftTable);
                    }
                }
            }
        }
    }

    /*
     * Helper function to do DFS recursively on sorted adjacent tables in ascending order
     * */
    private void DFS(TableFilter currentTable, Map<TableFilter, List<TableFilter>> adjacencyGraph,
                     Set<TableFilter> visited, List<TableFilter> bestJoinOrder) {
        visited.add(currentTable);
        bestJoinOrder.add(currentTable);

        List<TableFilter> currentAdjacentTables = adjacencyGraph.get(currentTable);

        if (currentAdjacentTables != null) {
            // Sorting in place by tables in ASC order for current table
            currentAdjacentTables.sort(Comparator.comparingLong(
                    adjacentTable -> adjacentTable.getTable().getRowCountApproximation(session)));

            for (TableFilter tableFilter : currentAdjacentTables) {
                if (!visited.contains(tableFilter)) {
                    DFS(tableFilter, adjacencyGraph, visited, bestJoinOrder);
                }
            }
        }
    }

}










