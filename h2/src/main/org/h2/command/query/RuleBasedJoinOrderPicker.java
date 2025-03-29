package org.h2.command.query;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.table.TableFilter;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Determines the best join order by following rules rather than considering every possible permutation.
 */
public class RuleBasedJoinOrderPicker {
 final SessionLocal session;
 final TableFilter[] filters;

 public RuleBasedJoinOrderPicker(SessionLocal session, TableFilter[] filters) {
  this.session = session;
  this.filters = filters;
 }

 public TableFilter[] bestOrder() {
  List<TableFilter> remaining = new ArrayList<>(List.of(filters));
  List<TableFilter> result = new ArrayList<>();

  Expression condition = filters[0].getFullCondition(); // assume same for all
  Map<String, Set<String>> joinGraph = buildJoinGraph(condition);

  TableFilter start = pickMinTableFilter(remaining);
  result.add(start);
  remaining.remove(start);

  while (!remaining.isEmpty()) {
   TableFilter next = pickNextJoinable(result, remaining, joinGraph);
   if (next == null) {
    throw new RuntimeException("No valid join order found without cartesian product");
   }
   result.add(next);
   remaining.remove(next);
  }

  return result.toArray(new TableFilter[0]);
 }

 private Map<String, Set<String>> buildJoinGraph(Expression condition) {
  Map<String, Set<String>> graph = new HashMap<>();
  if (condition == null) return graph;

  List<Expression> comparisons = flattenAndConditions(condition);
  for (Expression expr : comparisons) {
   if (expr instanceof Comparison) {
    Comparison cmp = (Comparison) expr;
    Expression left = cmp.getSubexpression(0);
    Expression right = cmp.getSubexpression(1);
    if (left instanceof ExpressionColumn && right instanceof ExpressionColumn) {
     String table1 = ((ExpressionColumn) left).getTableName();
     String table2 = ((ExpressionColumn) right).getTableName();
     if (table1 != null && table2 != null && !table1.equals(table2)) {
      graph.computeIfAbsent(table1, k -> new HashSet<>()).add(table2);
      graph.computeIfAbsent(table2, k -> new HashSet<>()).add(table1);
     }
    }
   }
  }
  return graph;
 }

 private List<Expression> flattenAndConditions(Expression expr) {
  List<Expression> result = new ArrayList<>();
  if (expr instanceof ConditionAndOr) {
   ConditionAndOr and = (ConditionAndOr) expr;
   result.addAll(flattenAndConditions(and.getSubexpression(0)));
   result.addAll(flattenAndConditions(and.getSubexpression(1)));
  } else if (expr instanceof ConditionAndOrN) {
   for (int i = 0; i < expr.getSubexpressionCount(); i++) {
    result.addAll(flattenAndConditions(expr.getSubexpression(i)));
   }
  } else {
   result.add(expr);
  }
  return result;
 }

 private TableFilter pickNextJoinable(List<TableFilter> current, List<TableFilter> remaining, Map<String, Set<String>> graph) {
  Set<String> joined = current.stream()
          .map(f -> f.getTable().getName())
          .collect(Collectors.toSet());

  return remaining.stream()
          .filter(f -> {
           String name = f.getTable().getName();
           Set<String> neighbors = graph.getOrDefault(name, Set.of());
           return !Collections.disjoint(neighbors, joined);
          })
          .min(Comparator.comparingLong(f -> f.getTable().getRowCountApproximation(session)))
          .orElse(null);
 }

 private TableFilter pickMinTableFilter(List<TableFilter> filters) {
  return filters.stream()
          .min(Comparator.comparingLong(f -> f.getTable().getRowCountApproximation(session)))
          .orElseThrow();
 }
}
