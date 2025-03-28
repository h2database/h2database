package org.h2.command.query;

import org.h2.engine.SessionLocal;
import org.h2.table.TableFilter;
import org.h2.expression.Expression;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        TableFilter[] list = new TableFilter[filters.length];
        List<TableFilter> tableFilterList = new ArrayList<>(Arrays.asList(filters));


//        Arrays.sort(filtersSorted, (a, b) -> Long.compare(a.getTable().getRowCountApproximation(session), b.getTable().getRowCountApproximation(session)));

        String conditionString = filters[0].getFullCondition().toString(); // all filters return the same condition
        List<String> conditions = new ArrayList<>();
        // get content inside parenthesis
        Pattern pattern = Pattern.compile("\\(([^)]+)\\)");
        Matcher matcher = pattern.matcher(conditionString);
        // get a list of conditions
        while (matcher.find()) {
            String conditionInsideParentheses = matcher.group(1);   // only get inside of parenthesis
            conditions.add(conditionInsideParentheses);
            System.out.println("Condition added: " + conditionInsideParentheses);
        }

        List<TableFilter> toRevisit = new ArrayList<>();                   // store Cartesian products here
        int insertIndex = 0;
        // add tables with lowest row count first that are not cartesian products
        tableFilterList.sort((a, b) -> Long.compare(a.getTable().getRowCountApproximation(session), b.getTable().getRowCountApproximation(session)));
        while(!tableFilterList.isEmpty()) {
            // if table is not in the condition it is a cartesian product
            String tableName = tableFilterList.get(0).getTable().getName().replace("PUBLIC.", "");
            boolean conditionSatisfied = false;
            if (insertIndex > 0 && !conditions.isEmpty()) {
                for (String condition : conditions) {
                    String tableNamePrev = list[insertIndex - 1].getTable().getName().replace("PUBLIC.", "");
                    if (condition.contains(tableName) &&  condition.contains(tableNamePrev)) {
                        conditionSatisfied = true;
                        System.out.println("Conditions is satisfied: " + conditionSatisfied);
                        break;
                    }
                }
            }
            if (insertIndex  == 0){
                conditionSatisfied = true;
            }
            if (conditionSatisfied) {
                System.out.println("Filter: " + tableName + " is in " + conditionString);
                list[insertIndex] = tableFilterList.get(0);
                insertIndex++;
                tableFilterList.remove(0);
            } else {
                System.out.println("Filter: " + tableName + " is NOT in " + conditionString);
                TableFilter temp = tableFilterList.get(0);
                tableFilterList.remove(0);
                tableFilterList.add(temp);
            }
        }

        // add remaining tables back
//        for (TableFilter filter : toRevisit) {
//            list[insertIndex] = filter;
//            insertIndex++;
//        }


        for (int i = 0; i < list.length; i++) {
            System.out.println(list[i]);
        }
        return list;
    }
}