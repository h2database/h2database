/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.SQLException;
import java.util.HashMap;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionVisitor;
import org.h2.util.ObjectArray;

/**
 * @author Thomas
 */
public class Plan {
    private TableFilter[] filters;
    private HashMap planItems = new HashMap();
    private Expression[] allConditions;
    private TableFilter[] allFilters;

    public Plan(TableFilter[] filters, int count, Expression condition) {
        this.filters = new TableFilter[count];
        System.arraycopy(filters, 0, this.filters, 0, count);
        ObjectArray allCond = new ObjectArray();
        ObjectArray allFilt= new ObjectArray();
        if(condition != null) {
            allCond.add(condition);
        }
        for(int i=0; i<count; i++) {
            TableFilter f = filters[i];
            do {
                allFilt.add(f);
                if(f.getJoinCondition() != null) {
                    allCond.add(f.getJoinCondition());
                }
                f = f.getJoin();
            } while(f != null);
        }
        allConditions = new Expression[allCond.size()];
        allCond.toArray(allConditions);
        allFilters = new TableFilter[allFilt.size()];
        allFilt.toArray(allFilters);
    }

    public PlanItem getItem(TableFilter filter) {
        return (PlanItem) planItems.get(filter);
    }

    public TableFilter[] getFilters() {
        return filters;
    }

    public void removeUnusableIndexConditions() {
        for(int i=0; i<allFilters.length; i++) {
            TableFilter f = allFilters[i];
            setEvaluatable(f, true);
            if(i < allFilters.length - 1) {
                // the last table doesn't need the optimization,
                // otherwise the expression is calculated twice unnecessarily
                // (not that bad but not optimal)
                f.optimizeFullCondition(false);
            }
            f.removeUnusableIndexConditions();
        }
        for(int i=0; i<allFilters.length; i++) {
            TableFilter f = allFilters[i];
            setEvaluatable(f, false);
        }
    }

    public double calculateCost(Session session) throws SQLException {
        double cost = 1;
        boolean invalidPlan = false;
        for (int i = 0; i < allFilters.length; i++) {
            TableFilter tableFilter = allFilters[i];
            PlanItem item = tableFilter.getBestPlanItem(session);
            planItems.put(tableFilter, item);
            cost += cost * item.cost;
            setEvaluatable(tableFilter, true);
            Expression on = tableFilter.getJoinCondition();
            if(on != null) {
                if(!on.isEverything(ExpressionVisitor.EVALUATABLE)) {
                    invalidPlan = true;
                    break;
                }
            }
        }
        if(invalidPlan) {
            cost = 1./0.; // Infinity
        }
        for (int i = 0; i < allFilters.length; i++) {
            setEvaluatable(allFilters[i], false);
        }
        return cost;
    }

    private void setEvaluatable(TableFilter filter, boolean b) {
        for(int j=0; j<allConditions.length; j++) {
            allConditions[j].setEvaluatable(filter, b);
        }
    }
}
