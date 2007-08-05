/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.sql.SQLException;
import java.util.BitSet;
import java.util.Random;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.table.Plan;
import org.h2.table.PlanItem;
import org.h2.table.TableFilter;
import org.h2.util.Permutations;

public class Optimizer {

    private static final int MAX_BRUTE_FORCE_FILTERS=7;
    private static final int MAX_BRUTE_FORCE=2000;
    private static final int MAX_GENETIC=2000;
    private long start;
    private BitSet switched;
    
    //  possible plans for filters:
    //  1 filter 1 plan
    //  2 filters 2 plans
    //  3 filters 6 plans
    //  4 filters 24 plans
    //  5 filters 120 plans
    //  6 filters 720 plans
    //  7 filters 5040 plans
    //  8 filters 40320 plan
    //  9 filters 362880 plans
    // 10 filters 3628800 filters
    // 1 of 1, 2, 3, 4, 5, 6 filters: 1, 2, 3, 4, 5, 6
    // 2 of 2, 3, 4, 5, 6 filters: 2, 6, 12, 20, 30
    // 3 of 3, 4, 5, 6 filters: 6, 24, 75, 120
    // 4 of 4, 5, 6 filters: 24, 120, 260
    
    private TableFilter[] filters;
    private Expression condition;
    private Session session;
    
    private Plan bestPlan;
    private TableFilter topFilter;
    private double cost;
    private Random random;
    
    private int getMaxBruteForceFilters(int filterCount) {
        int i = 0, j = filterCount, total = filterCount;
        while(j>0 && total < MAX_BRUTE_FORCE) {
            j--;
            total *= j;
            i++;
        }
        return i;
    }
    
    Optimizer(TableFilter[] filters, Expression condition, Session session) {
        this.filters = filters;
        this.condition = condition;
        this.session = session;
    }
    
    private void calculateBestPlan() throws SQLException {
        start = System.currentTimeMillis();        
        cost = -1;
        if(filters.length==1) {
            testPlan(filters);
        } else if (filters.length <= MAX_BRUTE_FORCE_FILTERS) {
            calculateBruteForceAll();
        } else {
            calculateBruteForceSome();
            random = new Random(0);
            calculateGenetic();
            // TODO optimizer: how to use rule based optimizer?
        }
    }
    
    private boolean canStop(int x) {
        if((x & 127) == 0) {
            long t = System.currentTimeMillis() - start;
            // don't calculate for simple queries (no rows or so)
            if(cost >= 0 && 10*t > cost) {
                return true;
            }
        }
        return false;
    }
    
    private void calculateBruteForceAll() throws SQLException {
        TableFilter[] list = new TableFilter[filters.length];
        Permutations p = new Permutations(filters, list);
        for(int x=0; !canStop(x) && p.next(); x++) {
            testPlan(list);
        }
    }
    
    private void calculateBruteForceSome() throws SQLException {
        int bruteForce = getMaxBruteForceFilters(filters.length);
        TableFilter[] list = new TableFilter[filters.length];
        Permutations p = new Permutations(filters, list, bruteForce);
        for(int x=0; !canStop(x) && p.next(); x++) {
            // find out what filters are not used yet
            for(int i=0; i<filters.length; i++) {
                filters[i].setUsed(false);
            }
            for(int i=0; i<bruteForce; i++) {
                list[i].setUsed(true);
            }
            // fill the remaining elements with the unused elements (greedy)
            for(int i=bruteForce; i<filters.length; i++) {
                double costPart = -1.0;
                int bestPart = -1;
                for(int j=0; j<filters.length; j++) {
                    if(!filters[j].getUsed()) {
                        if(i==filters.length-1) {
                            bestPart = j;
                            break;
                        }                        
                        list[i] = filters[j];
                        Plan part = new Plan(list, i+1, condition);
                        double costNow = part.calculateCost(session);
                        if (costPart < 0 || costNow < costPart) {
                            costPart = costNow;
                            bestPart = j;
                        }
                    }
                }
                filters[bestPart].setUsed(true);
                list[i] = filters[bestPart];
            }
            testPlan(list);
        }
    }
    
    private void calculateGenetic() throws SQLException {
        TableFilter[] best = new TableFilter[filters.length];        
        TableFilter[] list = new TableFilter[filters.length];        
        for(int x=0; x<MAX_GENETIC; x++) {
            if(canStop(x)) {
                break;
            }
            boolean generateRandom = (x & 127) == 0;
            if(!generateRandom) {
                System.arraycopy(best, 0, list, 0, filters.length);
                if(!shuffleTwo(list)) {
                    generateRandom = true;
                }
            }
            if(generateRandom) {
                switched = new BitSet();
                System.arraycopy(filters, 0, best, 0, filters.length);
                shuffleAll(best);
                System.arraycopy(best, 0, list, 0, filters.length);
            }
            if(testPlan(list)) {
                switched = new BitSet();
                System.arraycopy(list, 0, best, 0, filters.length);
            }
        }
    }    
    
    private boolean testPlan(TableFilter[] list) throws SQLException {
        Plan p = new Plan(list, list.length, condition);
        double costNow = p.calculateCost(session);
        if (cost < 0 || costNow < cost) {
            cost = costNow;
            bestPlan = p;
            return true;
        }
        return false;
    }
    
    private void shuffleAll(TableFilter[] f) {    
        for(int i=0; i<f.length-1; i++) {
            int j = i + random.nextInt(f.length - i);
            if(j != i) {
                TableFilter temp = f[i];
                f[i] = f[j];
                f[j] = temp;
            }
        }
    }

    private boolean shuffleTwo(TableFilter[] f) {
        int a = 0, b = 0, i = 0;
        for(; i<20; i++) {
            a = random.nextInt(f.length);
            b = random.nextInt(f.length);
            if(a==b) {
                continue;
            }
            if(a<b) {
                int temp = a;
                a = b;
                b = temp;
            }
            int s = a * f.length + b;
            if(switched.get(s)) {
                continue;
            }
            switched.set(s);
            break;
        }
        if(i==20) {
            return false;
        }
        TableFilter temp = f[a];
        f[a] = f[b];
        f[b] = temp;
        return true;
    }

    void optimize() throws SQLException {
        calculateBestPlan();
        bestPlan.removeUnusableIndexConditions();
        TableFilter[] f2 = bestPlan.getFilters();
        topFilter = f2[0];
        for (int i = 0; i < f2.length - 1; i++) {
            f2[i].addJoin(f2[i + 1], false, null);
        }
        for (int i = 0; i < f2.length; i++) {
            PlanItem item = bestPlan.getItem(f2[i]);
            f2[i].setPlanItem(item);
        }
    }

    public TableFilter getTopFilter() {
        return topFilter;
    }
    
    double getCost() {
        return cost;
    }

}
