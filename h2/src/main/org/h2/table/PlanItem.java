/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.index.Index;

/**
 * The plan item describes the index to be used, and the estimated cost when
 * using it.
 */
public class PlanItem {
    public double cost;
    private Index index;
    private PlanItem joinPlan;

    public void setIndex(Index index) {
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }

    public PlanItem getJoinPlan() {
        return joinPlan;
    }

    public void setJoinPlan(PlanItem joinPlan) {
        this.joinPlan = joinPlan;
    }

}
