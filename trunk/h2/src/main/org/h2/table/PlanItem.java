/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.index.Index;

/**
 * @author Thomas
 */
public class PlanItem {
    public double cost;
    public Index index;
    public PlanItem joinPlan;

    public Index getIndex() {
        return index;
    }

}
