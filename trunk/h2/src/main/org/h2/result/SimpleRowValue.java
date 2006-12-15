/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.value.Value;

public class SimpleRowValue implements SearchRow {
    
    private int pos;
    private int virtualColumnCount;
    private Value data;
    
    public SimpleRowValue(int columnCount) {
        this.virtualColumnCount = columnCount;
    }
    
    public int getColumnCount() {
        return virtualColumnCount;
    }
    public int getPos() {
        return pos;
    }
    public Value getValue(int index) {
        return data;
    }
    public void setPos(int pos) {
        this.pos = pos;
    }
    
    public void setValue(int idx, Value v) {
        data = v;
    }

}
