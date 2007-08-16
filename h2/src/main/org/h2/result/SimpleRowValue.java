/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Session;
import org.h2.value.Value;

public class SimpleRowValue implements SearchRow {
    
    private int pos;
    private int index;
    private int virtualColumnCount;
    private Value data;
    private int sessionId;
    private boolean deleted;
    
    public SimpleRowValue(int columnCount) {
        this.virtualColumnCount = columnCount;
    }
    
    public int getColumnCount() {
        return virtualColumnCount;
    }
    public int getPos() {
        return pos;
    }
    public Value getValue(int idx) {
        return idx == index ? data : null;
    }
    public void setPos(int pos) {
        this.pos = pos;
    }
    
    public void setValue(int idx, Value v) {
        index = idx;
        data = v;
    }

    public boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Session session, boolean deleted) {
        this.sessionId = session.getId();
        this.deleted = deleted;
    }
    
    public int getSessionId() {
        int testing;
        return sessionId;
    }
    
}
