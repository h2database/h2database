/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.engine.Session;
import org.h2.value.Value;

public class SimpleRow implements SearchRow {
    
    private int pos;
    private Value[] data;
    private int sessionId;
    private boolean deleted;

    public SimpleRow(Value[] data) {
        this.data = data;
    }

    public int getColumnCount() {
        return data.length;
    }

    public int getPos() {
        return pos;
    }
    
    public void setPos(int pos) {
        this.pos = pos;
    }
    
    public void setValue(int i, Value v) {
        data[i] = v;
    }

    public Value getValue(int i) {
        return data[i];
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
