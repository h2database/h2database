/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.store.DataPage;
import org.h2.store.Record;

public class BtreeHead extends Record {

    private int rootPosition;
    private boolean consistent;

    public BtreeHead() {
        // nothing to do
    }

    public BtreeHead(DataPage s) throws SQLException {
        rootPosition = s.readInt();
        consistent = s.readInt() == 1;
    }
    
    public boolean getConsistent() {
        return consistent;
    }
    
    public void setConsistent(boolean b) {
        this.consistent = b;
    }

    public int getByteCount(DataPage dummy) throws SQLException {
        return 1 + dummy.getIntLen();
    }

    public void write(DataPage buff) throws SQLException {
        buff.writeByte((byte) 'H');
        buff.writeInt(rootPosition);
        buff.writeInt(consistent ? 1 : 0);
    }

    void setRootPosition(int rootPosition) {
        this.rootPosition = rootPosition;
    }

    int getRootPosition() {
        return rootPosition;
    }
    
    public boolean isPinned() {
        return true;
    }

}
