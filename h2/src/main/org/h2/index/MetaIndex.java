/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;

import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.MetaTable;
import org.h2.util.ObjectArray;
import org.h2.value.Value;


/**
 * @author Thomas
 */

public class MetaIndex extends Index {

    private MetaTable meta;
    private boolean scan;
    
    public MetaIndex(MetaTable meta, Column[] columns, boolean scan) {
        super(meta, 0, null, columns, IndexType.createNonUnique(true));
        this.meta = meta;
        this.scan = scan;
    }
    
    public void close(Session session) throws SQLException {
        // nothing to do
    }

    public void add(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void remove(Session session, Row row) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        ObjectArray rows = meta.generateRows(session, first, last);
        return new MetaCursor(rows);
    }
    
    public int getCost(int[] masks) throws SQLException {
        if(scan) {
            return 10000;
        }
        return getCostRangeIndex(masks, 1000);
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void remove(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public int getColumnIndex(Column col) {
        if(scan) {
            // the scan index cannot use any columns
            return -1;
        }
        return super.getColumnIndex(col);
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public boolean needRebuild() {
        return false;
    }
    
    public String getCreateSQL() {
        return null;
    }

    public boolean canGetFirstOrLast(boolean first) {
        return false;
    }

    public Value findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException();
    } 
    
}
