/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;

public class MultiVersionIndex implements Index {
    
    private final Index base;
    private final TreeIndex delta;
    private final TableData table;
    private final Object sync;
    
    public MultiVersionIndex(Index base, TableData table) throws SQLException { 
        this.base = base;
        this.table = table;
        IndexType deltaIndexType = IndexType.createNonUnique(false);
        this.delta = new TreeIndex(table, -1, "DELTA", base.getIndexColumns(), deltaIndexType);
        this.sync = base.getDatabase();
    }    

    public void add(Session session, Row row) throws SQLException {
        synchronized (sync) {
            base.add(session, row);
            // for example rolling back an delete operation
            removeIfExists(session, row);
            if (row.getSessionId() != 0) {
                // don't insert rows that are added when creating an index
                delta.add(session, row);
            }
        }
    }

    public void close(Session session) throws SQLException {
        synchronized (sync) {
            base.close(session);
        }
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        synchronized (sync) {
            Cursor baseCursor = base.find(session, first, last);
            Cursor deltaCursor = delta.find(session, first, last);
            return new MultiVersionCursor(session, this, baseCursor, deltaCursor, sync);
        }
    }

    public boolean canGetFirstOrLast(boolean first) {
        return false;
    }
    
    public SearchRow findFirstOrLast(Session session, boolean first) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public double getCost(Session session, int[] masks) throws SQLException {
        return base.getCost(session, masks);
    }

    public boolean needRebuild() {
        return base.needRebuild();
    }
    
    private boolean removeIfExists(Session session, Row row) throws SQLException {
        // maybe it was inserted by the same session just before
        Cursor c = delta.find(session, row, row);
        while (c.next()) {
            Row r = c.get();
            if (r.getPos() == row.getPos()) {
                delta.remove(session, row);
                return true;
            }
        }
        return false;
    }

    public void remove(Session session, Row row) throws SQLException {
        synchronized (sync) {
            base.remove(session, row);
            if (removeIfExists(session, row)) {
                // added and deleted in the same transaction: no change
            } else {
                delta.add(session, row);
            }
        }
    }

    public void remove(Session session) throws SQLException {
        synchronized (sync) {
            base.remove(session);
        }
    }

    public void truncate(Session session) throws SQLException {
        synchronized (sync) {
            delta.truncate(session);
            base.truncate(session);
        }
    }
    
    public void commit(int operation, Row row) throws SQLException {
        synchronized (sync) {
            removeIfExists(null, row);
        }
    }

    public int compareKeys(SearchRow rowData, SearchRow compare) {
        return base.compareKeys(rowData, compare);
    }

    public int compareRows(SearchRow rowData, SearchRow compare) throws SQLException {
        return base.compareRows(rowData, compare);
    }

    public int getColumnIndex(Column col) {
        return base.getColumnIndex(col);
    }

    public String getColumnListSQL() {
        return base.getColumnListSQL();
    }

    public Column[] getColumns() {
        return base.getColumns();
    }
    
    public IndexColumn[] getIndexColumns() {
        return base.getIndexColumns();
    }    

    public long getCostRangeIndex(int[] masks, long rowCount) throws SQLException {
        return base.getCostRangeIndex(masks, rowCount);
    }

    public String getCreateSQL() {
        return base.getCreateSQL();
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        return base.getCreateSQLForCopy(table, quotedName);
    }

    public String getDropSQL() {
        return base.getDropSQL();
    }

    public SQLException getDuplicateKeyException() {
        return base.getDuplicateKeyException();
    }

    public IndexType getIndexType() {
        return base.getIndexType();
    }

    public int getLookupCost(long rowCount) {
        return base.getLookupCost(rowCount);
    }

    public String getPlanSQL() {
        return base.getPlanSQL();
    }

    public long getRowCount(Session session) {
        return base.getRowCount(session);
    }

    public Table getTable() {
        return base.getTable();
    }

    public int getType() {
        return base.getType();
    }

    public boolean isNull(Row newRow) {
        return base.isNull(newRow);
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        synchronized (sync) {
            table.removeIndex(this);
            remove(session);
        }
    }

    public String getSQL() {
        return base.getSQL();
    }

    public Schema getSchema() {
        return base.getSchema();
    }

    public void checkRename() throws SQLException {
        base.checkRename();
    }

    public ObjectArray getChildren() {
        return base.getChildren();
    }

    public String getComment() {
        return base.getComment();
    }

    public Database getDatabase() {
        return base.getDatabase();
    }

    public int getHeadPos() {
        return base.getHeadPos();
    }

    public int getId() {
        return base.getId();
    }

    public long getModificationId() {
        return base.getModificationId();
    }

    public String getName() {
        return base.getName();
    }

    public boolean getTemporary() {
        return base.getTemporary();
    }

    public void rename(String newName) throws SQLException {
        base.rename(newName);
    }

    public void setComment(String comment) {
        base.setComment(comment);
    }

    public void setModified() {
        base.setModified();
    }

    public void setTemporary(boolean temporary) {
        base.setTemporary(temporary);
    }

}
