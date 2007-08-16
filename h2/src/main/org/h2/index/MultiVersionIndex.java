package org.h2.index;

import java.sql.SQLException;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;

public class MultiVersionIndex implements Index {
    
    private Index base;
    private TreeIndex delta;
    
    public MultiVersionIndex(Index base, TableData table) throws SQLException { 
        this.base = base;
        IndexType deltaIndexType = IndexType.createNonUnique(false);
        this.delta = new TreeIndex(table, -1, "DELTA" ,base.getColumns(), deltaIndexType);
    }    

    public void add(Session session, Row row) throws SQLException {
        base.add(session, row);
        delta.add(session, row);
    }

    public void close(Session session) throws SQLException {
        base.close(session);
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        Cursor baseCursor = base.find(session, first, last);
        Cursor deltaCursor = delta.find(session, first, last);
        return new MultiVersionCursor(session, this, baseCursor, deltaCursor);
    }

    public boolean canGetFirstOrLast(boolean first) {
        int todoMVCC_Min_Max_Optimization;
        return false;
    }
    
    public SearchRow findFirstOrLast(Session session, boolean first) throws SQLException {
        int todoMVCC_Min_Max_Optimization;
        throw Message.getUnsupportedException();
    }

    public double getCost(Session session, int[] masks) throws SQLException {
        return base.getCost(session, masks);
    }

    public boolean needRebuild() {
        return base.needRebuild();
    }

    public void remove(Session session, Row row) throws SQLException {
        base.remove(session, row);
        delta.add(session, row);
    }

    public void remove(Session session) throws SQLException {
        base.remove(session);
    }

    public void truncate(Session session) throws SQLException {
        int todoLockingRequired;
        base.truncate(session);
    }
    
    public void commit(Row row) throws SQLException {
        delta.remove(null, row);
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
        base.removeChildrenAndResources(session);
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
