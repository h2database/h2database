/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.TableLink;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * @author Thomas
 */

public class LinkedIndex extends Index {

    private TableLink link;
    private String originalTable;
    
    public LinkedIndex(TableLink table, int id, Column[] columns, IndexType indexType) {
        super(table, id, null, columns, indexType);
        link = table;
        originalTable = link.getOriginalTable();
    }
    
    public String getCreateSQL() {
        return null;
    }

    public void close(Session session) throws SQLException {
    }

    public void add(Session session, Row row) throws SQLException {
        StringBuffer buff = new StringBuffer("INSERT INTO ");
        buff.append(originalTable);
        buff.append(" VALUES(");
        for(int i=0, j=0; i<row.getColumnCount(); i++) {
            Value v = row.getValue(i);
            if(j>0) {
                buff.append(',');
            }
            j++;
            if(v != null && v != ValueNull.INSTANCE) {
                buff.append('?');
            } else {
                buff.append("NULL");
            }
        }
        buff.append(')');
        String sql = buff.toString();
        try {
            PreparedStatement prep = link.getPreparedStatement(sql);
            for(int i=0, j=0; i<row.getColumnCount(); i++) {
                Value v = row.getValue(i);
                if(v != null && v != ValueNull.INSTANCE) {
                    v.set(prep, j+1);
                    j++;
                }
            }
            prep.executeUpdate();
            rowCount++;
        } catch(SQLException e) {
            throw Message.getSQLException(Message.ERROR_ACCESSING_LINKED_TABLE_1, new String[]{sql}, e);
        }
    }

    public void remove(Session session, Row row) throws SQLException {
        StringBuffer buff = new StringBuffer("DELETE FROM ");
        buff.append(originalTable);
        buff.append(" WHERE ");
        for(int i=0; i<row.getColumnCount(); i++) {
            if(i>0) {
                buff.append("AND ");
            }
            buff.append(table.getColumn(i).getSQL());
            buff.append("=? ");
        }
        String sql = buff.toString();
        try {
            PreparedStatement prep = link.getPreparedStatement(sql);
            for(int i=0, j=0; i<row.getColumnCount(); i++) {
                Value v = row.getValue(i);
                if(v != null) {
                    v.set(prep, j+1);
                    j++;
                }
            }
            prep.executeUpdate();
            rowCount--;
        } catch(SQLException e) {
            throw Message.getSQLException(Message.ERROR_ACCESSING_LINKED_TABLE_1, new String[]{sql}, e);
        }
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        StringBuffer buff = new StringBuffer();
        for(int i=0; first != null && i<first.getColumnCount(); i++) {
            Value v = first.getValue(i);
            if(v != null) {
                if(buff.length() != 0) {
                    buff.append(" AND ");
                }
                buff.append(table.getColumn(i).getSQL());
                buff.append(">=?");
            }
        }
        for(int i=0; last != null && i<last.getColumnCount(); i++) {
            Value v = last.getValue(i);
            if(v != null) {
                if(buff.length() != 0) {
                    buff.append(" AND ");
                }
                buff.append(table.getColumn(i).getSQL());
                buff.append("<=?");
            }
        }
        if(buff.length() > 0) {
            buff.insert(0, " WHERE ");
        }
        buff.insert(0, "SELECT * FROM "+originalTable);
        String sql = buff.toString();
        try {
            PreparedStatement prep = link.getPreparedStatement(sql);
            int j=0;
            for(int i=0; first != null && i<first.getColumnCount(); i++) {
                Value v = first.getValue(i);
                if(v != null) {
                    v.set(prep, j+1);
                    j++;
                }
            }
            for(int i=0; last != null && i<last.getColumnCount(); i++) {
                Value v = last.getValue(i);
                if(v != null) {
                    v.set(prep, j+1);
                    j++;
                }
            }        
            ResultSet rs = prep.executeQuery();
            return new LinkedCursor(table, rs, session);
        } catch(SQLException e) {
            throw Message.getSQLException(Message.ERROR_ACCESSING_LINKED_TABLE_1, new String[]{sql}, e);
        }
    }
    
    public int getLookupCost(int rowCount) {
        for(int i=0, j = 1; ; i++) {
            j *= 10;
            if(j>rowCount) {
                return i+1;
            }
        }
    }

    public int getCost(int[] masks) throws SQLException {
        return 100 + getCostRangeIndex(masks, rowCount + Constants.COST_ROW_OFFSET);
    }

    public void remove(Session session) throws SQLException {
    }

    public void truncate(Session session) throws SQLException {
    }
    
    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public boolean needRebuild() {
        return false;
    }
    
    public boolean canGetFirstOrLast(boolean first) {
        return false;
    }

    public Value findFirstOrLast(Session session, boolean first) throws SQLException {
        // TODO optimization: could get the first or last value (in any cases; maybe not optimized)
        throw Message.getUnsupportedException();
    }    

}
