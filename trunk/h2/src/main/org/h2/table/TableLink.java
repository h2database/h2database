/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.LinkedIndex;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.util.JdbcUtils;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.DataType;

/**
 * @author Thomas
 */

public class TableLink extends Table {

    private String driver, url, user, password, originalTable;
    private Connection conn;
    private HashMap prepared = new HashMap();
    private final ObjectArray indexes = new ObjectArray();
    private final boolean emitUpdates;
    private LinkedIndex linkedIndex;
    private SQLException connectException;

    public TableLink(Schema schema, int id, String name, String driver, String url, String user, String password,
            String originalTable, boolean emitUpdates, boolean force) throws SQLException {
        super(schema, id, name, false);
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
        this.originalTable = originalTable;
        this.emitUpdates = emitUpdates;
        try {
            connect();
        } catch (SQLException e) {
            connectException = e;
            if (!force) {
                throw e;
            }
            Column[] cols = new Column[0];
            setColumns(cols);
            linkedIndex = new LinkedIndex(this, id, cols, IndexType.createNonUnique(false));
            indexes.add(linkedIndex);
        }
    }

    private void connect() throws SQLException {
        conn = JdbcUtils.getConnection(driver, url, user, password);
        DatabaseMetaData meta = conn.getMetaData();
        boolean storesLowerCase = meta.storesLowerCaseIdentifiers();
        ResultSet rs = meta.getColumns(null, null, originalTable, null);
        int i = 0;
        ObjectArray columnList = new ObjectArray();
        HashMap columnMap = new HashMap();
        while (rs.next()) {
            String n = rs.getString("COLUMN_NAME");
            if (storesLowerCase && n.equals(StringUtils.toLowerEnglish(n))) {
                n = StringUtils.toUpperEnglish(n);
            }
            int sqlType = rs.getInt("DATA_TYPE");
            long precision = rs.getInt("COLUMN_SIZE");
            int scale = rs.getInt("DECIMAL_DIGITS");
            int type = DataType.convertSQLTypeToValueType(sqlType);
            Column col = new Column(n, type, precision, scale);
            col.setTable(this, i++);
            columnList.add(col);
            columnMap.put(n, col);
        }
        if (columnList.size() == 0) {
            Statement stat = null;
            try {
                stat = conn.createStatement();
                rs = stat.executeQuery("SELECT * FROM " + originalTable + " T WHERE 1=0");
                ResultSetMetaData rsMeta = rs.getMetaData();
                for (i = 0; i < rsMeta.getColumnCount();) {
                    String n = rsMeta.getColumnName(i + 1);
                    if (storesLowerCase && n.equals(StringUtils.toLowerEnglish(n))) {
                        n = StringUtils.toUpperEnglish(n);
                    }
                    int sqlType = rsMeta.getColumnType(i + 1);
                    long precision = rsMeta.getPrecision(i + 1);
                    int scale = rsMeta.getScale(i + 1);
                    int type = DataType.convertSQLTypeToValueType(sqlType);
                    Column col = new Column(n, type, precision, scale);
                    col.setTable(this, i++);
                    columnList.add(col);
                    columnMap.put(n, col);
                }
            } catch (SQLException e) {
                throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, new String[] { originalTable + "("
                        + e.toString() + ")" }, e);
            } finally {
                JdbcUtils.closeSilently(stat);
            }
        }
        Column[] cols = new Column[columnList.size()];
        columnList.toArray(cols);
        setColumns(cols);
        int id = getId();
        linkedIndex = new LinkedIndex(this, id, cols, IndexType.createNonUnique(false));
        indexes.add(linkedIndex);
        rs = meta.getPrimaryKeys(null, null, originalTable);
        String pkName = "";
        ObjectArray list;
        if (rs.next()) {
            // the problem is, the rows are not sorted by KEY_SEQ
            list = new ObjectArray();
            do {
                int idx = rs.getInt("KEY_SEQ");
                if (pkName == null) {
                    pkName = rs.getString("PK_NAME");
                }
                while (list.size() < idx) {
                    list.add(null);
                }
                String col = rs.getString("COLUMN_NAME");
                Column column = (Column) columnMap.get(col);
                list.set(idx - 1, column);
            } while (rs.next());
            addIndex(list, IndexType.createPrimaryKey(false, false));
        }
        try {
            rs = meta.getIndexInfo(null, null, originalTable, false, false);
        } catch (SQLException e) {
            // Oracle throws an exception if the table is not found or is a
            // SYNONYM
            rs = null;
        }
        String indexName = null;
        list = new ObjectArray();
        IndexType indexType = null;
        while (rs != null && rs.next()) {
            String newIndex = rs.getString("INDEX_NAME");
            if (pkName.equals(newIndex)) {
                continue;
            }
            if (indexName != null && !indexName.equals(newIndex)) {
                addIndex(list, indexType);
                indexName = null;
            }
            if (indexName == null) {
                indexName = newIndex;
                list.clear();
            }
            boolean unique = !rs.getBoolean("NON_UNIQUE");
            indexType = unique ? IndexType.createUnique(false, false) : IndexType.createNonUnique(false);
            String col = rs.getString("COLUMN_NAME");
            Column column = (Column) columnMap.get(col);
            list.add(column);
        }
        if (indexName != null) {
            addIndex(list, indexType);
        }
    }

    private void addIndex(ObjectArray list, IndexType indexType) {
        Column[] cols = new Column[list.size()];
        list.toArray(cols);
        Index index = new LinkedIndex(this, 0, cols, indexType);
        indexes.add(index);
    }

    public String getDropSQL() {
        return "DROP TABLE IF EXISTS " + getSQL();
    }

    public String getCreateSQL() {
        StringBuffer buff = new StringBuffer();
        buff.append("CREATE FORCE LINKED TABLE ");
        buff.append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ");
            buff.append(StringUtils.quoteStringSQL(comment));
        }
        buff.append("(");
        buff.append(StringUtils.quoteStringSQL(driver));
        buff.append(", ");
        buff.append(StringUtils.quoteStringSQL(url));
        buff.append(", ");
        buff.append(StringUtils.quoteStringSQL(user));
        buff.append(", ");
        buff.append(StringUtils.quoteStringSQL(password));
        buff.append(", ");
        buff.append(StringUtils.quoteStringSQL(originalTable));
        buff.append(")");
        if (emitUpdates) {
            buff.append(" EMIT UPDATES");
        }
        return buff.toString();
    }

    public Index addIndex(Session session, String indexName, int indexId, Column[] cols, IndexType indexType,
            int headPos, String comment) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void lock(Session session, boolean exclusive, boolean force) throws SQLException {
        // nothing to do
    }

    public boolean isLockedExclusively() {
        return false;
    }

    public Index getScanIndex(Session session) {
        return linkedIndex;
    }

    public void removeRow(Session session, Row row) throws SQLException {
        getScanIndex(session).remove(session, row);
    }

    public void addRow(Session session, Row row) throws SQLException {
        getScanIndex(session).add(session, row);
    }

    public void close(Session session) throws SQLException {
        if (conn != null) {
            try {
                conn.close();
            } finally {
                conn = null;
            }
        }
    }

    public long getRowCount(Session session) throws SQLException {
        PreparedStatement prep = getPreparedStatement("SELECT COUNT(*) FROM " + originalTable);
        ResultSet rs = prep.executeQuery();
        rs.next();
        long count = rs.getLong(1);
        rs.close();
        return count;
    }

    public String getOriginalTable() {
        return originalTable;
    }

    public PreparedStatement getPreparedStatement(String sql) throws SQLException {
        if (conn == null) {
            throw connectException;
        }
        PreparedStatement prep = (PreparedStatement) prepared.get(sql);
        if (prep == null) {
            prep = conn.prepareStatement(sql);
            prepared.put(sql, prep);
        }
        return prep;
    }

    public void unlock(Session s) {
        // nothing to do
    }

    public void checkRename() throws SQLException {
    }

    public void checkSupportAlter() throws SQLException {
        throw Message.getUnsupportedException();
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException();
    }

    public boolean canGetRowCount() {
        return true;
    }

    public boolean canDrop() {
        return true;
    }

    public String getTableType() {
        return Table.TABLE_LINK;
    }

    public void removeChildrenAndResources(Session session) throws SQLException {
        super.removeChildrenAndResources(session);
        close(session);
        driver = null;
        url = user = password = originalTable = null;
        conn = null;
        prepared = null;
        invalidate();
    }

    public ObjectArray getIndexes() {
        return indexes;
    }

    public long getMaxDataModificationId() {
        // data may have been modified externally
        return Long.MAX_VALUE;
    }

    public Index getUniqueIndex() {
        for (int i = 0; i < indexes.size(); i++) {
            Index idx = (Index) indexes.get(i);
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    public void updateRows(Prepared prepared, Session session, ObjectArray oldRows, ObjectArray newRows)
            throws SQLException {
        boolean deleteInsert;
        if (emitUpdates) {
            for (int i = 0; i < oldRows.size(); i++) {
                session.checkCancelled();
                Row oldRow = (Row) oldRows.get(i);
                Row newRow = (Row) newRows.get(i);
                linkedIndex.update(session, oldRow, newRow);
                session.log(this, UndoLogRecord.DELETE, oldRow);
                session.log(this, UndoLogRecord.INSERT, newRow);
            }
            deleteInsert = false;
        } else {
            deleteInsert = true;
        }
        if (deleteInsert) {
            super.updateRows(prepared, session, oldRows, newRows);
        }
    }

}
