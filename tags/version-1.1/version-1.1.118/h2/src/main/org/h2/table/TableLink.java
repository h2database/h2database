/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import org.h2.command.Prepared;
import org.h2.constant.ErrorCode;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.LinkedIndex;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.schema.Schema;
import org.h2.util.JdbcUtils;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.util.ObjectArray;
import org.h2.util.StringUtils;
import org.h2.value.DataType;
import org.h2.value.ValueDate;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;

/**
 * A linked table contains connection information for a table accessible by JDBC.
 * The table may be stored in a different database.
 */
public class TableLink extends Table {

    private static final long ROW_COUNT_APPROXIMATION = 100000;

    private String driver, url, user, password, originalSchema, originalTable, qualifiedTableName;
    private TableLinkConnection conn;
    private HashMap<String, PreparedStatement> prepared = New.hashMap();
    private final ObjectArray<Index> indexes = ObjectArray.newInstance();
    private final boolean emitUpdates;
    private LinkedIndex linkedIndex;
    private SQLException connectException;
    private boolean storesLowerCase;
    private boolean storesMixedCase;
    private boolean supportsMixedCaseIdentifiers;
    private boolean globalTemporary;
    private boolean readOnly;

    public TableLink(Schema schema, int id, String name, String driver, String url, String user, String password,
            String originalSchema, String originalTable, boolean emitUpdates, boolean force) throws SQLException {
        super(schema, id, name, false, true);
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
        this.originalSchema = originalSchema;
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
            linkedIndex = new LinkedIndex(this, id, IndexColumn.wrap(cols), IndexType.createNonUnique(false));
            indexes.add(linkedIndex);
        }
    }

    private void connect() throws SQLException {
        conn = database.getLinkConnection(driver, url, user, password);
        synchronized (conn) {
            try {
                readMetaData();
            } catch (SQLException e) {
                conn.close();
                conn = null;
                throw e;
            }
        }
    }

    private void readMetaData() throws SQLException {
        DatabaseMetaData meta = conn.getConnection().getMetaData();
        storesLowerCase = meta.storesLowerCaseIdentifiers();
        storesMixedCase = meta.storesMixedCaseIdentifiers();
        supportsMixedCaseIdentifiers = meta.supportsMixedCaseIdentifiers();
        ResultSet rs = meta.getTables(null, originalSchema, originalTable, null);
        if (rs.next() && rs.next()) {
            throw Message.getSQLException(ErrorCode.SCHEMA_NAME_MUST_MATCH, originalTable);
        }
        rs.close();
        rs = meta.getColumns(null, originalSchema, originalTable, null);
        int i = 0;
        ObjectArray<Column> columnList = ObjectArray.newInstance();
        HashMap<String, Column> columnMap = New.hashMap();
        String catalog = null, schema = null;
        while (rs.next()) {
            String thisCatalog = rs.getString("TABLE_CAT");
            if (catalog == null) {
                catalog = thisCatalog;
            }
            String thisSchema = rs.getString("TABLE_SCHEM");
            if (schema == null) {
                schema = thisSchema;
            }
            if (!StringUtils.equals(catalog, thisCatalog) || !StringUtils.equals(schema, thisSchema)) {
                // if the table exists in multiple schemas or tables,
                // use the alternative solution
                columnMap.clear();
                columnList.clear();
                break;
            }
            String n = rs.getString("COLUMN_NAME");
            n = convertColumnName(n);
            int sqlType = rs.getInt("DATA_TYPE");
            long precision = rs.getInt("COLUMN_SIZE");
            precision = convertPrecision(sqlType, precision);
            int scale = rs.getInt("DECIMAL_DIGITS");
            int displaySize = MathUtils.convertLongToInt(precision);
            int type = DataType.convertSQLTypeToValueType(sqlType);
            Column col = new Column(n, type, precision, scale, displaySize);
            col.setTable(this, i++);
            columnList.add(col);
            columnMap.put(n, col);
        }
        rs.close();
        if (originalTable.indexOf('.') < 0 && !StringUtils.isNullOrEmpty(schema)) {
            qualifiedTableName = schema + "." + originalTable;
        } else {
            qualifiedTableName = originalTable;
        }
        // check if the table is accessible
        Statement stat = null;
        try {
            stat = conn.getConnection().createStatement();
            rs = stat.executeQuery("SELECT * FROM " + qualifiedTableName + " T WHERE 1=0");
            if (columnList.size() == 0) {
                // alternative solution
                ResultSetMetaData rsMeta = rs.getMetaData();
                for (i = 0; i < rsMeta.getColumnCount();) {
                    String n = rsMeta.getColumnName(i + 1);
                    n = convertColumnName(n);
                    int sqlType = rsMeta.getColumnType(i + 1);
                    long precision = rsMeta.getPrecision(i + 1);
                    precision = convertPrecision(sqlType, precision);
                    int scale = rsMeta.getScale(i + 1);
                    int displaySize = rsMeta.getColumnDisplaySize(i + 1);
                    int type = DataType.convertSQLTypeToValueType(sqlType);
                    Column col = new Column(n, type, precision, scale, displaySize);
                    col.setTable(this, i++);
                    columnList.add(col);
                    columnMap.put(n, col);
                }
            }
            rs.close();
        } catch (SQLException e) {
            throw Message.getSQLException(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, e,
                    originalTable + "(" + e.toString() + ")");
        } finally {
            JdbcUtils.closeSilently(stat);
        }
        Column[] cols = new Column[columnList.size()];
        columnList.toArray(cols);
        setColumns(cols);
        int id = getId();
        linkedIndex = new LinkedIndex(this, id, IndexColumn.wrap(cols), IndexType.createNonUnique(false));
        indexes.add(linkedIndex);
        try {
            rs = meta.getPrimaryKeys(null, originalSchema, originalTable);
        } catch (SQLException e) {
            // Some ODBC bridge drivers don't support it:
            // some combinations of "DataDirect SequeLink(R) for JDBC"
            // http://www.datadirect.com/index.ssp
            rs = null;
        }
        String pkName = "";
        ObjectArray<Column> list;
        if (rs != null && rs.next()) {
            // the problem is, the rows are not sorted by KEY_SEQ
            list = ObjectArray.newInstance();
            do {
                int idx = rs.getInt("KEY_SEQ");
                if (pkName == null) {
                    pkName = rs.getString("PK_NAME");
                }
                while (list.size() < idx) {
                    list.add(null);
                }
                String col = rs.getString("COLUMN_NAME");
                col = convertColumnName(col);
                Column column = columnMap.get(col);
                if (idx == 0) {
                    // workaround for a bug in the SQLite JDBC driver
                    list.add(column);
                } else {
                    list.set(idx - 1, column);
                }
            } while (rs.next());
            addIndex(list, IndexType.createPrimaryKey(false, false));
            rs.close();
        }
        try {
            rs = meta.getIndexInfo(null, originalSchema, originalTable, false, true);
        } catch (SQLException e) {
            // Oracle throws an exception if the table is not found or is a
            // SYNONYM
            rs = null;
        }
        String indexName = null;
        list = ObjectArray.newInstance();
        IndexType indexType = null;
        if (rs != null) {
            while (rs.next()) {
                if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
                    // ignore index statistics
                    continue;
                }
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
                col = convertColumnName(col);
                Column column = columnMap.get(col);
                list.add(column);
            }
            rs.close();
        }
        if (indexName != null) {
            addIndex(list, indexType);
        }
    }

    private long convertPrecision(int sqlType, long precision) {
        // workaround for an Oracle problem
        // the precision reported by Oracle is 7 for a date column
        switch (sqlType) {
        case Types.DATE:
            precision = Math.max(ValueDate.PRECISION, precision);
            break;
        case Types.TIMESTAMP:
            precision = Math.max(ValueTimestamp.PRECISION, precision);
            break;
        case Types.TIME:
            precision = Math.max(ValueTime.PRECISION, precision);
            break;
        }
        return precision;
    }

    private String convertColumnName(String columnName) {
        if ((storesMixedCase || storesLowerCase) && columnName.equals(StringUtils.toLowerEnglish(columnName))) {
            columnName = StringUtils.toUpperEnglish(columnName);
        } else if (storesMixedCase && !supportsMixedCaseIdentifiers) {
            // TeraData
            columnName = StringUtils.toUpperEnglish(columnName);
        }
        return columnName;
    }

    private void addIndex(ObjectArray<Column> list, IndexType indexType) {
        Column[] cols = new Column[list.size()];
        list.toArray(cols);
        Index index = new LinkedIndex(this, 0, IndexColumn.wrap(cols), indexType);
        indexes.add(index);
    }

    public String getDropSQL() {
        return "DROP TABLE IF EXISTS " + getSQL();
    }

    public String getCreateSQL() {
        StringBuilder buff = new StringBuilder("CREATE FORCE ");
        if (isTemporary()) {
            if (globalTemporary) {
                buff.append("GLOBAL ");
            }
            buff.append("TEMP ");
        }
        buff.append("LINKED TABLE ").append(getSQL());
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        buff.append('(').
            append(StringUtils.quoteStringSQL(driver)).
            append(", ").
            append(StringUtils.quoteStringSQL(url)).
            append(", ").
            append(StringUtils.quoteStringSQL(user)).
            append(", ").
            append(StringUtils.quoteStringSQL(password)).
            append(", ").
            append(StringUtils.quoteStringSQL(originalTable)).
            append(')');
        if (emitUpdates) {
            buff.append(" EMIT UPDATES");
        }
        if (readOnly) {
            buff.append(" READONLY");
        }
        return buff.toString();
    }

    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            int headPos, String comment) throws SQLException {
        throw Message.getUnsupportedException("LINK");
    }

    public void lock(Session session, boolean exclusive, boolean force) {
        // nothing to do
    }

    public boolean isLockedExclusively() {
        return false;
    }

    public Index getScanIndex(Session session) {
        return linkedIndex;
    }

    private void checkReadOnly() throws SQLException {
        if (readOnly) {
            throw Message.getSQLException(ErrorCode.DATABASE_IS_READ_ONLY);
        }
    }

    public void removeRow(Session session, Row row) throws SQLException {
        checkReadOnly();
        getScanIndex(session).remove(session, row);
    }

    public void addRow(Session session, Row row) throws SQLException {
        checkReadOnly();
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

    public synchronized long getRowCount(Session session) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + qualifiedTableName;
        try {
            PreparedStatement prep = getPreparedStatement(sql, false);
            ResultSet rs = prep.executeQuery();
            rs.next();
            long count = rs.getLong(1);
            rs.close();
            return count;
        } catch (SQLException e) {
            throw wrapException(sql, e);
        }
    }

    /**
     * Wrap a SQL exception that occurred while accessing a linked table.
     *
     * @param sql the SQL statement
     * @param e the SQL exception from the remote database
     * @return the wrapped SQL exception
     */
    public SQLException wrapException(String sql, SQLException e) {
        return Message.getSQLException(ErrorCode.ERROR_ACCESSING_LINKED_TABLE_2, e, sql, e.toString());
    }

    public String getQualifiedTable() {
        return qualifiedTableName;
    }

    /**
     * Get a prepared statement object for the given statement. Prepared
     * statements are kept in a hash map to avoid re-creating them.
     *
     * @param sql the SQL statement
     * @param exclusive if the prepared statement must be removed from the map
     *          until reusePreparedStatement is called (only required for queries)
     * @return the prepared statement
     */
    public PreparedStatement getPreparedStatement(String sql, boolean exclusive) throws SQLException {
        Trace trace = database.getTrace(Trace.TABLE);
        if (trace.isDebugEnabled()) {
            trace.debug(getName() + ":\n" + sql);
        }
        if (conn == null) {
            throw connectException;
        }
        PreparedStatement prep = prepared.get(sql);
        if (prep == null) {
            prep = conn.getConnection().prepareStatement(sql);
            prepared.put(sql, prep);
        }
        if (exclusive) {
            prepared.remove(sql);
        }
        return prep;
    }

    public void unlock(Session s) {
        // nothing to do
    }

    public void checkRename() {
        // ok
    }

    public void checkSupportAlter() throws SQLException {
        throw Message.getUnsupportedException("LINK");
    }

    public void truncate(Session session) throws SQLException {
        throw Message.getUnsupportedException("LINK");
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
        database.removeMeta(session, getId());
        driver = null;
        url = user = password = originalTable = null;
        prepared = null;
        invalidate();
    }

    public boolean isOracle() {
        return url.startsWith("jdbc:oracle:");
    }

    public ObjectArray<Index> getIndexes() {
        return indexes;
    }

    public long getMaxDataModificationId() {
        // data may have been modified externally
        return Long.MAX_VALUE;
    }

    public Index getUniqueIndex() {
        for (Index idx : indexes) {
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    public void updateRows(Prepared prepared, Session session, RowList rows)
            throws SQLException {
        boolean deleteInsert;
        checkReadOnly();
        if (emitUpdates) {
            for (rows.reset(); rows.hasNext();) {
                prepared.checkCanceled();
                Row oldRow = rows.next();
                Row newRow = rows.next();
                linkedIndex.update(oldRow, newRow);
                session.log(this, UndoLogRecord.DELETE, oldRow);
                session.log(this, UndoLogRecord.INSERT, newRow);
            }
            deleteInsert = false;
        } else {
            deleteInsert = true;
        }
        if (deleteInsert) {
            super.updateRows(prepared, session, rows);
        }
    }

    public void setGlobalTemporary(boolean globalTemporary) {
        this.globalTemporary = globalTemporary;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public TableLinkConnection getConnection() {
        return conn;
    }

    public long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }

    /**
     * Add this prepared statement to the list of cached statements.
     *
     * @param prep the prepared statement
     * @param sql the SQL statement
     */
    public void reusePreparedStatement(PreparedStatement prep, String sql) {
        prepared.put(sql, prep);
    }

    public boolean isDeterministic() {
        return false;
    }

}
