/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.message.Message;
import org.h2.table.Column;
import org.h2.util.ObjectArray;
import org.h2.util.ValueHashMap;
import org.h2.value.DataType;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 */
public class LocalResult implements ResultInterface {

    private int maxMemoryRows;
    private Session session;
    private int visibleColumnCount;
    private Expression[] expressions;
    private int rowId, rowCount;
    private ObjectArray rows;
    private SortOrder sort;
    private ValueHashMap distinctRows;
    private Value[] currentRow;
    private int[] displaySizes;
    private int offset, limit;
    private ResultDiskBuffer disk;
    private int diskOffset;
    private boolean isUpdateCount;
    private int updateCount;

    public static LocalResult read(Session session, ResultSet rs, int maxrows) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        ObjectArray cols = new ObjectArray();
        int[] types = new int[columnCount];
        Database db = session == null ? null : session.getDatabase();
        for (int i = 0; i < columnCount; i++) {
            String name = meta.getColumnLabel(i + 1);
            int type = DataType.convertSQLTypeToValueType(meta.getColumnType(i + 1));
            types[i] = type;
            int precision = meta.getPrecision(i + 1);
            int scale = meta.getScale(i + 1);
            Column col = new Column(name, type, precision, scale);
            Expression expr = new ExpressionColumn(db, null, col);
            cols.add(expr);
        }
        LocalResult result = new LocalResult(session, cols, columnCount);
        for (int i = 0; (maxrows == 0 || i < maxrows) && rs.next(); i++) {
            Value[] list = new Value[columnCount];
            for (int j = 0; j < columnCount; j++) {
                list[j] = DataType.readValue(session, rs, j + 1, types[j]);
            }
            result.addRow(list);
        }
        result.done();
        return result;
    }

    public LocalResult(int updateCount) {
        this.isUpdateCount = true;
        this.updateCount = updateCount;
    }

    public LocalResult createShallowCopy(Session session) {
        if (disk == null && rows == null || rows.size() < rowCount) {
            return null;
        }
        LocalResult copy = new LocalResult(0);
        copy.maxMemoryRows = this.maxMemoryRows;
        copy.session = session;
        copy.visibleColumnCount = this.visibleColumnCount;
        copy.expressions = this.expressions;
        copy.rowId = -1;
        copy.rowCount = this.rowCount;
        copy.rows = this.rows;
        copy.sort = this.sort;
        copy.distinctRows = this.distinctRows;
        copy.currentRow = null;
        copy.displaySizes = this.displaySizes;
        copy.offset = 0;
        copy.limit = 0;
        copy.disk = this.disk;
        copy.diskOffset = this.diskOffset;
        copy.isUpdateCount = this.isUpdateCount;
        copy.updateCount = this.updateCount;
        return copy;
    }

    public boolean isUpdateCount() {
        return isUpdateCount;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public LocalResult(Session session, ObjectArray cols, int visibleColumnCount) {
        this.session = session;
        if (session == null) {
            this.maxMemoryRows = Integer.MAX_VALUE;
        } else {
            this.maxMemoryRows = session.getDatabase().getMaxMemoryRows();
        }
        this.expressions = new Expression[cols.size()];
        cols.toArray(expressions);
        this.displaySizes = new int[cols.size()];
        rows = new ObjectArray();
        this.visibleColumnCount = visibleColumnCount;
        rowId = -1;
    }

    public void setSortOrder(SortOrder sort) {
        this.sort = sort;
    }

    public void setDistinct() {
        // TODO big result sets: how to buffer distinct result sets? maybe do
        // the
        // distinct when sorting each block, and final merging
        distinctRows = new ValueHashMap(session.getDatabase());
    }

    public void removeDistinct(Value[] values) throws SQLException {
        if (distinctRows == null) {
            throw Message.getInternalError();
        }
        ValueArray array = ValueArray.get(values);
        distinctRows.remove(array);
        rowCount = distinctRows.size();
    }

    public boolean containsDistinct(Value[] values) throws SQLException {
        if (distinctRows == null) {
            throw Message.getInternalError();
        }
        ValueArray array = ValueArray.get(values);
        return distinctRows.get(array) != null;
    }

    public void reset() throws SQLException {
        rowId = -1;
        if (disk != null) {
            disk.reset();
            if (diskOffset > 0) {
                for (int i = 0; i < diskOffset; i++) {
                    disk.next();
                }
            }
        }
    }

    public Value[] currentRow() {
        return currentRow;
    }

    public boolean next() throws SQLException {
        if (rowId < rowCount) {
            rowId++;
            if (rowId < rowCount) {
                if (disk != null) {
                    currentRow = disk.next();
                } else {
                    currentRow = (Value[]) rows.get(rowId);
                }
                return true;
            }
            currentRow = null;
        }
        return false;
    }

    public int getRowId() {
        return rowId;
    }

    public void addRow(Value[] values) throws SQLException {
        for (int i = 0; i < values.length; i++) {
            // TODO display sizes: check if this is a performance problem, maybe
            // provide a setting to not do it
            Value v = values[i];
            int size = v.getDisplaySize();
            displaySizes[i] = Math.max(displaySizes[i], size);
        }
        if (distinctRows != null) {
            ValueArray array = ValueArray.get(values);
            distinctRows.put(array, values);
            rowCount = distinctRows.size();
            return;
        }
        rows.add(values);
        rowCount++;
        if (rows.size() > maxMemoryRows && session.getDatabase().isPersistent()) {
            if (disk == null) {
                disk = new ResultDiskBuffer(session, sort, values.length);
            }
            addRowsToDisk();
        }
    }

    private void addRowsToDisk() throws SQLException {
        disk.addRows(rows);
        rows.clear();
    }

    public int getVisibleColumnCount() {
        return visibleColumnCount;
    }

    public void done() throws SQLException {
        if (distinctRows != null) {
            rows = distinctRows.values();
            distinctRows = null;
        }
        if (disk != null) {
            addRowsToDisk();
            disk.done();
        } else {
            if (sort != null) {
                sort.sort(rows);
            }
        }
        applyOffset();
        applyLimit();
        reset();
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    private void applyLimit() {
        if (limit <= 0) {
            return;
        }
        if (disk == null) {
            if (rows.size() > limit) {
                rows.removeRange(limit, rows.size());
                rowCount = limit;
            }
        } else {
            if (limit < rowCount) {
                rowCount = limit;
            }
        }
    }

    public void close() {
        if (disk != null) {
            disk.close();
            disk = null;
        }
    }

    public String getAlias(int i) {
        return expressions[i].getAlias();
    }

    public String getTableName(int i) {
        return expressions[i].getTableName();
    }

    public String getSchemaName(int i) {
        return expressions[i].getSchemaName();
    }

    public int getDisplaySize(int i) {
        return displaySizes[i];
    }

    public String getColumnName(int i) {
        return expressions[i].getColumnName();
    }

    public int getColumnType(int i) {
        return expressions[i].getType();
    }

    public long getColumnPrecision(int i) {
        return expressions[i].getPrecision();
    }

    public int getNullable(int i) {
        return expressions[i].getNullable();
    }

    public boolean isAutoIncrement(int i) {
        return expressions[i].isAutoIncrement();
    }

    public int getColumnScale(int i) {
        return expressions[i].getScale();
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    private void applyOffset() {
        if (offset <= 0) {
            return;
        }
        if (disk == null) {
            if (offset >= rows.size()) {
                rows.clear();
                rowCount = 0;
            } else {
                // avoid copying the whole array for each row
                int remove = Math.min(offset, rows.size());
                rows.removeRange(0, remove);
                rowCount -= remove;
            }
        } else {
            if (offset >= rowCount) {
                rowCount = 0;
            } else {
                diskOffset = offset;
                rowCount -= offset;
            }
        }
    }

}
