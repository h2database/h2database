/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.MathUtils;
import org.h2.value.DataType;

/**
 * Represents the meta data for a ResultSet.
 */
public class JdbcResultSetMetaData extends TraceObject implements ResultSetMetaData {

    private final String catalog;
    private final JdbcResultSet rs;
    private final JdbcPreparedStatement prep;
    private final ResultInterface result;
    private final int columnCount;

    JdbcResultSetMetaData(JdbcResultSet rs, JdbcPreparedStatement prep, ResultInterface result,
                String catalog, Trace trace, int id) {
        setTrace(trace, TraceObject.RESULT_SET_META_DATA, id);
        this.catalog = catalog;
        this.rs = rs;
        this.prep = prep;
        this.result = result;
        this.columnCount = result.getVisibleColumnCount();
    }

    /**
     * Returns the number of columns.
     *
     * @return the number of columns
     * @throws SQLException if the result set is closed or invalid
     */
    public int getColumnCount() throws SQLException {
        try {
            debugCodeCall("getColumnCount");
            checkClosed();
            return columnCount;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the column label.
     *
     * @param column the column index (1,2,...)
     * @return the column label
     * @throws SQLException if the result set is closed or invalid
     */
    public String getColumnLabel(int column) throws SQLException {
        try {
            debugCodeCall("getColumnLabel", column);
            checkColumnIndex(column);
            return result.getAlias(--column);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the column name.
     *
     * @param column the column index (1,2,...)
     * @return the column name
     * @throws SQLException if the result set is closed or invalid
     */
    public String getColumnName(int column) throws SQLException {
        try {
            debugCodeCall("getColumnName", column);
            checkColumnIndex(column);
            return result.getColumnName(--column);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the data type of a column.
     *
     * @param column the column index (1,2,...)
     * @return the data type
     * @throws SQLException if the result set is closed or invalid
     */
    public int getColumnType(int column) throws SQLException {
        try {
            debugCodeCall("getColumnType", column);
            checkColumnIndex(column);
            int type = result.getColumnType(--column);
            return DataType.convertTypeToSQLType(type);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the data type name of a column.
     *
     * @param column the column index (1,2,...)
     * @return the data type
     * @throws SQLException if the result set is closed or invalid
     */
    public String getColumnTypeName(int column) throws SQLException {
        try {
            debugCodeCall("getColumnTypeName", column);
            checkColumnIndex(column);
            int type = result.getColumnType(--column);
            return DataType.getDataType(type).name;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the schema name.
     *
     * @param column the column index (1,2,...)
     * @return the schema name
     * @throws SQLException if the result set is closed or invalid
     */
    public String getSchemaName(int column) throws SQLException {
        try {
            debugCodeCall("getSchemaName", column);
            checkColumnIndex(column);
            return result.getSchemaName(--column);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the table name.
     *
     * @param column the column index (1,2,...)
     * @return the table name
     * @throws SQLException if the result set is closed or invalid
     */
    public String getTableName(int column) throws SQLException {
        try {
            debugCodeCall("getTableName", column);
            checkColumnIndex(column);
            return result.getTableName(--column);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the catalog name.
     *
     * @param column the column index (1,2,...)
     * @return the catalog name
     * @throws SQLException if the result set is closed or invalid
     */
    public String getCatalogName(int column) throws SQLException {
        try {
            debugCodeCall("getCatalogName", column);
            checkColumnIndex(column);
            return catalog;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this an autoincrement column.
     * It always returns false.
     *
     * @param column the column index (1,2,...)
     * @return false
     * @throws SQLException if the result set is closed or invalid
     */
    public boolean isAutoIncrement(int column) throws SQLException {
        try {
            debugCodeCall("isAutoIncrement", column);
            checkColumnIndex(column);
            return result.isAutoIncrement(--column);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this column is case sensitive.
     * It always returns true.
     *
     * @param column the column index (1,2,...)
     * @return true
     * @throws SQLException if the result set is closed or invalid
     */
    public boolean isCaseSensitive(int column) throws SQLException {
        try {
            debugCodeCall("isCaseSensitive", column);
            checkColumnIndex(column);
            return true;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this column is searchable.
     * It always returns true.
     *
     * @param column the column index (1,2,...)
     * @return true
     * @throws SQLException if the result set is closed or invalid
     */
    public boolean isSearchable(int column) throws SQLException {
        try {
            debugCodeCall("isSearchable", column);
            checkColumnIndex(column);
            return true;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this is a currency column.
     * It always returns false.
     *
     * @param column the column index (1,2,...)
     * @return false
     * @throws SQLException if the result set is closed or invalid
     */
    public boolean isCurrency(int column) throws SQLException {
        try {
            debugCodeCall("isCurrency", column);
            checkColumnIndex(column);
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this is nullable column. Returns
     * ResultSetMetaData.columnNullableUnknown if this is not a column of a
     * table. Otherwise, it returns ResultSetMetaData.columnNoNulls if the
     * column is not nullable, and ResultSetMetaData.columnNullable if it is
     * nullable.
     *
     * @param column the column index (1,2,...)
     * @return ResultSetMetaData.column*
     * @throws SQLException if the result set is closed or invalid
     */
    public int isNullable(int column) throws SQLException {
        try {
            debugCodeCall("isNullable", column);
            checkColumnIndex(column);
            return result.getNullable(--column);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this column is signed.
     * It always returns true.
     *
     * @param column the column index (1,2,...)
     * @return true
     * @throws SQLException if the result set is closed or invalid
     */
    public boolean isSigned(int column) throws SQLException {
        try {
            debugCodeCall("isSigned", column);
            checkColumnIndex(column);
            return true;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this column is read only.
     * It always returns false.
     *
     * @param column the column index (1,2,...)
     * @return false
     * @throws SQLException if the result set is closed or invalid
     */
    public boolean isReadOnly(int column) throws SQLException {
        try {
            debugCodeCall("isReadOnly", column);
            checkColumnIndex(column);
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks whether it is possible for a write on this column to succeed.
     * It always returns true.
     *
     * @param column the column index (1,2,...)
     * @return true
     * @throws SQLException if the result set is closed or invalid
     */
    public boolean isWritable(int column) throws SQLException {
        try {
            debugCodeCall("isWritable", column);
            checkColumnIndex(column);
            return true;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks whether a write on this column will definitely succeed.
     * It always returns false.
     *
     * @param column the column index (1,2,...)
     * @return false
     * @throws SQLException if the result set is closed or invalid
     */
    public boolean isDefinitelyWritable(int column) throws SQLException {
        try {
            debugCodeCall("isDefinitelyWritable", column);
            checkColumnIndex(column);
            return false;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the Java class name of the object that will be returned
     * if ResultSet.getObject is called.
     *
     * @param column the column index (1,2,...)
     * @return the Java class name
     * @throws SQLException if the result set is closed or invalid
     */
    public String getColumnClassName(int column) throws SQLException {
        try {
            debugCodeCall("getColumnClassName", column);
            checkColumnIndex(column);
            int type = result.getColumnType(--column);
            return DataType.getTypeClassName(type);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the precision for this column.
     * This method always returns 0.
     *
     * @param column the column index (1,2,...)
     * @return the precision
     * @throws SQLException if the result set is closed or invalid
     */
    public int getPrecision(int column) throws SQLException {
        try {
            debugCodeCall("getPrecision", column);
            checkColumnIndex(column);
            long prec = result.getColumnPrecision(--column);
            return MathUtils.convertLongToInt(prec);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the scale for this column.
     * This method always returns 0.
     *
     * @param column the column index (1,2,...)
     * @return the scale
     * @throws SQLException if the result set is closed or invalid
     */
    public int getScale(int column) throws SQLException {
        try {
            debugCodeCall("getScale", column);
            checkColumnIndex(column);
            return result.getColumnScale(--column);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the maximum display size for this column.
     *
     * @param column the column index (1,2,...)
     * @return the display size
     * @throws SQLException if the result set is closed or invalid
     */
    public int getColumnDisplaySize(int column) throws SQLException {
        try {
            debugCodeCall("getColumnDisplaySize", column);
            checkColumnIndex(column);
            return result.getDisplaySize(--column);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void checkClosed() throws SQLException {
        if (rs != null) {
            rs.checkClosed();
        }
        if (prep != null) {
            prep.checkClosed();
        }
    }

    private void checkColumnIndex(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > columnCount) {
            throw Message.getInvalidValueException("" + columnIndex, "columnIndex");
        }
    }

    /**
     * [Not supported] Return an object of this class if possible.
     */
/*## Java 1.6 begin ##
    public <T> T unwrap(Class<T> iface) throws SQLException {
        debugCodeCall("unwrap");
        throw Message.getUnsupportedException("unwrap");
    }
## Java 1.6 end ##*/

    /**
     * [Not supported] Checks if unwrap can return an object of this class.
     */
/*## Java 1.6 begin ##
    public boolean isWrapperFor(Class< ? > iface) throws SQLException {
        debugCodeCall("isWrapperFor");
        throw Message.getUnsupportedException("isWrapperFor");
    }
## Java 1.6 end ##*/

    /**
     * INTERNAL
     */
    public String toString() {
        return getTraceObjectName() + ": columns=" + columnCount;
    }

}
