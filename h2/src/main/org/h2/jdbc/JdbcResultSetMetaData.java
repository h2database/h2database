/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.h2.message.*;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.MathUtils;
import org.h2.value.DataType;

/**
 * Represents the meta data for a ResultSet.
 */
public class JdbcResultSetMetaData extends TraceObject implements ResultSetMetaData {

    private JdbcResultSet rs;
    private ResultInterface result;

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
            return result.getVisibleColumnCount();
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return result.getAlias(--column);
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return result.getColumnName(--column);
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            int type = result.getColumnType(--column);
            return DataType.convertTypeToSQLType(type);
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            int type = result.getColumnType(--column);
            return DataType.getDataType(type).name;
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return result.getSchemaName(--column);
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return result.getTableName(--column);
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return rs.getConnection().getCatalog();
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return result.isAutoIncrement(--column);
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return true;
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return true;
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return false;
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this is nullable column.
     * Returns ResultSetMetaData.columnNullableUnknown if this is not a column of a table.
     * Otherwise, it returns ResultSetMetaData.columnNoNulls if the column is not nullable, and
     * ResultSetMetaData.columnNullable if it is nullable.
     *
     * @param column the column index (1,2,...)
     * @return ResultSetMetaData.column*
     * @throws SQLException if the result set is closed or invalid
     */
    public int isNullable(int column) throws SQLException {
        try {
            debugCodeCall("isNullable", column);
            rs.checkColumnIndex(column);
            return result.getNullable(--column);
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return true;
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return false;
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return true;
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return false;
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            int type = result.getColumnType(--column);
            return DataType.getTypeClassName(type);
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            long prec = result.getColumnPrecision(--column);
            return MathUtils.convertLongToInt(prec);
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return result.getColumnScale(--column);
        } catch(Throwable e) {
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
            rs.checkColumnIndex(column);
            return result.getDisplaySize(--column);
        } catch(Throwable e) {
            throw logAndConvert(e);
        }
    }

    JdbcResultSetMetaData(JdbcResultSet rs, ResultInterface result, Trace trace, int id) {
        setTrace(trace, TraceObject.RESULT_SET_META_DATA, id);
        this.rs = rs;
        this.result = result;
    }

    void checkClosed() throws SQLException {
        rs.checkClosed();
    }

    /**
     * Return an object of this class if possible.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public Object unwrap(Class<?> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

    /**
     * Checks if unwrap can return an object of this class.
     * @throws SQLException Unsupported Feature (SQL State 0A000)
     */
    //#ifdef JDK16
/*
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw Message.getUnsupportedException();
    }
*/
    //#endif

}
