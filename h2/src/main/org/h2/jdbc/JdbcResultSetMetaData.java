/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.message.TraceObject;
import org.h2.result.ResultInterface;
import org.h2.util.MathUtils;
import org.h2.value.DataType;
import org.h2.value.ValueToObjectConverter;

/**
 * Represents the meta data for a ResultSet.
 */
public final class JdbcResultSetMetaData extends TraceObject implements ResultSetMetaData {

    private final String catalog;
    private final JdbcResultSet rs;
    private final JdbcPreparedStatement prep;
    private final ResultInterface result;
    private final int columnCount;

    JdbcResultSetMetaData(JdbcResultSet rs, JdbcPreparedStatement prep,
            ResultInterface result, String catalog, Trace trace, int id) {
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
    @Override
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
    @Override
    public String getColumnLabel(int column) throws SQLException {
        try {
            return result.getAlias(getColumn("getColumnLabel", column));
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
    @Override
    public String getColumnName(int column) throws SQLException {
        try {
            return result.getColumnName(getColumn("getColumnName", column));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the data type of a column.
     * See also java.sql.Type.
     *
     * @param column the column index (1,2,...)
     * @return the data type
     * @throws SQLException if the result set is closed or invalid
     */
    @Override
    public int getColumnType(int column) throws SQLException {
        try {
            return DataType.convertTypeToSQLType(result.getColumnType(getColumn("getColumnType", column)));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the data type name of a column.
     *
     * @param column the column index (1,2,...)
     * @return the data type name
     * @throws SQLException if the result set is closed or invalid
     */
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        try {
            return result.getColumnType(getColumn("getColumnTypeName", column)).getDeclaredTypeName();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Returns the schema name.
     *
     * @param column the column index (1,2,...)
     * @return the schema name, or "" (an empty string) if not applicable
     * @throws SQLException if the result set is closed or invalid
     */
    @Override
    public String getSchemaName(int column) throws SQLException {
        try {
            String schema = result.getSchemaName(getColumn("getSchemaName", column));
            return schema == null ? "" : schema;
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
    @Override
    public String getTableName(int column) throws SQLException {
        try {
            String table = result.getTableName(getColumn("getTableName", column));
            return table == null ? "" : table;
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
    @Override
    public String getCatalogName(int column) throws SQLException {
        try {
            getColumn("getCatalogName", column);
            return catalog == null ? "" : catalog;
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this an autoincrement column.
     *
     * @param column the column index (1,2,...)
     * @return false
     * @throws SQLException if the result set is closed or invalid
     */
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        try {
            return result.isIdentity(getColumn("isAutoIncrement", column));
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
    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        try {
            getColumn("isCaseSensitive", column);
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
    @Override
    public boolean isSearchable(int column) throws SQLException {
        try {
            getColumn("isSearchable", column);
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
    @Override
    public boolean isCurrency(int column) throws SQLException {
        try {
            getColumn("isCurrency", column);
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
    @Override
    public int isNullable(int column) throws SQLException {
        try {
            return result.getNullable(getColumn("isNullable", column));
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if this column is signed.
     * Returns true for numeric columns.
     *
     * @param column the column index (1,2,...)
     * @return true for numeric columns
     * @throws SQLException if the result set is closed or invalid
     */
    @Override
    public boolean isSigned(int column) throws SQLException {
        try {
            return DataType.isNumericType(result.getColumnType(getColumn("isSigned", column)).getValueType());
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
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        try {
            getColumn("isReadOnly", column);
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
    @Override
    public boolean isWritable(int column) throws SQLException {
        try {
            getColumn("isWritable", column);
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
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        try {
            getColumn("isDefinitelyWritable", column);
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
    @Override
    public String getColumnClassName(int column) throws SQLException {
        try {
            int type = result.getColumnType(getColumn("getColumnClassName", column)).getValueType();
            return ValueToObjectConverter.getDefaultClass(type, true).getName();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the precision for this column.
     *
     * @param column the column index (1,2,...)
     * @return the precision
     * @throws SQLException if the result set is closed or invalid
     */
    @Override
    public int getPrecision(int column) throws SQLException {
        try {
            return MathUtils.convertLongToInt(result.getColumnType(getColumn("getPrecision", column)).getPrecision());
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Gets the scale for this column.
     *
     * @param column the column index (1,2,...)
     * @return the scale
     * @throws SQLException if the result set is closed or invalid
     */
    @Override
    public int getScale(int column) throws SQLException {
        try {
            return result.getColumnType(getColumn("getScale", column)).getScale();
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
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        try {
            return result.getColumnType(getColumn("getColumnDisplaySize", column)).getDisplaySize();
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    private void checkClosed() {
        if (rs != null) {
            rs.checkClosed();
        }
        if (prep != null) {
            prep.checkClosed();
        }
    }

    /**
     * Writes trace information and checks validity of this object and
     * parameter.
     *
     * @param methodName
     *            the called method name
     * @param columnIndex
     *            1-based column index
     * @return 0-based column index
     */
    private int getColumn(String methodName, int columnIndex) {
        debugCodeCall(methodName, columnIndex);
        checkClosed();
        if (columnIndex < 1 || columnIndex > columnCount) {
            throw DbException.getInvalidValueException("columnIndex", columnIndex);
        }
        return columnIndex - 1;
    }

    /**
     * Return an object of this class if possible.
     *
     * @param iface the class
     * @return this
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            if (isWrapperFor(iface)) {
                return (T) this;
            }
            throw DbException.getInvalidValueException("iface", iface);
        } catch (Exception e) {
            throw logAndConvert(e);
        }
    }

    /**
     * Checks if unwrap can return an object of this class.
     *
     * @param iface the class
     * @return whether or not the interface is assignable from this class
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isAssignableFrom(getClass());
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": columns=" + columnCount;
    }

}
