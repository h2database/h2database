/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.table.TableLink;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;

/**
 * A linked index is a index for a linked (remote) table.
 * It is backed by an index on the remote table which is accessed over JDBC.
 */
public class LinkedIndex extends Index {

    private final TableLink link;
    private final String targetTableName;
    private long rowCount;

    private final int sqlFlags = QUOTE_ONLY_WHEN_REQUIRED;

    public LinkedIndex(TableLink table, int id, IndexColumn[] columns, int uniqueColumnCount, IndexType indexType) {
        super(table, id, null, columns, uniqueColumnCount, indexType);
        link = table;
        targetTableName = link.getQualifiedTable();
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public void close(SessionLocal session) {
        // nothing to do
    }

    private static boolean isNull(Value v) {
        return v == null || v == ValueNull.INSTANCE;
    }

    @Override
    public void add(SessionLocal session, Row row) {
        ArrayList<Value> params = Utils.newSmallArrayList();
        StringBuilder buff = new StringBuilder("INSERT INTO ");
        buff.append(targetTableName).append(" VALUES(");
        for (int i = 0; i < row.getColumnCount(); i++) {
            Value v = row.getValue(i);
            if (i > 0) {
                buff.append(", ");
            }
            if (v == null) {
                buff.append("DEFAULT");
            } else if (isNull(v)) {
                buff.append("NULL");
            } else {
                buff.append('?');
                params.add(v);
            }
        }
        buff.append(')');
        String sql = buff.toString();
        try {
            link.execute(sql, params, true, session);
            rowCount++;
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last, boolean reverse) {
        assert !reverse;
        ArrayList<Value> params = Utils.newSmallArrayList();
        StringBuilder builder = new StringBuilder("SELECT * FROM ").append(targetTableName).append(" T");
        boolean f = false;
        for (int i = 0; first != null && i < first.getColumnCount(); i++) {
            Value v = first.getValue(i);
            if (v != null) {
                builder.append(f ? " AND " : " WHERE ");
                f = true;
                Column col = table.getColumn(i);
                addColumnName(builder, col);
                if (v == ValueNull.INSTANCE) {
                    builder.append(" IS NULL");
                } else {
                    builder.append(">=");
                    addParameter(builder, col);
                    params.add(v);
                }
            }
        }
        for (int i = 0; last != null && i < last.getColumnCount(); i++) {
            Value v = last.getValue(i);
            if (v != null) {
                builder.append(f ? " AND " : " WHERE ");
                f = true;
                Column col = table.getColumn(i);
                addColumnName(builder, col);
                if (v == ValueNull.INSTANCE) {
                    builder.append(" IS NULL");
                } else {
                    builder.append("<=");
                    addParameter(builder, col);
                    params.add(v);
                }
            }
        }
        String sql = builder.toString();
        try {
            PreparedStatement prep = link.execute(sql, params, false, session);
            ResultSet rs = prep.getResultSet();
            return new LinkedCursor(link, rs, session, sql, prep);
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    private void addColumnName(StringBuilder builder, Column col) {
        String identifierQuoteString = link.getIdentifierQuoteString();
        String name = col.getName();
        if (identifierQuoteString == null || identifierQuoteString.isEmpty() || identifierQuoteString.equals(" ")) {
            builder.append(name);
        } else if (identifierQuoteString.equals("\"")) {
            /*
             * StringUtils.quoteIdentifier() can produce Unicode identifiers,
             * but target DBMS isn't required to support them
             */
            builder.append('"');
            int i = name.indexOf('"');
            if (i < 0) {
                builder.append(name);
            } else {
                builder.append(name, 0, ++i).append('"');
                for (int l = name.length(); i < l; i++) {
                    char c = name.charAt(i);
                    if (c == '"') {
                        builder.append('"');
                    }
                    builder.append(c);
                }
            }
            builder.append('"');
        } else {
            builder.append(identifierQuoteString).append(name).append(identifierQuoteString);
        }
    }

    private void addParameter(StringBuilder builder, Column col) {
        TypeInfo type = col.getType();
        if (type.getValueType() == Value.CHAR && link.isOracle()) {
            // workaround for Oracle
            // create table test(id int primary key, name char(15));
            // insert into test values(1, 'Hello')
            // select * from test where name = ? -- where ? = "Hello" > no rows
            builder.append("CAST(? AS CHAR(").append(type.getPrecision()).append("))");
        } else {
            builder.append('?');
        }
    }

    @Override
    public double getCost(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        return 100 + getCostRangeIndex(masks, rowCount +
                Constants.COST_ROW_OFFSET, filters, filter, sortOrder, false, allColumnsSet);
    }

    @Override
    public void remove(SessionLocal session) {
        // nothing to do
    }

    @Override
    public void truncate(SessionLocal session) {
        // nothing to do
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("LINKED");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        ArrayList<Value> params = Utils.newSmallArrayList();
        StringBuilder builder = new StringBuilder("DELETE FROM ").append(targetTableName).append(" WHERE ");
        for (int i = 0; i < row.getColumnCount(); i++) {
            if (i > 0) {
                builder.append("AND ");
            }
            Column col = table.getColumn(i);
            addColumnName(builder, col);
            Value v = row.getValue(i);
            if (isNull(v)) {
                builder.append(" IS NULL ");
            } else {
                builder.append('=');
                addParameter(builder, col);
                params.add(v);
                builder.append(' ');
            }
        }
        String sql = builder.toString();
        try {
            PreparedStatement prep = link.execute(sql, params, false, session);
            int count = prep.executeUpdate();
            link.reusePreparedStatement(prep, sql);
            rowCount -= count;
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    /**
     * Update a row using a UPDATE statement. This method is to be called if the
     * emit updates option is enabled.
     *
     * @param oldRow the old data
     * @param newRow the new data
     * @param session the session
     */
    public void update(Row oldRow, Row newRow, SessionLocal session) {
        ArrayList<Value> params = Utils.newSmallArrayList();
        StringBuilder builder = new StringBuilder("UPDATE ").append(targetTableName).append(" SET ");
        for (int i = 0; i < newRow.getColumnCount(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            table.getColumn(i).getSQL(builder, sqlFlags).append('=');
            Value v = newRow.getValue(i);
            if (v == null) {
                builder.append("DEFAULT");
            } else {
                builder.append('?');
                params.add(v);
            }
        }
        builder.append(" WHERE ");
        for (int i = 0; i < oldRow.getColumnCount(); i++) {
            Column col = table.getColumn(i);
            if (i > 0) {
                builder.append(" AND ");
            }
            addColumnName(builder, col);
            Value v = oldRow.getValue(i);
            if (isNull(v)) {
                builder.append(" IS NULL");
            } else {
                builder.append('=');
                params.add(v);
                addParameter(builder, col);
            }
        }
        String sql = builder.toString();
        try {
            link.execute(sql, params, true, session);
        } catch (Exception e) {
            throw TableLink.wrapException(sql, e);
        }
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return rowCount;
    }

}
