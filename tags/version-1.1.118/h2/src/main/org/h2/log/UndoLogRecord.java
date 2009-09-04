/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.log;

import java.sql.SQLException;

import org.h2.constant.ErrorCode;
import org.h2.constant.SysProperties;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.store.DataPage;
import org.h2.store.FileStore;
import org.h2.table.Table;
import org.h2.value.Value;

/**
 * An entry in a undo log.
 */
public class UndoLogRecord {

    /**
     * Operation type meaning the row was inserted.
     */
    public static final short INSERT = 0;

    /**
     * Operation type meaning the row was deleted.
     */
    public static final short DELETE = 1;

    private static final int IN_MEMORY = 0, STORED = 1, IN_MEMORY_INVALID = 2;
    private Table table;
    private Row row;
    private short operation;
    private short state;
    private int filePos;

    /**
     * Create a new undo log record
     *
     * @param table the table
     * @param op the operation type
     * @param row the row that was deleted or inserted
     */
    public UndoLogRecord(Table table, short op, Row row) {
        this.table = table;
        this.row = row;
        this.operation = op;
        this.state = IN_MEMORY;
    }

    /**
     * Check if the log record is stored in the file.
     *
     * @return true if it is
     */
    boolean isStored() {
        return state == STORED;
    }

    /**
     * Check if this undo log record can be store. Only record can be stored if
     * the table has a unique index.
     *
     * @return if it can be stored
     */
    boolean canStore() {
        return table.getUniqueIndex() != null;
    }

    /**
     * Un-do the operation. If the row was inserted before, it is deleted now,
     * and vice versa.
     *
     * @param session the session
     */
    public void undo(Session session) throws SQLException {
        switch (operation) {
        case INSERT:
            if (state == IN_MEMORY_INVALID) {
                Index index = table.getUniqueIndex();
                Cursor cursor = index.find(session, row, row);
                cursor.next();
                // can not just set the position, because the row
                // may already be in the cache
                row = cursor.get();
                state = IN_MEMORY;
            }
            if (session.getDatabase().getLockMode() == Constants.LOCK_MODE_OFF) {
                if (row.isDeleted()) {
                    // it might have been deleted by another thread
                    return;
                }
            }
            try {
                row.setDeleted(false);
                table.removeRow(session, row);
            } catch (SQLException e) {
                if (session.getDatabase().getLockMode() == Constants.LOCK_MODE_OFF
                        && e.getErrorCode() == ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1) {
                    // it might have been deleted by another thread
                    // ignore
                } else {
                    throw e;
                }
            }
            break;
        case DELETE:
            try {
                row.setPos(0);
                table.addRow(session, row);
                // reset session id, otherwise other session think
                // that this row was inserted by this session
                row.commit();
            } catch (SQLException e) {
                if (session.getDatabase().getLockMode() == Constants.LOCK_MODE_OFF
                        && e.getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                    // it might have been added by another thread
                    // ignore
                } else {
                    throw e;
                }
            }
            break;
        default:
            Message.throwInternalError("op=" + operation);
        }
    }

    /**
     * Go to the right position in the file.
     *
     * @param file the file
     */
    void seek(FileStore file) throws SQLException {
        file.seek(filePos * Constants.FILE_BLOCK_SIZE);
    }

    /**
     * Save the row in the file using the data page as a buffer.
     *
     * @param buff the data page that is used as a buffer
     * @param file the file
     */
    void save(DataPage buff, FileStore file) throws SQLException {
        buff.reset();
        buff.writeInt(0);
        buff.writeInt(operation);
        buff.writeByte(row.isDeleted() ? (byte) 1 : (byte) 0);
        buff.writeInt(row.getSessionId());
        buff.writeInt(row.getColumnCount());
        for (int i = 0; i < row.getColumnCount(); i++) {
            buff.writeValue(row.getValue(i));
        }
        buff.fillAligned();
        buff.setInt(0, buff.length() / Constants.FILE_BLOCK_SIZE);
        buff.updateChecksum();
        filePos = (int) (file.getFilePointer() / Constants.FILE_BLOCK_SIZE);
        file.write(buff.getBytes(), 0, buff.length());
        row = null;
        state = STORED;
    }

    /**
     * Load an undo log record row using the data page as a buffer.
     *
     * @param buff the data page that is used as a buffer
     * @param file the source file
     */
    void load(DataPage buff, FileStore file) throws SQLException {
        int min = Constants.FILE_BLOCK_SIZE;
        seek(file);
        buff.reset();
        file.readFully(buff.getBytes(), 0, min);
        int len = buff.readInt() * Constants.FILE_BLOCK_SIZE;
        buff.checkCapacity(len);
        if (len - min > 0) {
            file.readFully(buff.getBytes(), min, len - min);
        }
        buff.check(len);
        int op = buff.readInt();
        if (SysProperties.CHECK) {
            if (operation != op) {
                Message.throwInternalError("operation=" + operation + " op=" + op);
            }
        }
        boolean deleted = buff.readByte() == 1;
        int sessionId = buff.readInt();
        int columnCount = buff.readInt();
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            values[i] = buff.readValue();
        }
        row = new Row(values, 0);
        row.setDeleted(deleted);
        row.setSessionId(sessionId);
        state = IN_MEMORY_INVALID;
    }

    /**
     * Get the table.
     *
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    /**
     * This method is called after the operation was committed.
     * It commits the change to the indexes.
     */
    public void commit() throws SQLException {
        for (Index index : table.getIndexes()) {
            index.commit(operation, row);
        }
    }

    /**
     * Get the row that was deleted or inserted.
     *
     * @return the row
     */
    public Row getRow() {
        return row;
    }

    /**
     * Change the state from IN_MEMORY to IN_MEMORY_INVALID. This method is
     * called if a later record was read from the temporary file, and therefore
     * the position could have changed.
     */
    void invalidatePos() {
        if (this.state == IN_MEMORY) {
            state = IN_MEMORY_INVALID;
        }
    }
}
