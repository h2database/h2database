/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.sql.SQLException;

import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.table.Table;
import org.h2.value.Value;

/**
 * @author Thomas
 */
public class UndoLogRecord {
    public static final short INSERT = 0, DELETE = 1;
    private static final int IN_MEMORY = 0, STORED = 1, IN_MEMORY_READ_POS = 2;
    private Table table;
    private Row row;
    private short operation;
    private short state;
    private int filePos;

    public boolean isStored() {
        return state == STORED;
    }

    public boolean canStore() {
        return table.getUniqueIndex() != null;
    }

    public UndoLogRecord(Table table, short op, Row row) {
        this.table = table;
        this.row = row;
        this.operation = op;
        this.state = IN_MEMORY;
    }

    public void undo(Session session) throws SQLException {
        switch (operation) {
        case INSERT:
            if(state == IN_MEMORY_READ_POS) {
                Index index = table.getUniqueIndex();
                Cursor cursor = index.find(session, row, row);
                cursor.next();
                int pos = cursor.getPos();
                row.setPos(pos);
                state = IN_MEMORY;
            }
            if(session.getDatabase().getLockMode()==Constants.LOCK_MODE_OFF) {
                if(row.getDeleted()) {
                    // it might have been deleted by another thread
                    return;
                }
            }
            try {
                table.removeRow(session, row);
            } catch(SQLException e) {
                if(session.getDatabase().getLockMode()==Constants.LOCK_MODE_OFF && e.getErrorCode() == Message.ROW_NOT_FOUND_WHEN_DELETING_1) {
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
                } catch(SQLException e) {
                    if(session.getDatabase().getLockMode()==Constants.LOCK_MODE_OFF && e.getErrorCode() == Message.DUPLICATE_KEY_1) {
                        // it might have been added by another thread
                        // ignore
                    } else {
                        throw e;
                    }
                }
            break;
        default:
            throw Message.getInternalError("op=" + operation);
        }
    }

    public void save(DataPage buff, FileStore file) throws SQLException {
        buff.reset();
        buff.writeInt(0);
        buff.writeInt(operation);
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

    public void seek(FileStore file) throws SQLException {
        file.seek(filePos * Constants.FILE_BLOCK_SIZE);
    }

    public void load(DataPage buff, FileStore file, Session session) throws SQLException {
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
        if (Constants.CHECK) {
            if (operation != op) {
                throw Message.getInternalError("operation=" + operation + " op=" + op);
            }
        }
        int columnCount = buff.readInt();
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            values[i] = buff.readValue();
        }
        row = new Row(values);
        state = IN_MEMORY_READ_POS;
    }

    public Table getTable() {
        return table;
    }
}
