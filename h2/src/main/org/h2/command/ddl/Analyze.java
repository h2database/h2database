/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.Arrays;

import org.h2.command.CommandInterface;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Right;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.result.Row;
import org.h2.schema.Schema;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableType;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * This class represents the statements
 * ANALYZE and ANALYZE TABLE
 */
public class Analyze extends DefineCommand {

    private static final class SelectivityData {

        private long distinctCount;

        /**
         * The number of occupied slots, excluding the zero element (if any).
         */
        private int size;

        private int[] elements;

        /**
         * Whether the zero element is present.
         */
        private boolean zeroElement;

        private int maxSize;

        SelectivityData() {
            elements = new int[8];
            maxSize = 7;
        }

        void add(Value v) {
            int currentSize = currentSize();
            if (currentSize >= Constants.SELECTIVITY_DISTINCT_COUNT) {
                size = 0;
                Arrays.fill(elements, 0);
                zeroElement = false;
                distinctCount += currentSize;
            }
            int hash = v.hashCode();
            if (hash == 0) {
                zeroElement = true;
            } else {
                if (size >= maxSize) {
                    rehash();
                }
                add(hash);
            }
        }

        int getSelectivity(long count) {
            int s;
            if (count == 0) {
                s = 0;
            } else {
                s = (int) (100 * (distinctCount + currentSize()) / count);
                if (s <= 0) {
                    s = 1;
                }
            }
            return s;
        }

        private int currentSize() {
            int size = this.size;
            if (zeroElement) {
                size++;
            }
            return size;
        }

        private void add(int element) {
            int len = elements.length;
            int mask = len - 1;
            int index = element & mask;
            int plus = 1;
            do {
                int k = elements[index];
                if (k == 0) {
                    // found an empty record
                    size++;
                    elements[index] = element;
                    return;
                } else if (k == element) {
                    // existing element
                    return;
                }
                index = (index + plus++) & mask;
            } while (plus <= len);
            // no space, ignore
        }

        private void rehash() {
            size = 0;
            int[] oldElements = elements;
            int len = oldElements.length << 1;
            elements = new int[len];
            maxSize = (int) (len * 90L / 100);
            for (int k : oldElements) {
                if (k != 0) {
                    add(k);
                }
            }
        }

    }

    /**
     * The sample size.
     */
    private int sampleRows;
    /**
     * used in ANALYZE TABLE...
     */
    private Table table;

    public Analyze(SessionLocal session) {
        super(session);
        sampleRows = getDatabase().getSettings().analyzeSample;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public long update() {
        session.getUser().checkAdmin();
        Database db = getDatabase();
        if (table != null) {
            analyzeTable(session, table, sampleRows, true);
        } else {
            for (Schema schema : db.getAllSchemasNoMeta()) {
                for (Table table : schema.getAllTablesAndViews(null)) {
                    analyzeTable(session, table, sampleRows, true);
                }
            }
        }
        return 0;
    }

    /**
     * Analyze this table.
     *
     * @param session the session
     * @param table the table
     * @param sample the number of sample rows
     * @param manual whether the command was called by the user
     */
    public static void analyzeTable(SessionLocal session, Table table, int sample, boolean manual) {
        if (!table.isValid()
                || table.getTableType() != TableType.TABLE //
                || session == null //
                || !manual && (session.getDatabase().isSysTableLocked() || table.hasSelectTrigger()) //
                || table.isTemporary() && !table.isGlobalTemporary() //
                        && session.findLocalTempTable(table.getName()) == null //
                || table.isLockedExclusively() && !table.isLockedExclusivelyBy(session)
                || !session.getUser().hasTableRight(table, Right.SELECT) //
                // if the connection is closed and there is something to undo
                || session.getCancel() != 0) {
            return;
        }
        table.lock(session, Table.READ_LOCK);
        Column[] columns = table.getColumns();
        int columnCount = columns.length;
        if (columnCount == 0) {
            return;
        }
        Cursor cursor = table.getScanIndex(session).find(session, null, null, false);
        if (cursor.next()) {
            SelectivityData[] array = new SelectivityData[columnCount];
            for (int i = 0; i < columnCount; i++) {
                Column col = columns[i];
                if (!DataType.isLargeObject(col.getType().getValueType())) {
                    array[i] = new SelectivityData();
                }
            }
            long rowNumber = 0;
            do {
                Row row = cursor.get();
                for (int i = 0; i < columnCount; i++) {
                    SelectivityData selectivity = array[i];
                    if (selectivity != null) {
                        selectivity.add(row.getValue(i));
                    }
                }
                rowNumber++;
            } while ((sample <= 0 || rowNumber < sample) && cursor.next());
            for (int i = 0; i < columnCount; i++) {
                SelectivityData selectivity = array[i];
                if (selectivity != null) {
                    columns[i].setSelectivity(selectivity.getSelectivity(rowNumber));
                }
            }
        } else {
            for (int i = 0; i < columnCount; i++) {
                columns[i].setSelectivity(0);
            }
        }
        session.getDatabase().updateMeta(session, table);
    }

    public void setTop(int top) {
        this.sampleRows = top;
    }

    @Override
    public int getType() {
        return CommandInterface.ANALYZE;
    }

}
