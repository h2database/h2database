package org.h2.table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.h2.command.Prepared;
import org.h2.constraint.Constraint;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.SchemaObjectBase;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.value.CompareMode;
import org.h2.value.Value;

/**
 * Abstract base class for tables and table synonyms.
 */
public abstract class AbstractTable extends SchemaObjectBase {
    /**
     * Protected tables are not listed in the meta data and are excluded when
     * using the SCRIPT command.
     */
    protected boolean isHidden;

    public abstract boolean isView();

    /**
     * Lock the table for the given session.
     * This method waits until the lock is granted.
     *
     * @param session the session
     * @param exclusive true for write locks, false for read locks
     * @param forceLockEvenInMvcc lock even in the MVCC mode
     * @return true if the table was already exclusively locked by this session.
     * @throws DbException if a lock timeout occurred
     */
    public abstract boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc);

    /**
     * Close the table object and flush changes.
     *
     * @param session the session
     */
    public abstract void close(Session session);

    /**
     * Release the lock for this session.
     *
     * @param s the session
     */
    public abstract void unlock(Session s);

    /**
     * Create an index for this table
     *
     * @param session the session
     * @param indexName the name of the index
     * @param indexId the id
     * @param cols the index columns
     * @param indexType the index type
     * @param create whether this is a new index
     * @param indexComment the comment
     * @return the index
     */
    public abstract Index addIndex(Session session, String indexName,
                                   int indexId, IndexColumn[] cols, IndexType indexType,
                                   boolean create, String indexComment);

    /**
     * Remove a row from the table and all indexes.
     *
     * @param session the session
     * @param row the row
     */
    public abstract void removeRow(Session session, Row row);

    /**
     * Remove all rows from the table and indexes.
     *
     * @param session the session
     */
    public abstract void truncate(Session session);

    /**
     * Add a row to the table and all indexes.
     *
     * @param session the session
     * @param row the row
     * @throws DbException if a constraint was violated
     */
    public abstract void addRow(Session session, Row row);

    /**
     * Commit an operation (when using multi-version concurrency).
     *
     * @param operation the operation
     * @param row the row
     */
    @SuppressWarnings("unused")
    public abstract void commit(short operation, Row row);

    /**
     * Check if this table supports ALTER TABLE.
     *
     * @throws DbException if it is not supported
     */
    public abstract void checkSupportAlter();

    /**
     * Get the table type name
     *
     * @return the table type name
     */
    public abstract TableType getTableType();

    /**
     * Get the scan index to iterate through all rows.
     *
     * @param session the session
     * @return the index
     */
    public abstract Index getScanIndex(Session session);

    /**
     * Get the scan index for this table.
     *
     * @param session the session
     * @param masks the search mask
     * @param filters the table filters
     * @param filter the filter index
     * @param sortOrder the sort order
     * @param allColumnsSet all columns
     * @return the scan index
     */
    @SuppressWarnings("unused")
    public abstract Index getScanIndex(Session session, int[] masks,
                                       TableFilter[] filters, int filter, SortOrder sortOrder,
                                       HashSet<Column> allColumnsSet);

    /**
     * Get any unique index for this table if one exists.
     *
     * @return a unique index
     */
    public abstract Index getUniqueIndex();

    /**
     * Get all indexes for this table.
     *
     * @return the list of indexes
     */
    public abstract ArrayList<Index> getIndexes();

    /**
     * Get an index by name.
     *
     * @param indexName the index name to search for
     * @return the found index
     */
    public abstract Index getIndex(String indexName);

    /**
     * Check if this table is locked exclusively.
     *
     * @return true if it is.
     */
    public abstract boolean isLockedExclusively();

    /**
     * Get the last data modification id.
     *
     * @return the modification id
     */
    public abstract long getMaxDataModificationId();

    /**
     * Check if the table is deterministic.
     *
     * @return true if it is
     */
    public abstract boolean isDeterministic();

    /**
     * Check if the row count can be retrieved quickly.
     *
     * @return true if it can
     */
    public abstract boolean canGetRowCount();

    /**
     * Check if this table can be referenced.
     *
     * @return true if it can
     */
    public abstract boolean canReference();

    /**
     * Check if this table can be dropped.
     *
     * @return true if it can
     */
    public abstract boolean canDrop();

    /**
     * Get the row count for this table.
     *
     * @param session the session
     * @return the row count
     */
    public abstract long getRowCount(Session session);

    /**
     * Get the approximated row count for this table.
     *
     * @return the approximated row count
     */
    public abstract long getRowCountApproximation();

    public abstract long getDiskSpaceUsed();

    /**
     * Get the row id column if this table has one.
     *
     * @return the row id column, or null
     */
    public abstract Column getRowIdColumn();

    /**
     * Check whether the table (or view) contains no columns that prevent index
     * conditions to be used. For example, a view that contains the ROWNUM()
     * pseudo-column prevents this.
     *
     * @return true if the table contains no query-comparable column
     */
    public abstract boolean isQueryComparable();

    /**
     * Add all objects that this table depends on to the hash set.
     *
     * @param dependencies the current set of dependencies
     */
    public abstract void addDependencies(HashSet<DbObject> dependencies);

    /**
     * Rename a column of this table.
     *
     * @param column the column to rename
     * @param newName the new column name
     */
    public abstract void renameColumn(Column column, String newName);

    /**
     * Check if the table is exclusively locked by this session.
     *
     * @param session the session
     * @return true if it is
     */
    @SuppressWarnings("unused")
    public abstract boolean isLockedExclusivelyBy(Session session);

    /**
     * Update a list of rows in this table.
     *
     * @param prepared the prepared statement
     * @param session the session
     * @param rows a list of row pairs of the form old row, new row, old row,
     *            new row,...
     */
    public abstract void updateRows(Prepared prepared, Session session, RowList rows);

    public abstract ArrayList<TableView> getViews();

    /**
     * Check that these columns are not referenced by a multi-column constraint
     * or multi-column index. If it is, an exception is thrown. Single-column
     * references and indexes are dropped.
     *
     * @param session the session
     * @param columnsToDrop the columns to drop
     * @throws DbException if the column is referenced by multi-column
     *             constraints or indexes
     */
    public abstract void dropMultipleColumnsConstraintsAndIndexes(Session session,
                                                                  ArrayList<Column> columnsToDrop);

    public abstract Row getTemplateRow();

    /**
     * Get a new simple row object.
     *
     * @param singleColumn if only one value need to be stored
     * @return the simple row object
     */
    public abstract SearchRow getTemplateSimpleRow(boolean singleColumn);

    abstract Row getNullRow();

    public abstract Column[] getColumns();

    /**
     * Get the column at the given index.
     *
     * @param index the column index (0, 1,...)
     * @return the column
     */
    public abstract Column getColumn(int index);

    /**
     * Get the column with the given name.
     *
     * @param columnName the column name
     * @return the column
     * @throws DbException if the column was not found
     */
    public abstract Column getColumn(String columnName);

    /**
     * Does the column with the given name exist?
     *
     * @param columnName the column name
     * @return true if the column exists
     */
    public abstract boolean doesColumnExist(String columnName);

    /**
     * Get the best plan for the given search mask.
     *
     * @param session the session
     * @param masks per-column comparison bit masks, null means 'always false',
     *              see constants in IndexCondition
     * @param filters all joined table filters
     * @param filter the current table filter index
     * @param sortOrder the sort order
     * @param allColumnsSet the set of all columns
     * @return the plan item
     */
    public abstract PlanItem getBestPlanItem(Session session, int[] masks,
                                             TableFilter[] filters, int filter, SortOrder sortOrder,
                                             HashSet<Column> allColumnsSet);

    /**
     * Get the primary key index if there is one, or null if there is none.
     *
     * @return the primary key index or null
     */
    public abstract Index findPrimaryKey();

    public abstract Index getPrimaryKey();

    /**
     * Validate all values in this row, convert the values if required, and
     * update the sequence values if required. This call will also set the
     * default values if required and set the computed column if there are any.
     *
     * @param session the session
     * @param row the row
     */
    public abstract void validateConvertUpdateSequence(Session session, Row row);

    /**
     * Remove the given index from the list.
     *
     * @param index the index to remove
     */
    public abstract void removeIndex(Index index);

    /**
     * Remove the given view from the list.
     *
     * @param view the view to remove
     */
    public abstract void removeView(TableView view);

    /**
     * Remove the given synonym from the list.
     *
     * @param synonym the synonym to remove
     */
    public abstract void removeSynonym(TableSynonym synonym);

    /**
     * Remove the given constraint from the list.
     *
     * @param constraint the constraint to remove
     */
    public abstract void removeConstraint(Constraint constraint);

    /**
     * Remove a sequence from the table. Sequences are used as identity columns.
     *
     * @param sequence the sequence to remove
     */
    public abstract void removeSequence(Sequence sequence);

    /**
     * Remove the given trigger from the list.
     *
     * @param trigger the trigger to remove
     */
    public abstract void removeTrigger(TriggerObject trigger);

    /**
     * Add a view to this table.
     *
     * @param view the view to add
     */
    public abstract void addView(TableView view);

    /**
     * Add a synonym to this table.
     *
     * @param synonym the synonym to add
     */
    public abstract void addSynonym(TableSynonym synonym);

    /**
     * Add a constraint to the table.
     *
     * @param constraint the constraint to add
     */
    public abstract void addConstraint(Constraint constraint);

    public abstract ArrayList<Constraint> getConstraints();

    /**
     * Add a sequence to this table.
     *
     * @param sequence the sequence to add
     */
    public abstract void addSequence(Sequence sequence);

    /**
     * Add a trigger to this table.
     *
     * @param trigger the trigger to add
     */
    public abstract void addTrigger(TriggerObject trigger);

    /**
     * Fire the triggers for this table.
     *
     * @param session the session
     * @param type the trigger type
     * @param beforeAction whether 'before' triggers should be called
     */
    public abstract void fire(Session session, int type, boolean beforeAction);

    /**
     * Check whether this table has a select trigger.
     *
     * @return true if it has
     */
    public abstract boolean hasSelectTrigger();

    /**
     * Check if row based triggers or constraints are defined.
     * In this case the fire after and before row methods need to be called.
     *
     *  @return if there are any triggers or rows defined
     */
    public abstract boolean fireRow();

    /**
     * Fire all triggers that need to be called before a row is updated.
     *
     * @param session the session
     * @param oldRow the old data or null for an insert
     * @param newRow the new data or null for a delete
     * @return true if no further action is required (for 'instead of' triggers)
     */
    public abstract boolean fireBeforeRow(Session session, Row oldRow, Row newRow);

    /**
     * Fire all triggers that need to be called after a row is updated.
     *
     *  @param session the session
     *  @param oldRow the old data or null for an insert
     *  @param newRow the new data or null for a delete
     *  @param rollback when the operation occurred within a rollback
     */
    public abstract void fireAfterRow(Session session, Row oldRow, Row newRow,
                                      boolean rollback);

    public abstract boolean isGlobalTemporary();

    /**
     * Check if this table can be truncated.
     *
     * @return true if it can
     */
    public abstract boolean canTruncate();

    /**
     * Enable or disable foreign key constraint checking for this table.
     *
     * @param session the session
     * @param enabled true if checking should be enabled
     * @param checkExisting true if existing rows must be checked during this
     *            call
     */
    public abstract void setCheckForeignKeyConstraints(Session session, boolean enabled,
                                                       boolean checkExisting);

    public abstract boolean getCheckForeignKeyConstraints();

    /**
     * Get the index that has the given column as the first element.
     * This method returns null if no matching index is found.
     *
     * @param column the column
     * @param needGetFirstOrLast if the returned index must be able
     *          to do {@link Index#canGetFirstOrLast()}
     * @param needFindNext if the returned index must be able to do
     *          {@link Index#findNext(Session, SearchRow, SearchRow)}
     * @return the index or null
     */
    public abstract Index getIndexForColumn(Column column,
                                            boolean needGetFirstOrLast, boolean needFindNext);

    public abstract boolean getOnCommitDrop();

    public abstract void setOnCommitDrop(boolean onCommitDrop);

    public abstract boolean getOnCommitTruncate();

    public abstract void setOnCommitTruncate(boolean onCommitTruncate);

    /**
     * If the index is still required by a constraint, transfer the ownership to
     * it. Otherwise, the index is removed.
     *
     * @param session the session
     * @param index the index that is no longer required
     */
    public abstract void removeIndexOrTransferOwnership(Session session, Index index);

    /**
     * Check if a deadlock occurred. This method is called recursively. There is
     * a circle if the session to be tested has already being visited. If this
     * session is part of the circle (if it is the clash session), the method
     * must return an empty object array. Once a deadlock has been detected, the
     * methods must add the session to the list. If this session is not part of
     * the circle, or if no deadlock is detected, this method returns null.
     *
     * @param session the session to be tested for
     * @param clash set with sessions already visited, and null when starting
     *            verification
     * @param visited set with sessions already visited, and null when starting
     *            verification
     * @return an object array with the sessions involved in the deadlock, or
     *         null
     */
    @SuppressWarnings("unused")
    public abstract ArrayList<Session> checkDeadlock(Session session, Session clash,
                                                     Set<Session> visited);

    public abstract boolean isPersistIndexes();

    public abstract boolean isPersistData();

    /**
     * Compare two values with the current comparison mode. The values may be of
     * different type.
     *
     * @param a the first value
     * @param b the second value
     * @return 0 if both values are equal, -1 if the first value is smaller, and
     *         1 otherwise
     */
    public abstract int compareTypeSafe(Value a, Value b);

    public abstract CompareMode getCompareMode();

    /**
     * Tests if the table can be written. Usually, this depends on the
     * database.checkWritingAllowed method, but some tables (eg. TableLink)
     * overwrite this default behaviour.
     */
    public abstract void checkWritingAllowed();

    /**
     * Get or generate a default value for the given column.
     *
     * @param session the session
     * @param column the column
     * @return the value
     */
    public abstract Value getDefaultValue(Session session, Column column);

    @Override
    public boolean isHidden() {
        return isHidden;
    }

    public void setHidden(boolean hidden) {
        this.isHidden = hidden;
    }

    public abstract boolean isMVStore();
}
