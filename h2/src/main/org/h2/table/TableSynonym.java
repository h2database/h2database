package org.h2.table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.h2.command.Prepared;
import org.h2.command.ddl.CreateSynonymData;
import org.h2.constraint.Constraint;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.result.Row;
import org.h2.result.RowList;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.schema.Sequence;
import org.h2.schema.TriggerObject;
import org.h2.value.CompareMode;
import org.h2.value.Value;

/**
 * Synonym for an existing table or view. All DML requests are forwarded to the backing table. Adding indices
 * to a synonym or altering the table is not supported.
 */
public class TableSynonym extends AbstractTable {

    private CreateSynonymData data;

    private AbstractTable synonymFor;

    public TableSynonym(CreateSynonymData data) {
        initSchemaObjectBase(data.schema, data.id, data.synonymName, Trace.TABLE);
        this.data = data;
    }

    public void updateData(CreateSynonymData data) {
        this.data = data;
    }

    private AbstractTable getSynonymFor() {
        return synonymFor;
    }

    @Override
    public void addDependencies(HashSet<DbObject> dependencies) {
        // no dependency. A table synonym will not prevent the backing table from being dropped, but
        // will become invalid instead.
    }

    @Override
    public Column[] getColumns() {
        return getSynonymFor().getColumns();
    }

    @Override
    public int getType() {
        return TABLE_OR_VIEW;
    }

    @Override
    public Column getColumn(String columnName) {
        return getSynonymFor().getColumn(columnName);
    }

    @Override
    public boolean doesColumnExist(String columnName) {
        return getSynonymFor().doesColumnExist(columnName);
    }

    @Override
    public PlanItem getBestPlanItem(Session session, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        return getSynonymFor().getBestPlanItem(session, masks, filters, filter, sortOrder, allColumnsSet);
    }

    @Override
    public Column getColumn(int index) {
        return getSynonymFor().getColumn(index);
    }

    @Override
    public Row getTemplateRow() {
        return getSynonymFor().getTemplateRow();
    }

    @Override
    public SearchRow getTemplateSimpleRow(boolean singleColumn) {
        return getSynonymFor().getTemplateSimpleRow(singleColumn);
    }

    @Override
    public Row getNullRow() {
        return getSynonymFor().getNullRow();
    }

    @Override
    public boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc) {
        return getSynonymFor().lock(session, exclusive, forceLockEvenInMvcc);
    }

    @Override
    public void close(Session session) {
        getSynonymFor().close(session);
    }

    @Override
    public void unlock(Session s) {
        getSynonymFor().unlock(s);
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType, boolean create, String indexComment) {
        throw DbException.getUnsupportedException("SYNONYM");
    }

    @Override
    public void rename(String newName) {
        getSynonymFor().rename(newName);
    }

    @Override
    public void removeRow(Session session, Row row) {
        getSynonymFor().removeRow(session, row);
    }

    @Override
    public void truncate(Session session) {
        getSynonymFor().truncate(session);
    }

    @Override
    public boolean isView() {
        return getSynonymFor().isView();
    }

    @Override
    public void addRow(Session session, Row row) {
        getSynonymFor().addRow(session, row);
    }

    @Override
    public void checkSupportAlter() {
        throw DbException.getUnsupportedException("SYNONYM");
    }

    @Override
    public void commit(short operation, Row row) {
        getSynonymFor().commit(operation, row);
    }

    @Override
    public TableType getTableType() {
        return TableType.SYNONYM;
    }

    @Override
    public Index getScanIndex(Session session) {
        return getSynonymFor().getScanIndex(session);
    }

    @Override
    public Index getScanIndex(Session session, int[] masks, TableFilter[] filters, int filter, SortOrder sortOrder, HashSet<Column> allColumnsSet) {
        return getSynonymFor().getScanIndex(session, masks, filters, filter, sortOrder, allColumnsSet);
    }

    @Override
    public Index getIndex(String indexName) {
        return getSynonymFor().getIndex(indexName);
    }

    @Override
    public Index getUniqueIndex() {
        return getSynonymFor().getUniqueIndex();
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return getSynonymFor().getIndexes();
    }

    @Override
    public boolean canReference() {
        return getSynonymFor().canReference();
    }

    @Override
    public boolean isLockedExclusively() {
        return getSynonymFor().isLockedExclusively();
    }

    @Override
    public Column getRowIdColumn() {
        return getSynonymFor().getRowIdColumn();
    }

    @Override
    public String getCreateSQLForCopy(AbstractTable table, String quotedName) {
        return getSynonymFor().getCreateSQLForCopy(table, quotedName);
    }

    @Override
    public long getMaxDataModificationId() {
        return getSynonymFor().getMaxDataModificationId();
    }

    @Override
    public boolean isDeterministic() {
        return getSynonymFor().isDeterministic();
    }

    @Override
    public boolean canGetRowCount() {
        return getSynonymFor().canGetRowCount();
    }

    @Override
    public boolean isQueryComparable() {
        return getSynonymFor().isQueryComparable();
    }

    @Override
    public boolean canDrop() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return getSynonymFor().getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return getSynonymFor().getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return getSynonymFor().getDiskSpaceUsed();
    }

    @Override
    public ArrayList<DbObject> getChildren() {
        return getSynonymFor().getChildren();
    }

    @Override
    public void renameColumn(Column column, String newName) {
        getSynonymFor().renameColumn(column, newName);
    }

    @Override
    public boolean isLockedExclusivelyBy(Session session) {
        return getSynonymFor().isLockedExclusivelyBy(session);
    }

    @Override
    public void updateRows(Prepared prepared, Session session, RowList rows) {
        getSynonymFor().updateRows(prepared, session, rows);
    }

    @Override
    public ArrayList<TableView> getViews() {
        return getSynonymFor().getViews();
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        synonymFor.removeSynonym(this);
        database.removeMeta(session, getId());
    }

    @Override
    public void removeSequence(Sequence sequence) {
        getSynonymFor().removeSequence(sequence);
    }

    @Override
    public void dropMultipleColumnsConstraintsAndIndexes(Session session, ArrayList<Column> columnsToDrop) {
        getSynonymFor().dropMultipleColumnsConstraintsAndIndexes(session, columnsToDrop);
    }

    @Override
    public String getCreateSQL() {
        return "CREATE SYNONYM " + getName() + " FOR " + data.synonymForSchema.getName() + "." + data.synonymFor;
    }

    @Override
    public String getDropSQL() {
        return "DROP SYNONYM " + getName();
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("SYNONYM");
    }

    @Override
    public boolean canTruncate() {
        return getSynonymFor().canTruncate();
    }

    @Override
    public void setCheckForeignKeyConstraints(Session session, boolean enabled, boolean checkExisting) {
        getSynonymFor().setCheckForeignKeyConstraints(session, enabled, checkExisting);
    }

    @Override
    public boolean getCheckForeignKeyConstraints() {
        return getSynonymFor().getCheckForeignKeyConstraints();
    }

    @Override
    public Index getIndexForColumn(Column column, boolean needGetFirstOrLast, boolean needFindNext) {
        return getSynonymFor().getIndexForColumn(column, needGetFirstOrLast, needFindNext);
    }

    @Override
    public boolean getOnCommitDrop() {
        return getSynonymFor().getOnCommitDrop();
    }

    @Override
    public void setOnCommitDrop(boolean onCommitDrop) {
        getSynonymFor().setOnCommitDrop(onCommitDrop);
    }

    @Override
    public boolean getOnCommitTruncate() {
        return getSynonymFor().getOnCommitTruncate();
    }

    @Override
    public void setOnCommitTruncate(boolean onCommitTruncate) {
        getSynonymFor().setOnCommitTruncate(onCommitTruncate);
    }

    @Override
    public void removeIndexOrTransferOwnership(Session session, Index index) {
        getSynonymFor().removeIndexOrTransferOwnership(session, index);
    }

    @Override
    public ArrayList<Session> checkDeadlock(Session session, Session clash, Set<Session> visited) {
        return getSynonymFor().checkDeadlock(session, clash, visited);
    }

    @Override
    public boolean isPersistIndexes() {
        return getSynonymFor().isPersistIndexes();
    }

    @Override
    public boolean isPersistData() {
        return getSynonymFor().isPersistData();
    }

    @Override
    public int compareTypeSafe(Value a, Value b) {
        return getSynonymFor().compareTypeSafe(a, b);
    }

    @Override
    public CompareMode getCompareMode() {
        return getSynonymFor().getCompareMode();
    }

    @Override
    public void checkWritingAllowed() {
        getSynonymFor().checkWritingAllowed();
    }

    @Override
    public Value getDefaultValue(Session session, Column column) {
        return getSynonymFor().getDefaultValue(session, column);
    }

    @Override
    public boolean isMVStore() {
        return getSynonymFor().isMVStore();
    }

    public String getSynonymForName() {
        return data.synonymFor;
    }

    public boolean isInvalid() {
        return data.synonymForSchema.findTableOrView(data.session, data.synonymFor) == null;
    }

    @Override
    public Index findPrimaryKey() {
        return getSynonymFor().findPrimaryKey();
    }

    @Override
    public Index getPrimaryKey() {
        return getSynonymFor().getPrimaryKey();
    }

    @Override
    public void validateConvertUpdateSequence(Session session, Row row) {
        getSynonymFor().validateConvertUpdateSequence(session, row);
    }

    @Override
    public void removeIndex(Index index) {
        getSynonymFor().removeIndex(index);
    }

    @Override
    public void removeView(TableView view) {
        getSynonymFor().removeView(view);
    }

    @Override
    public void removeSynonym(TableSynonym synonym) {
        throw DbException.getUnsupportedException("SYNONYM");
    }

    @Override
    public void removeConstraint(Constraint constraint) {
        getSynonymFor().removeConstraint(constraint);
    }

    @Override
    public void removeTrigger(TriggerObject trigger) {
        getSynonymFor().removeTrigger(trigger);
    }

    @Override
    public void addView(TableView view) {
        getSynonymFor().addView(view);
    }

    @Override
    public void addSynonym(TableSynonym synonym) {
        throw DbException.getUnsupportedException("SYNONYM");
    }

    @Override
    public void addConstraint(Constraint constraint) {
        getSynonymFor().addConstraint(constraint);
    }

    @Override
    public ArrayList<Constraint> getConstraints() {
        return getSynonymFor().getConstraints();
    }

    @Override
    public void addSequence(Sequence sequence) {
        getSynonymFor().addSequence(sequence);
    }

    @Override
    public void addTrigger(TriggerObject trigger) {
        getSynonymFor().addTrigger(trigger);
    }

    @Override
    public void fire(Session session, int type, boolean beforeAction) {
        getSynonymFor().fire(session, type, beforeAction);
    }

    @Override
    public boolean hasSelectTrigger() {
        return getSynonymFor().hasSelectTrigger();
    }

    @Override
    public boolean fireRow() {
        return getSynonymFor().fireRow();
    }

    @Override
    public boolean fireBeforeRow(Session session, Row oldRow, Row newRow) {
        return getSynonymFor().fireBeforeRow(session, oldRow, newRow);
    }

    @Override
    public void fireAfterRow(Session session, Row oldRow, Row newRow, boolean rollback) {
        getSynonymFor().fireAfterRow(session, oldRow, newRow, rollback);
    }

    public void updateSynonymFor() {
        if (synonymFor != null) {
            synonymFor.removeSynonym(this);
        }
        synonymFor = data.synonymForSchema.getTableOrView(data.session, data.synonymFor);
        synonymFor.addSynonym(this);
    }

    @Override
    public boolean isGlobalTemporary() {
        return getSynonymFor().isGlobalTemporary();
    }


}
