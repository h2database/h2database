package org.h2.table;

import org.h2.command.ddl.CreateSynonymData;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Sysonym for an existing table or view. All DML requests are forwarded to the backing table. Adding indices
 * to a synonym or altering the table is not supported.
 */
public class TableSynonym extends Table {

    private CreateSynonymData data;

    public TableSynonym(CreateSynonymData data) {
        super(data.schema, data.id, data.synonymName, false, false);
        this.data = data;
    }

    public void updateData(CreateSynonymData data) {
        this.data = data;
    }

    private Table getSynonymFor() {
        return data.synonymForSchema.getTableOrView(data.session, data.synonymFor);
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
    public Column getColumn(String columnName) {
        return getSynonymFor().getColumn(columnName);
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
    public void removeRow(Session session, Row row) {
        getSynonymFor().removeRow(session, row);
    }

    @Override
    public void truncate(Session session) {
        getSynonymFor().truncate(session);
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
    public TableType getTableType() {
        return TableType.SYNONYM;
    }

    @Override
    public Index getScanIndex(Session session) {
        return getSynonymFor().getScanIndex(session);
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
    public boolean isLockedExclusively() {
        return getSynonymFor().isLockedExclusively();
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

    public boolean canTruncate() {
        return getSynonymFor().canTruncate();
    }

    public String getSynonymForName() {
        return data.synonymFor;
    }

    public boolean isInvalid() {
        try {
            getSynonymFor();
            return false;
        } catch (DbException e) {
            return true;
        }
    }

}
