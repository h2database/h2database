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

    private final Table synonymFor;

    public TableSynonym(CreateSynonymData data) {
        super(data.schema, data.id, data.synonymName, false, false);
        this.synonymFor = data.schema.getTableOrView(data.session, data.synonymFor);
    }

    @Override
    public void addDependencies(HashSet<DbObject> dependencies) {
        dependencies.add(synonymFor);
    }

    @Override
    public Column[] getColumns() {
        return synonymFor.getColumns();
    }

    @Override
    public boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc) {
        return synonymFor.lock(session, exclusive, forceLockEvenInMvcc);
    }

    @Override
    public void close(Session session) {
        synonymFor.close(session);
    }

    @Override
    public void unlock(Session s) {
        synonymFor.unlock(s);
    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType, boolean create, String indexComment) {
        throw DbException.getUnsupportedException("SYNONYM");
    }

    @Override
    public void removeRow(Session session, Row row) {
        synonymFor.removeRow(session, row);
    }

    @Override
    public void truncate(Session session) {
        synonymFor.truncate(session);
    }

    @Override
    public void addRow(Session session, Row row) {
        synonymFor.addRow(session, row);
    }

    @Override
    public void checkSupportAlter() {
        throw DbException.getUnsupportedException("SYNONYM");
    }

    @Override
    public String getTableType() {
        return SYNONYM;
    }

    @Override
    public Index getScanIndex(Session session) {
        return synonymFor.getScanIndex(session);
    }

    @Override
    public Index getUniqueIndex() {
        return synonymFor.getUniqueIndex();
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return synonymFor.getIndexes();
    }

    @Override
    public boolean isLockedExclusively() {
        return synonymFor.isLockedExclusively();
    }

    @Override
    public long getMaxDataModificationId() {
        return synonymFor.getMaxDataModificationId();
    }

    @Override
    public boolean isDeterministic() {
        return synonymFor.isDeterministic();
    }

    @Override
    public boolean canGetRowCount() {
        return synonymFor.canGetRowCount();
    }

    @Override
    public boolean canDrop() {
        return false;
    }

    @Override
    public long getRowCount(Session session) {
        return synonymFor.getRowCount(session);
    }

    @Override
    public long getRowCountApproximation() {
        return synonymFor.getRowCountApproximation();
    }

    @Override
    public long getDiskSpaceUsed() {
        return synonymFor.getDiskSpaceUsed();
    }

    @Override
    public String getCreateSQL() {
        return "CREATE SYNONYM " + getName() + " FOR " + synonymFor;
    }

    @Override
    public String getDropSQL() {
        return "DROP SYNONYM " + getName() + " FOR " + synonymFor;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("SYNONYM");
    }

    public boolean canTruncate() {
        return synonymFor.canTruncate();
    }
}
