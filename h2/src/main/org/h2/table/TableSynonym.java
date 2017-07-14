package org.h2.table;

import org.h2.command.ddl.CreateSynonymData;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.message.Trace;

/**
 * Synonym for an existing table or view. All DML requests are forwarded to the backing table. Adding indices
 * to a synonym or altering the table is not supported.
 */
public class TableSynonym extends AbstractTable {

    private CreateSynonymData data;

    private Table synonymFor;

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
    public int getType() {
        return TABLE_OR_VIEW;
    }

    @Override
    public String getCreateSQLForCopy(AbstractTable table, String quotedName) {
        return getSynonymFor().getCreateSQLForCopy(table, quotedName);
    }

    @Override
    public void rename(String newName) { throw DbException.getUnsupportedException("SYNONYM"); }


    @Override
    public TableType getTableType() {
        return TableType.SYNONYM;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        synonymFor.removeSynonym(this);
        database.removeMeta(session, getId());
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

    public String getSynonymForName() {
        return data.synonymFor;
    }

    public boolean isInvalid() {
        return data.synonymForSchema.findTableViewOrSynonym(data.session, data.synonymFor) == null;
    }


    public void updateSynonymFor() {
        if (synonymFor != null) {
            synonymFor.removeSynonym(this);
        }
        synonymFor = data.synonymForSchema.getTableOrView(data.session, data.synonymFor).resolve();
        synonymFor.addSynonym(this);
    }

    @Override
    public Table resolve() {
        return synonymFor;
    }

    @Override
    public Table asTable() { throw DbException.getUnsupportedException("SYNONYM"); }
}
