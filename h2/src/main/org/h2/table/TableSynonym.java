/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.command.ddl.CreateSynonymData;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObjectBase;

/**
 * Synonym for an existing table or view. All DML requests are forwarded to the backing table.
 * Adding indices to a synonym or altering the table is not supported.
 */
public class TableSynonym extends SchemaObjectBase {

    private CreateSynonymData data;

    private Table synonymFor;

    public TableSynonym(CreateSynonymData data) {
        initSchemaObjectBase(data.schema, data.id, data.synonymName, Trace.TABLE);
        this.data = data;
    }

    public Table getSynonymFor() {
        return synonymFor;
    }

    public void updateData(CreateSynonymData data) {
        this.data = data;
    }

    @Override
    public int getType() {
        return SYNONYM;
    }


    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        return synonymFor.getCreateSQLForCopy(table, quotedName);
    }

    @Override
    public void rename(String newName) { throw DbException.getUnsupportedException("SYNONYM"); }

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

    public Schema getSynonymForSchema() {
        return data.synonymForSchema;
    }

    public boolean isInvalid() {
        return synonymFor.isValid();
    }


    public void updateSynonymFor() {
        if (synonymFor != null) {
            synonymFor.removeSynonym(this);
        }
        synonymFor = data.synonymForSchema.getTableOrView(data.session, data.synonymFor);
        synonymFor.addSynonym(this);
    }

}
