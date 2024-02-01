/*
 * Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.command.ddl.CreateSynonymData;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.schema.Schema;
import org.h2.schema.SchemaObject;
import org.h2.util.ParserUtil;

/**
 * Synonym for an existing table or view. All DML requests are forwarded to the backing table.
 * Adding indices to a synonym or altering the table is not supported.
 */
public class TableSynonym extends SchemaObject {

    private CreateSynonymData data;

    /**
     * The table the synonym is created for.
     */
    private Table synonymFor;

    public TableSynonym(CreateSynonymData data) {
        super(data.schema, data.id, data.synonymName, Trace.TABLE);
        this.data = data;
    }

    /**
     * @return the table this is a synonym for
     */
    public Table getSynonymFor() {
        return synonymFor;
    }

    /**
     * Set (update) the data.
     *
     * @param data the new data
     */
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
    public void removeChildrenAndResources(SessionLocal session) {
        synonymFor.removeSynonym(this);
        database.removeMeta(session, getId());
    }

    @Override
    public String getCreateSQL() {
        StringBuilder builder = new StringBuilder("CREATE SYNONYM ");
        getSQL(builder, DEFAULT_SQL_FLAGS).append(" FOR ");
        ParserUtil.quoteIdentifier(builder, data.synonymForSchema.getName(), DEFAULT_SQL_FLAGS).append('.');
        ParserUtil.quoteIdentifier(builder, data.synonymFor, DEFAULT_SQL_FLAGS);
        return builder.toString();
    }

    @Override
    public String getDropSQL() {
        return getSQL(new StringBuilder("DROP SYNONYM "), DEFAULT_SQL_FLAGS).toString();
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("SYNONYM");
    }

    /**
     * @return the table this synonym is for
     */
    public String getSynonymForName() {
        return data.synonymFor;
    }

    /**
     * @return the schema this synonym is for
     */
    public Schema getSynonymForSchema() {
        return data.synonymForSchema;
    }

    /**
     * @return true if this synonym currently points to a real table
     */
    public boolean isInvalid() {
        return synonymFor.isValid();
    }

    /**
     * Update the table that this is a synonym for, to know about this synonym.
     */
    public void updateSynonymFor() {
        if (synonymFor != null) {
            synonymFor.removeSynonym(this);
        }
        synonymFor = data.synonymForSchema.getTableOrView(data.session, data.synonymFor);
        synonymFor.addSynonym(this);
    }

}
