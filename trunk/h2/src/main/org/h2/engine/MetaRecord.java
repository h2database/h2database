/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import org.h2.api.DatabaseEventListener;
import org.h2.command.Prepared;
import org.h2.message.Message;
import org.h2.message.Trace;
import org.h2.result.SearchRow;
import org.h2.value.ValueInt;
import org.h2.value.ValueString;

/**
 * A record in the system table of the database.
 * It contains the SQL statement to create the database object.
 */
public class MetaRecord implements Comparable<MetaRecord> {

    private int id;
    private int objectType;
    private String sql;

    public MetaRecord(SearchRow r) throws SQLException {
        id = r.getValue(0).getInt();
        objectType = r.getValue(2).getInt();
        sql = r.getValue(3).getString();
    }

    MetaRecord(DbObject obj) {
        id = obj.getId();
        objectType = obj.getType();
        sql = obj.getCreateSQL();
    }

    void setRecord(SearchRow r) {
        r.setValue(0, ValueInt.get(id));
        r.setValue(1, ValueInt.get(0));
        r.setValue(2, ValueInt.get(objectType));
        r.setValue(3, ValueString.get(sql));
    }

    /**
     * Execute the meta data statement.
     *
     * @param db the database
     * @param systemSession the system session
     * @param listener the database event listener
     */
    void execute(Database db, Session systemSession, DatabaseEventListener listener) throws SQLException {
        try {
            Prepared command = systemSession.prepare(sql);
            command.setObjectId(id);
            command.setCreate(false);
            command.update();
        } catch (Exception e) {
            SQLException s = Message.addSQL(Message.convert(e), sql);
            db.getTrace(Trace.DATABASE).error(sql, s);
            if (listener != null) {
                listener.exceptionThrown(s, sql);
                // continue startup in this case
            } else {
                throw s;
            }
        }
    }

    public int getId() {
        return id;
    }

    public int getObjectType() {
        return objectType;
    }

    public String getSQL() {
        return sql;
    }

    /**
     * Sort the list of meta records by 'create order'.
     *
     * @param other the other record
     * @return -1, 0, or 1
     */
    public int compareTo(MetaRecord other) {
        int c1 = DbObjectBase.getCreateOrder(getObjectType());
        int c2 = DbObjectBase.getCreateOrder(other.getObjectType());
        if (c1 != c2) {
            return c1 - c2;
        }
        return getId() - other.getId();
    }

}
