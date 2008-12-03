/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.result.LocalResult;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.util.ObjectArray;

/**
 * This class represents the statement
 * ANALYZE
 */
public class Analyze extends DefineCommand {

    private int sampleRows = Constants.SELECTIVITY_ANALYZE_SAMPLE_ROWS;

    public Analyze(Session session) {
        super(session);
    }

    public int update() throws SQLException {
        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkAdmin();
        ObjectArray tables = db.getAllSchemaObjects(DbObject.TABLE_OR_VIEW);
        // TODO do we need to lock the table?
        for (int i = 0; i < tables.size(); i++) {
            Table table = (Table) tables.get(i);
            if (!(table instanceof TableData)) {
                continue;
            }
            Column[] columns = table.getColumns();
            StringBuffer buff = new StringBuffer();
            buff.append("SELECT ");
            for (int j = 0; j < columns.length; j++) {
                if (j > 0) {
                    buff.append(", ");
                }
                buff.append("SELECTIVITY(");
                buff.append(columns[j].getSQL());
                buff.append(")");
            }
            buff.append(" FROM ");
            buff.append(table.getSQL());
            if (sampleRows > 0) {
                buff.append(" LIMIT 1 SAMPLE_SIZE ");
                buff.append(sampleRows);
            }
            String sql = buff.toString();
            Prepared command = session.prepare(sql);
            LocalResult result = command.query(0);
            result.next();
            for (int j = 0; j < columns.length; j++) {
                int selectivity = result.currentRow()[j].getInt();
                columns[j].setSelectivity(selectivity);
            }
            db.update(session, table);
        }
        return 0;
    }

    public void setTop(int top) {
        this.sampleRows = top;
    }

}
