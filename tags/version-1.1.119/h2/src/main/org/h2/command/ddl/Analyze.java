/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.sql.SQLException;
import org.h2.command.Prepared;
import org.h2.engine.Constants;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.result.LocalResult;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.table.TableData;
import org.h2.util.StatementBuilder;

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
        // TODO do we need to lock the table?
        for (Table table : db.getAllTablesAndViews()) {
            if (!(table instanceof TableData)) {
                continue;
            }
            StatementBuilder buff = new StatementBuilder("SELECT ");
            Column[] columns = table.getColumns();
            for (Column col : columns) {
                buff.appendExceptFirst(", ");
                buff.append("SELECTIVITY(").append(col.getSQL()).append(')');
            }
            buff.append(" FROM ").append(table.getSQL());
            if (sampleRows > 0) {
                buff.append(" LIMIT 1 SAMPLE_SIZE ").append(sampleRows);
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
