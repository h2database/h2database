/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.ResultSet;

import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Database;
import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.ResultInterface;
import org.h2.table.Column;
import org.h2.tools.Csv;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.ValueInteger;
import org.h2.value.ValueVarchar;

/**
 * This class represents the statement CALL.
 */
public class Help extends Prepared {

    private final String[] conditions;

    private final Expression[] expressions;

    public Help(SessionLocal session, String[] conditions) {
        super(session);
        this.conditions = conditions;
        Database db = session.getDatabase();
        expressions = new Expression[] { //
                new ExpressionColumn(db, new Column("ID", TypeInfo.TYPE_INTEGER)), //
                new ExpressionColumn(db, new Column("SECTION", TypeInfo.TYPE_VARCHAR)), //
                new ExpressionColumn(db, new Column("TOPIC", TypeInfo.TYPE_VARCHAR)), //
                new ExpressionColumn(db, new Column("SYNTAX", TypeInfo.TYPE_VARCHAR)), //
                new ExpressionColumn(db, new Column("TEXT", TypeInfo.TYPE_VARCHAR)), //
        };
    }

    @Override
    public ResultInterface queryMeta() {
        LocalResult result = new LocalResult(session, expressions, 5, 5);
        result.done();
        return result;
    }

    @Override
    public ResultInterface query(long maxrows) {
        LocalResult result = new LocalResult(session, expressions, 5, 5);
        try {
            ResultSet rs = getTable();
            loop: for (int i = 0; rs.next(); i++) {
                String topic = rs.getString(2).trim();
                for (String condition : conditions) {
                    if (!topic.contains(condition)) {
                        continue loop;
                    }
                }
                result.addRow(
                        // ID
                        ValueInteger.get(i),
                        // SECTION
                        ValueVarchar.get(rs.getString(1).trim(), session),
                        // TOPIC
                        ValueVarchar.get(topic, session),
                        // SYNTAX
                        ValueVarchar.get(rs.getString(3).trim(), session),
                        // TEXT
                        ValueVarchar.get(rs.getString(4).trim(), session));
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        result.done();
        return result;
    }

    /**
     * Returns HELP table.
     *
     * @return HELP table
     * @throws IOException
     *             on I/O exception
     */
    public static ResultSet getTable() throws IOException {
        Reader reader = new InputStreamReader(new ByteArrayInputStream(Utils.getResource("/org/h2/res/help.csv")));
        Csv csv = new Csv();
        csv.setLineCommentCharacter('#');
        return csv.read(reader, null);
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public int getType() {
        return CommandInterface.CALL;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

}
