/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
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
        Database db = getDatabase();
        expressions = new Expression[] { //
                new ExpressionColumn(db, new Column("SECTION", TypeInfo.TYPE_VARCHAR)), //
                new ExpressionColumn(db, new Column("TOPIC", TypeInfo.TYPE_VARCHAR)), //
                new ExpressionColumn(db, new Column("SYNTAX", TypeInfo.TYPE_VARCHAR)), //
                new ExpressionColumn(db, new Column("TEXT", TypeInfo.TYPE_VARCHAR)), //
        };
    }

    @Override
    public ResultInterface queryMeta() {
        LocalResult result = new LocalResult(session, expressions, 4, 4);
        result.done();
        return result;
    }

    @Override
    public ResultInterface query(long maxrows) {
        LocalResult result = new LocalResult(session, expressions, 4, 4);
        try {
            ResultSet rs = getTable();
            loop: while (rs.next()) {
                String topic = rs.getString(2).trim();
                for (String condition : conditions) {
                    if (!topic.contains(condition)) {
                        continue loop;
                    }
                }
                result.addRow(
                        // SECTION
                        ValueVarchar.get(rs.getString(1).trim(), session),
                        // TOPIC
                        ValueVarchar.get(topic, session),
                        // SYNTAX
                        ValueVarchar.get(stripAnnotationsFromSyntax(rs.getString(3)), session),
                        // TEXT
                        ValueVarchar.get(processHelpText(rs.getString(4)), session));
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        result.done();
        return result;
    }

    /**
     * Strip out the special annotations we use to help build the railroad/BNF diagrams
     * @param s to process
     * @return cleaned text
     */
    public static String stripAnnotationsFromSyntax(String s) {
        // SYNTAX column - Strip out the special annotations we use to
        // help build the railroad/BNF diagrams.
        return s.replaceAll("@c@ ", "").replaceAll("@h2@ ", "")
                .replaceAll("@c@", "").replaceAll("@h2@", "").trim();
    }

    /**
     * Sanitize value read from csv file (i.e. help.csv)
     * @param s text to process
     * @return text without wrapping quotes and trimmed
     */
    public static String processHelpText(String s) {
        int len = s.length();
        int end = 0;
        for (; end < len; end++) {
            char ch = s.charAt(end);
            if (ch == '.') {
                end++;
                break;
            }
            if (ch == '"') {
                do {
                    end++;
                } while (end < len && s.charAt(end) != '"');
            }
        }
        s = s.substring(0, end);
        return s.trim();
    }

    /**
     * Returns HELP table.
     *
     * @return HELP table with columns SECTION,TOPIC,SYNTAX,TEXT
     * @throws IOException
     *             on I/O exception
     */
    public static ResultSet getTable() throws IOException {
        Reader reader = new InputStreamReader(new ByteArrayInputStream(Utils.getResource("/org/h2/res/help.csv")),
                StandardCharsets.UTF_8);
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
