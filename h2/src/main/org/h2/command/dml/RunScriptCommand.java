/*
 * Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.h2.command.CommandContainer;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;

/**
 * This class represents the statement
 * RUNSCRIPT
 */
public class RunScriptCommand extends ScriptBase {

    /**
     * The byte order mark.
     * 0xfeff because this is the Unicode char
     * represented by the UTF-8 byte order mark (EF BB BF).
     */
    private static final char UTF8_BOM = '\uFEFF';

    private Charset charset = StandardCharsets.UTF_8;

    private boolean quirksMode;

    private boolean variableBinary;

    private boolean from1X;

    public RunScriptCommand(SessionLocal session) {
        super(session);
    }

    @Override
    public long update() {
        session.getUser().checkAdmin();
        int count = 0;
        boolean oldQuirksMode = session.isQuirksMode();
        boolean oldVariableBinary = session.isVariableBinary();
        try {
            openInput(charset);
            // if necessary, strip the BOM from the front of the file
            reader.mark(1);
            if (reader.read() != UTF8_BOM) {
                reader.reset();
            }
            if (quirksMode) {
                session.setQuirksMode(true);
            }
            if (variableBinary) {
                session.setVariableBinary(true);
            }
            ScriptReader r = new ScriptReader(reader);
            while (true) {
                String sql = r.readStatement();
                if (sql == null) {
                    break;
                }
                execute(sql);
                count++;
                if ((count & 127) == 0) {
                    checkCanceled();
                }
            }
            r.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        } finally {
            if (quirksMode) {
                session.setQuirksMode(oldQuirksMode);
            }
            if (variableBinary) {
                session.setVariableBinary(oldVariableBinary);
            }
            closeIO();
        }
        return count;
    }

    private void execute(String sql) {
        if (from1X) {
            sql = sql.trim();
            if (sql.startsWith("--")) {
                int i = 2, l = sql.length();
                char c;
                do {
                    if (i >= l) {
                        return;
                    }
                    c = sql.charAt(i++);
                } while (c != '\n' && c != '\r');
                sql = StringUtils.trimSubstring(sql, i);
            }
            if (sql.startsWith("INSERT INTO SYSTEM_LOB_STREAM VALUES(")) {
                int idx = sql.indexOf(", NULL, '");
                if (idx >= 0) {
                    sql = new StringBuilder(sql.length() + 1).append(sql, 0, idx + 8).append("X'")
                            .append(sql, idx + 9, sql.length()).toString();
                }
            }
        }
        try {
            Prepared command = session.prepare(sql);
            CommandContainer commandContainer = new CommandContainer(session, sql, command);
            if (commandContainer.isQuery()) {
                commandContainer.executeQuery(0, false);
            } else {
                commandContainer.executeUpdate(null);
            }
        } catch (DbException e) {
            throw e.addSQL(sql);
        }
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    /**
     * Enables or disables the quirks mode.
     *
     * @param quirksMode
     *            whether quirks mode should be enabled
     */
    public void setQuirksMode(boolean quirksMode) {
        this.quirksMode = quirksMode;
    }

    /**
     * Changes parsing of a BINARY data type.
     *
     * @param variableBinary
     *            {@code true} to parse BINARY as VARBINARY, {@code false} to
     *            parse it as is
     */
    public void setVariableBinary(boolean variableBinary) {
        this.variableBinary = variableBinary;
    }

    /**
     * Enables quirks for parsing scripts from H2 1.*.*.
     */
    public void setFrom1X() {
        variableBinary = quirksMode = from1X = true;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return CommandInterface.RUNSCRIPT;
    }

}
