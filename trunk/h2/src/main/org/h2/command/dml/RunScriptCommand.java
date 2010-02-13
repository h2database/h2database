/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import org.h2.command.Prepared;
import org.h2.constant.SysProperties;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.util.ScriptReader;

/**
 * This class represents the statement
 * RUNSCRIPT
 */
public class RunScriptCommand extends ScriptBase {

    private String charset = SysProperties.FILE_ENCODING;

    public RunScriptCommand(Session session) {
        super(session);
    }

    public int update() {
        session.getUser().checkAdmin();
        int count = 0;
        try {
            openInput();
            Reader reader = new InputStreamReader(in, charset);
            ScriptReader r = new ScriptReader(reader);
            while (true) {
                String sql = r.readStatement();
                if (sql == null) {
                    break;
                }
                execute(sql);
                count++;
            }
            reader.close();
        } catch (IOException e) {
            throw DbException.convertIOException(e, null);
        } finally {
            closeIO();
        }
        return count;
    }

    private void execute(String sql) {
        try {
            Prepared command = session.prepare(sql);
            if (command.isQuery()) {
                command.query(0);
            } else {
                command.update();
            }
            if (session.getAutoCommit()) {
                session.commit(false);
            }
        } catch (DbException e) {
            throw e.addSQL(sql);
        }
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public ResultInterface queryMeta() {
        return null;
    }

}
