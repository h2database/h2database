/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.dml;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;

import org.h2.command.Prepared;
import org.h2.engine.Session;
import org.h2.message.Message;
import org.h2.util.ScriptReader;
import org.h2.util.StringUtils;

public class RunScript extends ScriptBase {
    
    private String charset = StringUtils.getDefaultCharset();

    public RunScript(Session session) {
        super(session);
    }
    
    public int update() throws SQLException {
        session.getUser().checkAdmin();
        int count = 0;
        try {
            openInput();
            Reader reader = new InputStreamReader(in, charset);
            ScriptReader r = new ScriptReader(reader);
            while(true) {
                String sql = r.readStatement();
                if(sql == null) {
                    break;
                }
                execute(sql);
                count++;
            }
            reader.close();
        } catch(IOException e) {
            throw Message.convert(e);
        } finally {
            closeIO();
        }
        return count;
    }
    
    private void execute(String sql) throws SQLException {
        try {
            Prepared command = session.prepare(sql);
            if(command.isQuery()) {
                command.query(0);
            } else {
                command.update();
            }
            if(session.getAutoCommit()) {
                session.commit();
            }
        } catch(SQLException e) {
            throw Message.addSQL(e, sql);
        }
    }
    
    public void setCharset(String charset) {
        this.charset = charset;
    }
    
}
