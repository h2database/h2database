/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;
import org.h2.command.CommandInterface;
import org.h2.result.ResultInterface;
import org.h2.util.ObjectArray;
import org.h2.value.Value;

/**
 * The base class for both remote and embedded sessions.
 */
public abstract class SessionWithState implements SessionInterface {

    protected ObjectArray<String> sessionState;
    protected boolean sessionStateChanged;
    private boolean sessionStateUpdating;

    /**
     * Re-create the session state using the stored sessionState list.
     */
    protected void recreateSessionState() throws SQLException {
        if (sessionState != null && sessionState.size() > 0) {
            sessionStateUpdating = true;
            try {
                for (String sql : sessionState) {
                    CommandInterface ci = prepareCommand(sql, Integer.MAX_VALUE);
                    ci.executeUpdate();
                }
            } finally {
                sessionStateUpdating = false;
                sessionStateChanged = false;
            }
        }
    }

    /**
     * Read the session state if necessary.
     */
    public void readSessionState() throws SQLException {
        if (!sessionStateChanged || sessionStateUpdating) {
            return;
        }
        sessionStateChanged = false;
        sessionState = ObjectArray.newInstance();
        CommandInterface ci = prepareCommand("SELECT * FROM INFORMATION_SCHEMA.SESSION_STATE", Integer.MAX_VALUE);
        ResultInterface result = ci.executeQuery(0, false);
        while (result.next()) {
            Value[] row = result.currentRow();
            sessionState.add(row[1].getString());
        }
    }

}
