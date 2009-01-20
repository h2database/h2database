/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.sql.SQLException;

/**
 * A factory for embedded database sessions.
 */
public class SessionFactoryEmbedded implements SessionFactory {

    public SessionInterface createSession(ConnectionInfo ci) throws SQLException {
        return Engine.getInstance().getSession(ci);
    }

}
