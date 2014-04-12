/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu;

/**
 * Classes implementing this interface can be used as a declaration in an
 * update statement.
 */
public interface UpdateColumn {

    /**
     * Append the SQL to the given statement using the given query.
     *
     * @param stat the statement to append the SQL to
     */
    void appendSQL(SQLStatement stat);

}
