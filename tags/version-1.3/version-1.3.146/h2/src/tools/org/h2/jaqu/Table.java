/*
 * Copyright 2004-2010 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * A class that implements this interface can be used as a database table.
 */
public interface Table {

    /**
     * This method is called to let the table define the primary key, indexes,
     * and the table name.
     */
    void define();
}
