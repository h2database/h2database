/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.result;

import org.h2.value.Value;

/**
 * The interface for rows stored in a table, and for partial rows stored in the
 * index.
 */
public interface SearchRow {

    /**
     * Get the column count.
     *
     * @return the column count
     */
    int getColumnCount();

    /**
     * Get the value for the column
     *
     * @param index the column number (starting with 0)
     * @return the value
     */
    Value getValue(int index);

    /**
     * Set the value for given column
     *
     * @param index the column number (starting with 0)
     * @param v the new value
     */
    void setValue(int index, Value v);

    /**
     * Set the position and version to match another row.
     *
     * @param old the other row.
     */
    void setPosAndVersion(SearchRow old);

    /**
     * Set the position (where the row is stored in the data file).
     *
     * @param pos the position.
     */
    void setPos(int pos);

    /**
     * Get the position of the row in the data file.
     *
     * @return the position
     */
    int getPos();

    /**
     * Get the version of the row.
     *
     * @return the version
     */
    int getVersion();

}
