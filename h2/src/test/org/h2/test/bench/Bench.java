/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0
 * (license2)
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

/**
 * The interface for benchmark tests.
 */
public interface Bench {

    /**
     * Initialize the database. This includes creating tables and inserting data.
     *
     * @param db the database object
     * @param size the amount of data
     */
    void init(Database db, int size) throws Exception;

    /**
     * Run the test.
     */
    void runTest() throws Exception;

    /**
     * Get the name of the test.
     *
     * @return the test name
     */
    String getName();

}
