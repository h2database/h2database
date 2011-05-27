/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License, 
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.indexer;

/**
 * Represents a weight of a token in a page.
 */
public class Weight {
    static final int TITLE = 10000, HEADER = 100, PARAGRAPH = 1;
    Page page;
    int value;
}
