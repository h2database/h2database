/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools.indexer;

public class Weight {
    static final int TITLE = 10000, HEADER = 100, PARAGRAPH = 1;
    Page page;
    int value;
}
