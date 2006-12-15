/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.tools.indexer;


public class Page {
    int id;
    String fileName;
    String name;
    String title;
    // TODO page.totalWeight is currently not used
    int totalWeight;
    int relations;
    
    Page(int id, String fileName, String name) {
        this.id = id;
        this.fileName = fileName;
        this.name = name;
    }

}
