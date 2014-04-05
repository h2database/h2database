/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

/**
 * The head element of the linked list.
 */
public class CacheHead extends CacheObject {

    @Override
    public boolean canRemove() {
        return false;
    }

    @Override
    public int getMemory() {
        return 0;
    }

}
