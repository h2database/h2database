/*
 * Copyright 2004-2008 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.value.Value;

/**
 * An index entry of a linear hash index.
 */
public class LinearHashEntry {

//    private LinearHashEntry(int home, int hash, Value key, int value) {
//        this.home = home;
//        this.hash = hash;
//        this.key = key;
//        this.value = value;
//    }

    public int home;
    public int hash;
    public Value key;
    public int value;

}
