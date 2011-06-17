/*
 * Copyright 2004-2008 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.build.indexer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

/**
 * Represents a word of the full text index.
 */
public class Word {

    /**
     * The word text.
     */
    String name;

    private HashMap pages = new HashMap();
    private ArrayList weightList;

    Word(String name) {
        this.name = name;
    }

    /**
     * Add a page to this word.
     *
     * @param page the page
     * @param weight the weight of this word in this page
     */
    void addPage(Page page, int weight) {
        Weight w = (Weight) pages.get(page);
        if (w == null) {
            w = new Weight();
            w.page = page;
            pages.put(page, w);
        }
        w.value += weight;
        page.relations++;
    }

    ArrayList getSortedWeights() {
        if (weightList == null) {
            weightList = new ArrayList(pages.values());
            Collections.sort(weightList, new Comparator() {
                public int compare(Object o0, Object o1) {
                    Weight w0 = (Weight) o0;
                    Weight w1 = (Weight) o1;
                    return w0.value < w1.value ? 1 : w0.value == w1.value ? 0 : -1;
                }
            });
        }
        return weightList;
    }
}
