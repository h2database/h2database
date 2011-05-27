/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

/**
 * Represents a BNF rule.
 */
public interface Rule {

    /**
     * Get the name of the rule.
     *
     * @return the name
     */
    String name();

    /**
     * Get a random entry.
     *
     * @param config
     * @param level
     * @return the entry
     */
    String random(Bnf config, int level);

    /**
     * Get the last entry.
     *
     * @return the last entry
     */
    Rule last();

    /**
     * Update cross references.
     *
     * @param ruleMap the reference map
     */
    void setLinks(HashMap ruleMap);

    /**
     * Add the next possible token for a query.
     * Used for autocomplete support.
     *
     * @param query the query
     * @param sentence the sentence context
     */
    void addNextTokenList(String query, Sentence sentence);

    /**
     * Remove a token from a sentence.
     * Used for autocomplete support.
     *
     * @param query the query
     * @param sentence the sentence context
     * @return null if not a match or a partial match, query.substring... if a full match
     */
    String matchRemove(String query, Sentence sentence);

}
