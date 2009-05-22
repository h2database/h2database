/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
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
     * @param config the configuration
     * @param level the call level
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
    void setLinks(HashMap<String, RuleHead> ruleMap);

    /**
     * Add the next possible token for a query.
     * Used for autocomplete support.
     *
     * @param sentence the sentence context
     */
    void addNextTokenList(Sentence sentence);

    /**
     * Remove a token from a sentence. Used for autocomplete support.
     * If there was a match, the query in the sentence is updated
     * (the matched token is removed).
     *
     * @param sentence
     *            the sentence context
     * @return false if not a match or a partial match, true if a full match
     */
    boolean matchRemove(Sentence sentence);

}
