/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

public interface Rule {
    String name();
    String random(Bnf config, int level);
    Rule last();
    void setLinks(HashMap ruleMap);
    void addNextTokenList(String query, Sentence sentence);
    
    /**
     * 
     * @return null if not a match or a partial match, query.substring... if a full match
     */
    String matchRemove(String query, Sentence sentence);
    
}
