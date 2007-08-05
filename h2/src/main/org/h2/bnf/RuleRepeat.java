/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

public class RuleRepeat implements Rule {
    
    Rule rule;
    
    RuleRepeat(Rule rule) {
        this.rule = rule;
    }

    public String name() {
        return rule.name();
    }

    public Rule last() {
        return this;
    }

    public void setLinks(HashMap ruleMap) {
//        rule.setLinks(ruleMap);
    }

    public String random(Bnf config, int level) {
        return rule.random(config, level);
    }
    
    public String matchRemove(String query, Sentence sentence) {
        if(sentence.stop()) {
            return null;
        }
        if(query.length()==0) {
            return null;
        }
        while(true) {
            String s = rule.matchRemove(query, sentence);
            if(s==null) {
                return query;
            } else if(s.length()==0) {
                return s;
            }
            query = s;
        }
    }      

    public void addNextTokenList(String query, Sentence sentence) {
        if(sentence.stop()) {
            return;
        }
        while(true) {
            rule.addNextTokenList(query, sentence);
            String s = rule.matchRemove(query, sentence);
            if(s == null || s==query) {
                break;
            }
            query = s;
        }
    }

}
