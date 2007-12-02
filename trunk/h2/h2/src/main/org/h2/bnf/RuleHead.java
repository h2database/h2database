/*
 * Copyright 2004-2007 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;


public class RuleHead {
    int id;
    String section;
    String topic;
    Rule rule;

    RuleHead(int id, String section, String topic, Rule rule) {
        this.id = id;
        this.section = section;
        this.topic = topic;
        this.rule = rule;
    }
    
    public String getTopic() {
        return topic;
    }

    public Rule getRule() {
        return rule;
    }

}
