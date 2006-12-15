/*
 * Copyright 2004-2006 H2 Group. Licensed under the H2 License, Version 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.HashMap;

import org.h2.util.StringUtils;

public class RuleElement implements Rule {

    private boolean keyword;
    private String name;
    private Rule link;
    private int type;
    private String topic;
    
    public RuleElement(String name, boolean keyword, String topic) {
        this.name = name;
        this.topic = topic;
        this.keyword = keyword;
        this.type = Sentence.CONTEXT;
    }

    public RuleElement(String name, String topic) {
        this.name = name;
        this.topic = topic;
        if(name.length()==1 || name.equals(StringUtils.toUpperEnglish(name))) {
            keyword = true;
        }
        topic = StringUtils.toLowerEnglish(topic);
        this.type = topic.startsWith("function") ? Sentence.FUNCTION : Sentence.KEYWORD;
    }
    
    public RuleElement merge(RuleElement rule) {
        return new RuleElement(name + " " + rule.name, topic);
    }
    
    public String random(Bnf config, int level) {
        if(keyword) {
            return name.length() > 1 ? " " + name + " " : name;
        }
        if(link != null) {
            return link.random(config, level+1);
        }
        throw new Error(">>>" + name + "<<<");
    }

    public String name() {
        return name;
    }

    public Rule last() {
        return this;
    }

    public void setLinks(HashMap ruleMap) {
        if(link != null) {
            link.setLinks(ruleMap);
        }        
        if(keyword) {
            return;
        }
        for(int i=0; i<name.length() && link == null; i++) {
            String test = StringUtils.toLowerEnglish(name.substring(i));
            RuleHead r = (RuleHead)ruleMap.get(test);
            if(r != null) {
                link = r.rule;
                return;
            }
        }
        if(link == null) {
            throw new Error(">>>" + name + "<<<");
        }
    }
    
    public String matchRemove(String query, Sentence sentence) {
        if(sentence.stop()) {
            return null;
        }
        if(query.length()==0) {
            return null;
        }        
        if(keyword) {
            String up = StringUtils.toUpperEnglish(query);
            if(up.startsWith(name)) {
                query = query.substring(name.length());
                while(!name.equals("_") && query.length()>0 && Character.isWhitespace(query.charAt(0))) {
                    query = query.substring(1);
                }
                return query;
            }
            return null;
        } else {
            query = link.matchRemove(query, sentence);
            if(query != null && !name.startsWith("@") && (link.name() == null || !link.name().startsWith("@"))) {
                while(query.length()>0 && Character.isWhitespace(query.charAt(0))) {
                    query = query.substring(1);
                }
            }
            return query;
        }
    }    

    public void addNextTokenList(String query, Sentence sentence) {
        if(sentence.stop()) {
            return;
        }
        if(keyword) {
            String q = query.trim();
            String up = StringUtils.toUpperEnglish(q);
            if(q.length() == 0 || name.startsWith(up)) {
                if(q.length() < name.length()) {
                    sentence.add(name, name.substring(q.length()), type);
                }
            }
            return;
        }
        link.addNextTokenList(query, sentence);
    }

    public boolean isKeyword() {
        return keyword;
    }

}
