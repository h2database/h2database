/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.bnf;

import java.util.ArrayList;
import java.util.HashMap;
import org.h2.util.New;

/**
 * Represents a sequence of BNF rules, or a list of alternative rules.
 */
public class RuleList implements Rule {

    private boolean or;
    private ArrayList<Rule> list;
    private boolean mapSet;

    RuleList(Rule first, Rule next, boolean or) {
        list = New.arrayList();
        if (first instanceof RuleList && ((RuleList) first).or == or) {
            list.addAll(((RuleList) first).list);
        } else {
            list.add(first);
        }
        if (next instanceof RuleList && ((RuleList) next).or == or) {
            list.addAll(((RuleList) next).list);
        } else {
            list.add(next);
        }
        this.or = or;
    }

    public String toString() {
        StringBuffer buff = new StringBuffer();
        if (or) {
            buff.append("{");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    buff.append("|");
                }
                buff.append(list.get(i).toString());
            }
            buff.append("}");
        } else {
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) {
                    buff.append(" ");
                }
                buff.append(list.get(i).toString());
            }
        }
        return buff.toString();
    }

    public String random(Bnf config, int level) {
        if (or) {
            if (level > 10) {
                if (level > 1000) {
                    // better than stack overflow
                    throw new Error();
                }
                return get(0).random(config, level);
            }
            int idx = config.getRandom().nextInt(list.size());
            return get(idx).random(config, level + 1);
        }
        StringBuffer buff = new StringBuffer();
        for (Rule r : list) {
            buff.append(r.random(config, level+1));
        }
        return buff.toString();
    }

    private Rule get(int idx) {
        return list.get(idx);
    }

    public String name() {
        return null;
    }

    public Rule last() {
        return get(list.size() - 1);
    }

    public void setLinks(HashMap<String, RuleHead> ruleMap) {
        if (!mapSet) {
            for (Rule r : list) {
                r.setLinks(ruleMap);
            }
            mapSet = true;
        }
    }

    public boolean matchRemove(Sentence sentence) {
        String query = sentence.getQuery();
        if (query.length() == 0) {
            return false;
        }
        if (or) {
            for (Rule r : list) {
                if (r.matchRemove(sentence)) {
                    return true;
                }
            }
            return false;
        }
        for (Rule r : list) {
            if (!r.matchRemove(sentence)) {
                return false;
            }
        }
        return true;
    }

    public void addNextTokenList(Sentence sentence) {
        String old = sentence.getQuery();
        if (or) {
            for (Rule r : list) {
                sentence.setQuery(old);
                r.addNextTokenList(sentence);
            }
        } else {
            for (Rule r : list) {
                r.addNextTokenList(sentence);
                if (!r.matchRemove(sentence)) {
                    break;
                }
            }
        }
        sentence.setQuery(old);
    }

}
