/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

/**
 * JSON object.
 */
public final class JSONObject extends JSONValue {

    private final ArrayList<SimpleImmutableEntry<String, JSONValue>> members = new ArrayList<>();

    public JSONObject() {
    }

    /**
     * Add a key-value pair.
     *
     * @param name
     *            the key
     * @param value
     *            the value
     */
    public void addMember(String name, JSONValue value) {
        members.add(new SimpleImmutableEntry<>(name, value));
    }

    @Override
    public void addTo(JSONTarget<?> target) {
        target.startObject();
        for (SimpleImmutableEntry<String, JSONValue> member : members) {
            target.member(member.getKey());
            member.getValue().addTo(target);
        }
        target.endObject();
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    @SuppressWarnings("unchecked")
    public Entry<String, JSONValue>[] getMembers() {
        return members.toArray(new Entry[0]);
    }

    /**
     * Returns the value as stream.
     *
     * @return the stream.
     */
    public Stream<SimpleImmutableEntry<String, JSONValue>> getMembersAsStream() {
        return members.stream();
    }

    /**
     * Returns value of the first member with the specified name.
     *
     * @param name
     *            name of the member
     * @return value of the first member with the specified name, or
     *         {@code null}
     */
    public JSONValue getFirst(String name) {
        for (SimpleImmutableEntry<String, JSONValue> entry : members) {
            if (name.equals(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * Returns values of all members with the specified name, or all values if
     * the specified name is null.
     *
     * @param name
     *            name of the member, or ({@code null}
     * @return values of matched members
     */
    public List<JSONValue> getAll(String name) {
        ArrayList<JSONValue> list;
        if (name == null) {
            list = new ArrayList<>(members.size());
            for (SimpleImmutableEntry<String, JSONValue> entry : members) {
                list.add(entry.getValue());
            }
        } else {
            list = new ArrayList<>(1);
            for (SimpleImmutableEntry<String, JSONValue> entry : members) {
                if (name.equals(entry.getKey())) {
                    list.add(entry.getValue());
                }
            }
        }
        return list;
    }

}
