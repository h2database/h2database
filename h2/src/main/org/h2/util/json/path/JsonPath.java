/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.h2.engine.CastDataProvider;
import org.h2.message.DbException;
import org.h2.util.SmallLRUCache;
import org.h2.util.json.JSONValue;

/**
 * SQL/JSON path.
 */
public final class JsonPath {

    private static final SmallLRUCache<String, JsonPath> CACHE = SmallLRUCache.newInstance(95);

    private static final SmallLRUCache<String, JsonPath> CACHE_MEMBER_ACCESSORS = SmallLRUCache.newInstance(95);

    /**
     * Returns compiled SQL/JSON path.
     *
     * @param path
     *            path string
     * @return compiled path
     */
    public static JsonPath get(String path) {
        if (path.length() > 10_000) {
            return getNoCache(path);
        }
        JsonPath oldPath;
        synchronized (CACHE) {
            oldPath = CACHE.get(path);
        }
        if (oldPath != null) {
            return oldPath;
        }
        JsonPath newPath = getNoCache(path);
        synchronized (CACHE) {
            oldPath = CACHE.putIfAbsent(path, newPath);
        }
        return oldPath != null ? oldPath : newPath;
    }

    private static JsonPath getNoCache(String path) {
        PathParser parser = new PathParser(path);
        JsonPath newPath = new JsonPath(parser.parse(), parser.isStrict());
        return newPath;
    }

    /**
     * Returns compiled SQL/JSON path with member accessor {@code '$."name"'}.
     *
     * @param name
     *            member name
     * @return compiled path
     */
    public static JsonPath getMemberAccessor(String name) {
        if (name.length() > 10_000) {
            return getMemberAccessorNoCache(name);
        }
        JsonPath oldPath;
        synchronized (CACHE_MEMBER_ACCESSORS) {
            oldPath = CACHE_MEMBER_ACCESSORS.get(name);
        }
        if (oldPath != null) {
            return oldPath;
        }
        JsonPath newPath = getMemberAccessorNoCache(name);
        synchronized (CACHE_MEMBER_ACCESSORS) {
            oldPath = CACHE_MEMBER_ACCESSORS.putIfAbsent(name, newPath);
        }
        return oldPath != null ? oldPath : newPath;
    }

    private static JsonPath getMemberAccessorNoCache(String name) {
        return new JsonPath(new MemberAccessorExpression(ContextVariableExpression.INSTANCE, name), false);
    }

    private final JsonPathExpression expression;

    private final boolean strict;

    private JsonPath(JsonPathExpression expression, boolean strict) {
        this.expression = expression;
        this.strict = strict;
    }

    /**
     * Executes this path.
     *
     * @param provider
     *            cast data provider
     * @param context
     *            the context value
     * @param parameters
     *            the parameters or an empty map
     * @return execution result as a possibly empty list
     * @throws DbException
     *             on exception
     */
    public List<JSONValue> execute(CastDataProvider provider, JSONValue context, Map<String, JSONValue> parameters) {
        return expression.getValue(new Parameters(provider, strict, context, parameters), null, -1)
                .collect(Collectors.toList());
    }

}
