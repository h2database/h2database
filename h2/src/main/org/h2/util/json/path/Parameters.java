/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import java.util.Map;

import org.h2.engine.CastDataProvider;
import org.h2.util.json.JSONValue;

/**
 * SQL/JSON path execution parameters.
 */
final class Parameters {

    /**
     * Cast data provider for datetime function and comparison operators.
     */
    final CastDataProvider provider;

    /**
     * {@code true} for strict mode, {@code false} for lax mode.
     */
    final boolean strict;

    /**
     * JSON context item value.
     */
    final JSONValue context;

    /**
     * Passed JSON arguments or an empty map.
     */
    final Map<String, JSONValue> parameters;

    Parameters(CastDataProvider provider, boolean strict, JSONValue context, //
            Map<String, JSONValue> parameters) {
        this.provider = provider;
        this.strict = strict;
        this.context = context;
        this.parameters = parameters;
    }

}
