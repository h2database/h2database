/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util.json.path;

import static org.h2.util.json.path.PathToken.ABS;
import static org.h2.util.json.path.PathToken.CEILING;
import static org.h2.util.json.path.PathToken.DOUBLE;
import static org.h2.util.json.path.PathToken.FLOOR;
import static org.h2.util.json.path.PathToken.KEYVALUE;
import static org.h2.util.json.path.PathToken.SIZE;
import static org.h2.util.json.path.PathToken.TYPE;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.h2.message.DbException;
import org.h2.util.json.JSONArray;
import org.h2.util.json.JSONBoolean;
import org.h2.util.json.JSONNull;
import org.h2.util.json.JSONNumber;
import org.h2.util.json.JSONObject;
import org.h2.util.json.JSONString;
import org.h2.util.json.JSONValue;
import org.h2.value.Value;

/**
 * A simple method.
 */
final class SimpleMethod extends JsonPathExpression {

    private static final JSONString TYPE_NULL = new JSONString("null");

    private static final JSONString TYPE_BOOLEAN = new JSONString("boolean");

    private static final JSONString TYPE_NUMERIC = new JSONString("number");

    private static final JSONString TYPE_STRING = new JSONString("string");

    private static final JSONString TYPE_DATE = new JSONString("date");

    private static final JSONString TYPE_TIME = new JSONString("time without time zone");

    private static final JSONString TYPE_TIME_TZ = new JSONString("time with time zone");

    private static final JSONString TYPE_TIMESTAMP = new JSONString("timestamp without time zone");

    private static final JSONString TYPE_TIMESTAMP_TZ = new JSONString("timestamp with time zone");

    private static final JSONString TYPE_ARRAY = new JSONString("array");

    private static final JSONString TYPE_OBJECT = new JSONString("object");

    private static final AtomicLong KEYVALUE_COUNTER = new AtomicLong();

    private final JsonPathExpression expression;

    private final int method;

    SimpleMethod(JsonPathExpression expression, int method) {
        this.expression = expression;
        this.method = method;
    }

    @Override
    Stream<JSONValue> getValue(Parameters params, JSONValue item, int lastCardinality) {
        Stream<JSONValue> stream = expression.getValue(params, item, lastCardinality);
        boolean strict = params.strict;
        switch (method) {
        case TYPE:
            stream = stream.map(SimpleMethod::type);
            break;
        case SIZE:
            stream = stream.map(SimpleMethod::size);
            break;
        case DOUBLE:
            if (!strict) {
                stream = stream.flatMap(ARRAY_ELEMENTS_LAX);
            }
            stream = stream.map(SimpleMethod::doubleValue);
            break;
        case CEILING:
            stream = toNumericStream(stream, strict).map(SimpleMethod::ceiling);
            break;
        case FLOOR:
            stream = toNumericStream(stream, strict).map(SimpleMethod::floor);
            break;
        case ABS:
            stream = toNumericStream(stream, strict).map(SimpleMethod::abs);
            break;
        case KEYVALUE:
            if (!strict) {
                stream = stream.flatMap(ARRAY_ELEMENTS_LAX);
            }
            stream = stream.flatMap(SimpleMethod::keyvalue);
            break;
        default:
            throw DbException.getInternalError("method = " + method);
        }
        return stream;
    }

    private static JSONValue type(JSONValue value) {
        Class<? extends JSONValue> clazz = value.getClass();
        if (clazz == JSONNull.class) {
            return TYPE_NULL;
        } else if (clazz == JSONBoolean.class) {
            return TYPE_BOOLEAN;
        } else if (clazz == JSONNumber.class) {
            return TYPE_NUMERIC;
        } else if (clazz == JSONString.class) {
            return TYPE_STRING;
        } else if (clazz == JSONDatetime.class) {
            switch (((JSONDatetime) value).getValue().getValueType()) {
            case Value.DATE:
                return TYPE_DATE;
            case Value.TIME:
                return TYPE_TIME;
            case Value.TIME_TZ:
                return TYPE_TIME_TZ;
            case Value.TIMESTAMP:
                return TYPE_TIMESTAMP;
            case Value.TIMESTAMP_TZ:
                return TYPE_TIMESTAMP_TZ;
            }
        } else if (clazz == JSONArray.class) {
            return TYPE_ARRAY;
        } else if (clazz == JSONObject.class) {
            return TYPE_OBJECT;
        }
        throw DbException.getInternalError(clazz.getName());
    }

    private static JSONValue size(JSONValue value) {
        return JSONNumber.valueOf(value instanceof JSONArray ? ((JSONArray) value).length() : 1);
    }

    private static JSONValue doubleValue(JSONValue value) {
        double d;
        if (value instanceof JSONNumber) {
            JSONNumber numeric = (JSONNumber) value;
            d = numeric.getBigDecimal().doubleValue();
        } else if (value instanceof JSONString) {
            try {
                d = Double.parseDouble(((JSONString) value).getString());
            } catch (NumberFormatException e) {
                throw DbException.getInvalidValueException(e, "double value", value.toString());
            }
        } else {
            throw DbException.getInvalidValueException("double value", value.toString());
        }
        return new JSONNumber(BigDecimal.valueOf(d));
    }

    private static JSONValue ceiling(JSONNumber value) {
        BigDecimal bigDecimal = value.getBigDecimal();
        return bigDecimal.scale() == 0 ? value : new JSONNumber(bigDecimal.setScale(0, RoundingMode.CEILING));
    }

    private static JSONValue floor(JSONNumber value) {
        BigDecimal bigDecimal = value.getBigDecimal();
        return bigDecimal.scale() == 0 ? value : new JSONNumber(bigDecimal.setScale(0, RoundingMode.FLOOR));
    }

    private static JSONValue abs(JSONNumber value) {
        BigDecimal bigDecimal = value.getBigDecimal();
        return bigDecimal.signum() >= 0 ? value : new JSONNumber(bigDecimal.negate());
    }

    private static Stream<JSONValue> keyvalue(JSONValue value) {
        if (value instanceof JSONObject) {
            JSONObject object = (JSONObject) value;
            JSONNumber id = JSONNumber.valueOf(KEYVALUE_COUNTER.incrementAndGet());
            return object.getMembersAsStream().map(entry -> {
                JSONObject row = new JSONObject();
                row.addMember("name", new JSONString(entry.getKey()));
                row.addMember("value", entry.getValue());
                row.addMember("id", id);
                return row;
            });
        } else {
            throw DbException.getInvalidValueException("object", value.toString());
        }
    }

}
